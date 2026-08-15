package controllers

import (
	"context"
	"errors"
	"fmt"
	"sort"

	kmmv1beta1 "github.com/kubernetes-sigs/kernel-module-management/api/v1beta1"
	"github.com/kubernetes-sigs/kernel-module-management/internal/module"
	"github.com/kubernetes-sigs/kernel-module-management/internal/node"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// reconcileTargetLabel makes targetLabel reflect the desired state on every node it can affect: the
// nodes the Module currently selects, and the nodes that already carry the label. A node is only
// meant to carry it while the Module both selects it and can schedule on it, so narrowing the
// selector or removing a selector label from a node takes the label off as well.
//
// DaemonSet Pods tolerate the unschedulable taint, so this label is what lets a plugin Pod be
// evicted before the kernel module it depends on is unloaded. A stale label would keep the Pod on a
// node the Module no longer targets, and the kernel-module-ready label is not removed until the
// unloader succeeds, which is the deadlock this label exists to break.
//
// forceKeep names the nodes that must keep the label whatever the Module selector and the node's
// taints say, because something on them is still using what the plugin provides. It may be nil.
func reconcileTargetLabel(
	ctx context.Context,
	nodeAPI node.Node,
	mod *kmmv1beta1.Module,
	targetLabel string,
	forceKeep sets.Set[string],
) error {
	selectedNodes, err := nodeAPI.GetAllNodesBySelector(ctx, mod.Spec.Selector)
	if err != nil {
		return fmt.Errorf("could not list nodes targeted by module: %v", err)
	}

	// By key, not by key and value: a node whose label value was corrupted still needs correcting,
	// and the DaemonSet requires the value to be empty.
	labeledNodes, err := nodeAPI.GetAllNodesByLabelKey(ctx, targetLabel)
	if err != nil {
		return fmt.Errorf("could not list nodes with %s label: %v", targetLabel, err)
	}

	selectedNames := sets.New[string]()
	nodesByName := make(map[string]*v1.Node, len(selectedNodes)+len(labeledNodes))

	for i := range selectedNodes {
		n := &selectedNodes[i]
		selectedNames.Insert(n.Name)
		nodesByName[n.Name] = n
	}

	for i := range labeledNodes {
		n := &labeledNodes[i]
		if _, ok := nodesByName[n.Name]; !ok {
			nodesByName[n.Name] = n
		}
	}

	tolerations := module.EffectiveTolerations(mod.Spec.Tolerations)

	// Sorted so that the errors returned for a given cluster state are always the same.
	names := make([]string, 0, len(nodesByName))
	for name := range nodesByName {
		names = append(names, name)
	}
	sort.Strings(names)

	var errs []error
	for _, name := range names {
		n := nodesByName[name]

		value, hasLabel := n.Labels[targetLabel]

		// Cordoning, an unrelated NoSchedule taint and a narrowed selector all leave running Pods
		// in place, so a node still in use overrides both conditions.
		wantLabel := forceKeep.Has(name) ||
			(selectedNames.Has(name) && nodeAPI.IsNodeSchedulable(n, tolerations))

		// The DaemonSet selects on targetLabel="", so the value is part of the desired state and a
		// node carrying any other one still has to be patched.
		converged := (wantLabel && hasLabel && value == "") || (!wantLabel && !hasLabel)
		if converged {
			continue
		}

		// A node deleted between the list and the patch needs no label, so its NotFound must not
		// hold back the DaemonSet and DeviceClass reconciliation that follows.
		if wantLabel {
			if err := nodeAPI.UpdateLabels(ctx, n, map[string]string{targetLabel: ""}, nil); k8serrors.IsNotFound(err) {
				continue
			} else if err != nil {
				errs = append(errs, fmt.Errorf("could not add %s label to node %s: %v", targetLabel, name, err))
			}
		} else {
			if err := nodeAPI.UpdateLabels(ctx, n, nil, map[string]string{targetLabel: ""}); k8serrors.IsNotFound(err) {
				continue
			} else if err != nil {
				errs = append(errs, fmt.Errorf("could not remove %s label from node %s: %v", targetLabel, name, err))
			}
		}
	}

	return errors.Join(errs...)
}

// ensureTargetNodeSelector adds targetLabel to the node selector of every DaemonSet passed in.
// Only the DaemonSet for the Module's current version goes through the setAsDesired helpers, so
// without this the DaemonSets an ordered upgrade left on older versions, and those created before
// the target label existed at all, would keep selecting nodes on the kernel-module-ready label
// alone, and cordoning one of their nodes would not evict their Pod. Nothing else is touched, so
// each DaemonSet keeps its own image and version selector.
func ensureTargetNodeSelector(ctx context.Context, clnt client.Client, existingDS []appsv1.DaemonSet, targetLabel string) error {
	logger := log.FromContext(ctx)

	var errs []error
	for i := range existingDS {
		ds := &existingDS[i]

		// A DaemonSet on its way out does not need migrating, and patching it would only race with
		// its deletion.
		if ds.GetDeletionTimestamp() != nil {
			continue
		}

		// The value matters as much as the key: nodes carry the label with an empty value, so a
		// DaemonSet asking for any other value would select no node at all, drop to zero desired
		// replicas and become eligible for garbage collection while its nodes still need it.
		if value, ok := ds.Spec.Template.Spec.NodeSelector[targetLabel]; ok && value == "" {
			continue
		}

		patchFrom := client.MergeFrom(ds.DeepCopy())

		if ds.Spec.Template.Spec.NodeSelector == nil {
			ds.Spec.Template.Spec.NodeSelector = make(map[string]string, 1)
		}
		ds.Spec.Template.Spec.NodeSelector[targetLabel] = ""

		if err := clnt.Patch(ctx, ds, patchFrom); k8serrors.IsNotFound(err) {
			continue
		} else if err != nil {
			errs = append(errs, fmt.Errorf("could not add %s to DaemonSet %s: %v", targetLabel, ds.Name, err))
			continue
		}

		logger.Info("Added the target node selector to an existing DaemonSet", "name", ds.Name, "label", targetLabel)
	}

	return errors.Join(errs...)
}

// removeTargetLabel removes targetLabel from every node carrying it. It selects on the label itself,
// so it also reaches nodes that the Module's selector no longer matches.
func removeTargetLabel(ctx context.Context, nodeAPI node.Node, targetLabel string) error {
	nodes, err := nodeAPI.GetAllNodesByLabelKey(ctx, targetLabel)
	if err != nil {
		return fmt.Errorf("could not list nodes with %s label: %v", targetLabel, err)
	}

	var errs []error
	for i := range nodes {
		n := &nodes[i]
		if err := nodeAPI.UpdateLabels(ctx, n, nil, map[string]string{targetLabel: ""}); k8serrors.IsNotFound(err) {
			continue
		} else if err != nil {
			errs = append(errs, fmt.Errorf("could not remove %s label from node %s: %v", targetLabel, n.Name, err))
		}
	}

	return errors.Join(errs...)
}
