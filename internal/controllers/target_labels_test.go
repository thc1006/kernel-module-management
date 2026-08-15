package controllers

import (
	"context"
	"fmt"

	kmmv1beta1 "github.com/kubernetes-sigs/kernel-module-management/api/v1beta1"
	"github.com/kubernetes-sigs/kernel-module-management/internal/client"
	"github.com/kubernetes-sigs/kernel-module-management/internal/module"
	"github.com/kubernetes-sigs/kernel-module-management/internal/node"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const targetLabel = "kmm.node.kubernetes.io/namespace.module.some-target"

func labeledNode(name string) v1.Node {
	return v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{targetLabel: ""}},
	}
}

var _ = Describe("reconcileTargetLabel", func() {
	var (
		ctrl        *gomock.Controller
		clnt        *client.MockClient
		nm          *node.MockNode
		ctx         context.Context
		mod         *kmmv1beta1.Module
		tolerations []v1.Toleration
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		clnt = client.NewMockClient(ctrl)
		nm = node.NewMockNode(ctrl)
		ctx = context.Background()
		mod = &kmmv1beta1.Module{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "module"},
			Spec:       kmmv1beta1.ModuleSpec{Selector: map[string]string{"worker": "true"}},
		}
		tolerations = module.EffectiveTolerations(mod.Spec.Tolerations)
	})

	It("should return an error when listing selected nodes fails", func() {
		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return(nil, fmt.Errorf("some error"))

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).NotTo(Succeed())
	})

	It("should return an error when listing labeled nodes fails", func() {
		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return(nil, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return(nil, fmt.Errorf("some error"))

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).NotTo(Succeed())
	})

	It("should add the label to a selected, schedulable node", func() {
		n := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return([]v1.Node{n}, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return(nil, nil)
		nm.EXPECT().IsNodeSchedulable(&n, tolerations).Return(true)
		nm.EXPECT().UpdateLabels(ctx, &n, map[string]string{targetLabel: ""}, nil).Return(nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).To(Succeed())
	})

	It("should remove the label from a selected but unschedulable node", func() {
		n := labeledNode("node1")

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return([]v1.Node{n}, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{n}, nil)
		nm.EXPECT().IsNodeSchedulable(gomock.Any(), tolerations).Return(false)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), nil, map[string]string{targetLabel: ""}).Return(nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).To(Succeed())
	})

	It("should remove the label from a node the selector no longer matches", func() {
		stale := labeledNode("stale-node")

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return(nil, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{stale}, nil)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), nil, map[string]string{targetLabel: ""}).Return(nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).To(Succeed())
	})

	It("should remove the label from an unselected node even while it is unschedulable", func() {
		stale := labeledNode("cordoned-stale-node")
		stale.Spec.Taints = []v1.Taint{{Key: v1.TaintNodeUnschedulable, Effect: v1.TaintEffectNoSchedule}}

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return(nil, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{stale}, nil)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), nil, map[string]string{targetLabel: ""}).Return(nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).To(Succeed())
	})

	It("should restore the label once a node matches the selector again", func() {
		n := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{"worker": "true"}}}

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return([]v1.Node{n}, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return(nil, nil)
		nm.EXPECT().IsNodeSchedulable(gomock.Any(), tolerations).Return(true)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), map[string]string{targetLabel: ""}, nil).Return(nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).To(Succeed())
	})

	It("should not patch any node once the cluster has converged", func() {
		labeled := labeledNode("labeled-and-selected")
		unlabeled := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "unlabeled-and-unschedulable"}}

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return([]v1.Node{labeled, unlabeled}, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{labeled}, nil)
		nm.EXPECT().IsNodeSchedulable(gomock.Any(), tolerations).DoAndReturn(
			func(n *v1.Node, _ []v1.Toleration) bool { return n.Name == labeled.Name },
		).Times(2)

		// No UpdateLabels expectation: any patch here fails the test.
		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).To(Succeed())
	})

	DescribeTable("should keep the label on a node whose taint does not make it a non-target",
		func(taint v1.Taint, modTolerations []v1.Toleration) {
			mod.Spec.Tolerations = modTolerations
			taintedNode := v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "tainted-node"},
				Spec:       v1.NodeSpec{Taints: []v1.Taint{taint}},
			}

			gomock.InOrder(
				clnt.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ interface{}, list *v1.NodeList, _ ...interface{}) error {
						list.Items = []v1.Node{taintedNode}
						return nil
					},
				),
				clnt.EXPECT().List(ctx, gomock.Any(), gomock.Any()).Return(nil),
				clnt.EXPECT().Patch(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, n *v1.Node, _ ctrlclient.Patch, _ ...ctrlclient.PatchOption) error {
						Expect(n.Labels).To(HaveKeyWithValue(targetLabel, ""))
						return nil
					},
				),
			)

			Expect(reconcileTargetLabel(ctx, node.NewNode(clnt), mod, targetLabel, nil)).To(Succeed())
		},
		Entry(
			"cordoned, but the Module tolerates it",
			v1.Taint{Key: v1.TaintNodeUnschedulable, Effect: v1.TaintEffectNoSchedule},
			[]v1.Toleration{{Key: v1.TaintNodeUnschedulable, Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule}},
		),
		Entry(
			"under memory pressure, which the module reconciler tolerates internally",
			v1.Taint{Key: v1.TaintNodeMemoryPressure, Effect: v1.TaintEffectNoSchedule},
			nil,
		),
		Entry(
			"under disk pressure, which the module reconciler tolerates internally",
			v1.Taint{Key: v1.TaintNodeDiskPressure, Effect: v1.TaintEffectNoSchedule},
			nil,
		),
		Entry(
			"under PID pressure, which the module reconciler tolerates internally",
			v1.Taint{Key: v1.TaintNodePIDPressure, Effect: v1.TaintEffectNoSchedule},
			nil,
		),
	)

	It("should remove the label from a node carrying an untolerated taint", func() {
		taintedNode := labeledNode("tainted-node")
		taintedNode.Labels["worker"] = "true"
		taintedNode.Spec.Taints = []v1.Taint{{Key: "dedicated", Effect: v1.TaintEffectNoSchedule}}

		gomock.InOrder(
			clnt.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ interface{}, list *v1.NodeList, _ ...interface{}) error {
					list.Items = []v1.Node{taintedNode}
					return nil
				},
			),
			clnt.EXPECT().List(ctx, gomock.Any(), gomock.Any()).Return(nil),
			clnt.EXPECT().Patch(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, n *v1.Node, _ ctrlclient.Patch, _ ...ctrlclient.PatchOption) error {
					Expect(n.Labels).NotTo(HaveKey(targetLabel))
					return nil
				},
			),
		)

		Expect(reconcileTargetLabel(ctx, node.NewNode(clnt), mod, targetLabel, nil)).To(Succeed())
	})

	It("should normalize a target label whose value is not empty", func() {
		n := v1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node1", Labels: map[string]string{targetLabel: "corrupted"}},
		}

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return([]v1.Node{n}, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{n}, nil)
		nm.EXPECT().IsNodeSchedulable(gomock.Any(), tolerations).Return(true)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), map[string]string{targetLabel: ""}, nil).Return(nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).To(Succeed())
	})

	It("should remove a non-empty target label from a node the selector no longer matches", func() {
		n := v1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "stale", Labels: map[string]string{targetLabel: "corrupted"}},
		}

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return(nil, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{n}, nil)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), nil, map[string]string{targetLabel: ""}).Return(nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).To(Succeed())
	})

	It("should continue processing nodes if one fails and return a combined error", func() {
		node1 := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}}
		node2 := labeledNode("node2")

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return([]v1.Node{node1, node2}, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{node2}, nil)
		nm.EXPECT().IsNodeSchedulable(gomock.Any(), tolerations).DoAndReturn(
			func(n *v1.Node, _ []v1.Toleration) bool { return n.Name == "node1" },
		).Times(2)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), map[string]string{targetLabel: ""}, nil).Return(fmt.Errorf("conflict"))
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), nil, map[string]string{targetLabel: ""}).Return(fmt.Errorf("conflict"))

		err := reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("node1"))
		Expect(err.Error()).To(ContainSubstring("node2"))
	})
})

var _ = Describe("removeTargetLabel", func() {
	var (
		ctrl *gomock.Controller
		nm   *node.MockNode
		ctx  context.Context
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		nm = node.NewMockNode(ctrl)
		ctx = context.Background()
	})

	It("should return an error when listing nodes fails", func() {
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return(nil, fmt.Errorf("some error"))

		Expect(removeTargetLabel(ctx, nm, targetLabel)).NotTo(Succeed())
	})

	It("should select nodes by the label rather than by a Module selector", func() {
		n := labeledNode("labeled-node")

		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{n}, nil)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), nil, map[string]string{targetLabel: ""}).Return(nil)

		Expect(removeTargetLabel(ctx, nm, targetLabel)).To(Succeed())
	})

	It("should not fail when a node being unlabeled has already gone", func() {
		n := labeledNode("going-away")

		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{n}, nil)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), nil, map[string]string{targetLabel: ""}).
			Return(fmt.Errorf("could not patch node: %w", k8serrors.NewNotFound(v1.Resource("nodes"), n.Name)))

		Expect(removeTargetLabel(ctx, nm, targetLabel)).To(Succeed())
	})

	It("should continue processing nodes if one fails and return a combined error", func() {
		node1 := labeledNode("node1")
		node2 := labeledNode("node2")

		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{node1, node2}, nil)
		nm.EXPECT().UpdateLabels(ctx, &node1, nil, map[string]string{targetLabel: ""}).Return(fmt.Errorf("conflict"))
		nm.EXPECT().UpdateLabels(ctx, &node2, nil, map[string]string{targetLabel: ""}).Return(nil)

		err := removeTargetLabel(ctx, nm, targetLabel)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("node1"))
		Expect(err.Error()).NotTo(ContainSubstring("node2"))
	})
})

var _ = Describe("reconcileTargetLabel node deletion race", func() {
	var (
		ctrl *gomock.Controller
		clnt *client.MockClient
		ctx  context.Context
		mod  *kmmv1beta1.Module
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		clnt = client.NewMockClient(ctrl)
		ctx = context.Background()
		mod = &kmmv1beta1.Module{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "module"},
			Spec:       kmmv1beta1.ModuleSpec{Selector: map[string]string{"worker": "true"}},
		}
	})

	// These go through the real node API rather than a mock, so that the error wrapping it relies
	// on is covered too.
	DescribeTable("should not fail the reconciliation when a node disappears mid-flight",
		func(selected, labeled []v1.Node) {
			gomock.InOrder(
				clnt.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ interface{}, list *v1.NodeList, _ ...interface{}) error {
						list.Items = selected
						return nil
					},
				),
				clnt.EXPECT().List(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ interface{}, list *v1.NodeList, _ ...interface{}) error {
						list.Items = labeled
						return nil
					},
				),
				clnt.EXPECT().Patch(ctx, gomock.Any(), gomock.Any()).
					Return(k8serrors.NewNotFound(v1.Resource("nodes"), "going-away")),
			)

			Expect(reconcileTargetLabel(ctx, node.NewNode(clnt), mod, targetLabel, nil)).To(Succeed())
		},
		Entry("while the label is being added",
			[]v1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "going-away"}}},
			nil,
		),
		Entry("while the label is being removed",
			nil,
			[]v1.Node{labeledNode("going-away")},
		),
	)
})

var _ = Describe("ensureTargetNodeSelector", func() {
	var (
		ctrl *gomock.Controller
		clnt *client.MockClient
		ctx  context.Context
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		clnt = client.NewMockClient(ctrl)
		ctx = context.Background()
	})

	dsWithSelector := func(name string, selector map[string]string) appsv1.DaemonSet {
		return appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: appsv1.DaemonSetSpec{
				Template: v1.PodTemplateSpec{Spec: v1.PodSpec{NodeSelector: selector}},
			},
		}
	}

	It("should add the target selector to every DaemonSet missing it", func() {
		const (
			readyLabel   = "kmm.node.kubernetes.io/namespace.module.ready"
			versionLabel = "beta.kmm.node.kubernetes.io/version-schedule-pod.namespace.module"
		)

		existing := []appsv1.DaemonSet{
			dsWithSelector("old-version", map[string]string{readyLabel: "", versionLabel: "1"}),
			dsWithSelector("current-version", map[string]string{readyLabel: "", versionLabel: "2"}),
		}

		patched := make(map[string]map[string]string)
		clnt.EXPECT().Patch(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, ds *appsv1.DaemonSet, _ ctrlclient.Patch, _ ...ctrlclient.PatchOption) error {
				patched[ds.Name] = ds.Spec.Template.Spec.NodeSelector
				return nil
			},
		).Times(2)

		Expect(ensureTargetNodeSelector(ctx, clnt, existing, targetLabel)).To(Succeed())

		// Each DaemonSet keeps its own version selector and only gains the shared target label.
		Expect(patched).To(Equal(map[string]map[string]string{
			"old-version":     {readyLabel: "", versionLabel: "1", targetLabel: ""},
			"current-version": {readyLabel: "", versionLabel: "2", targetLabel: ""},
		}))
	})

	It("should populate an empty node selector", func() {
		clnt.EXPECT().Patch(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, ds *appsv1.DaemonSet, _ ctrlclient.Patch, _ ...ctrlclient.PatchOption) error {
				Expect(ds.Spec.Template.Spec.NodeSelector).To(Equal(map[string]string{targetLabel: ""}))
				return nil
			},
		)

		Expect(ensureTargetNodeSelector(ctx, clnt, []appsv1.DaemonSet{dsWithSelector("no-selector", nil)}, targetLabel)).To(Succeed())
	})

	It("should normalize a target selector whose value is not empty", func() {
		existing := []appsv1.DaemonSet{dsWithSelector("corrupted", map[string]string{targetLabel: "wrong"})}

		clnt.EXPECT().Patch(ctx, gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, ds *appsv1.DaemonSet, _ ctrlclient.Patch, _ ...ctrlclient.PatchOption) error {
				Expect(ds.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue(targetLabel, ""))
				return nil
			},
		)

		Expect(ensureTargetNodeSelector(ctx, clnt, existing, targetLabel)).To(Succeed())
	})

	It("should skip a DaemonSet that is already being deleted", func() {
		ds := dsWithSelector("going-away", nil)
		ds.SetDeletionTimestamp(&metav1.Time{})

		// No Patch expectation: any call fails the test.
		Expect(ensureTargetNodeSelector(ctx, clnt, []appsv1.DaemonSet{ds}, targetLabel)).To(Succeed())
	})

	It("should not fail when a DaemonSet disappears mid-flight", func() {
		existing := []appsv1.DaemonSet{dsWithSelector("going-away", nil)}

		clnt.EXPECT().Patch(ctx, gomock.Any(), gomock.Any()).
			Return(k8serrors.NewNotFound(appsv1.Resource("daemonsets"), "going-away"))

		Expect(ensureTargetNodeSelector(ctx, clnt, existing, targetLabel)).To(Succeed())
	})

	It("should not patch a DaemonSet that already has the target selector", func() {
		existing := []appsv1.DaemonSet{dsWithSelector("already-migrated", map[string]string{targetLabel: ""})}

		// No Patch expectation: any call fails the test.
		Expect(ensureTargetNodeSelector(ctx, clnt, existing, targetLabel)).To(Succeed())
	})

	It("should keep going and aggregate errors when a patch fails", func() {
		existing := []appsv1.DaemonSet{dsWithSelector("first", nil), dsWithSelector("second", nil)}

		clnt.EXPECT().Patch(ctx, gomock.Any(), gomock.Any()).Return(fmt.Errorf("conflict"))
		clnt.EXPECT().Patch(ctx, gomock.Any(), gomock.Any()).Return(nil)

		err := ensureTargetNodeSelector(ctx, clnt, existing, targetLabel)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("first"))
		Expect(err.Error()).NotTo(ContainSubstring("second"))
	})
})

var _ = Describe("reconcileTargetLabel in-use override", func() {
	var (
		ctrl *gomock.Controller
		nm   *node.MockNode
		ctx  context.Context
		mod  *kmmv1beta1.Module
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		nm = node.NewMockNode(ctrl)
		ctx = context.Background()
		mod = &kmmv1beta1.Module{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "module"},
			Spec:       kmmv1beta1.ModuleSpec{Selector: map[string]string{"worker": "true"}},
		}
	})

	It("should keep the label on a cordoned node that is still in use", func() {
		n := labeledNode("cordoned-but-in-use")
		n.Spec.Taints = []v1.Taint{{Key: v1.TaintNodeUnschedulable, Effect: v1.TaintEffectNoSchedule}}

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return([]v1.Node{n}, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{n}, nil)

		// No IsNodeSchedulable and no UpdateLabels: being in use settles it on its own.
		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, sets.New(n.Name))).To(Succeed())
	})

	It("should keep the label on a node the selector no longer matches while it is still in use", func() {
		n := labeledNode("unselected-but-in-use")

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return(nil, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{n}, nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, sets.New(n.Name))).To(Succeed())
	})

	It("should add the label back to an in-use node that has lost it", func() {
		n := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "in-use-unlabeled"}}

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return([]v1.Node{n}, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return(nil, nil)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), map[string]string{targetLabel: ""}, nil).Return(nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, sets.New(n.Name))).To(Succeed())
	})

	It("should drop the label once the node stops being in use", func() {
		n := labeledNode("no-longer-in-use")
		n.Spec.Taints = []v1.Taint{{Key: v1.TaintNodeUnschedulable, Effect: v1.TaintEffectNoSchedule}}

		nm.EXPECT().GetAllNodesBySelector(ctx, mod.Spec.Selector).Return([]v1.Node{n}, nil)
		nm.EXPECT().GetAllNodesByLabelKey(ctx, targetLabel).Return([]v1.Node{n}, nil)
		nm.EXPECT().IsNodeSchedulable(gomock.Any(), gomock.Any()).Return(false)
		nm.EXPECT().UpdateLabels(ctx, gomock.Any(), nil, map[string]string{targetLabel: ""}).Return(nil)

		Expect(reconcileTargetLabel(ctx, nm, mod, targetLabel, nil)).To(Succeed())
	})
})
