# Reconciliation loop

## Modules

Each time a new `Module` is created, we need to find to which nodes it applies.

1. A first filtering is performed using the `.spec.selector` field, then we go through the module’s kernel mappings to find a container image that matches the node’s kernel.

1. We end up with a certain number of (kernel, image) pairs; for each of these pairs, there should be a `DaemonSet`.

1. We first look for a `DaemonSet` that would already be targeting the same kernel and DriverContainer image (that data is stored in the `DaemonSet`’s labels).

1. If there is already such a `DaemonSet`, we patch it, if needed.

1. If there is not already a matching `DaemonSet`, we create it and set the `Module` as owner.

1. When a `Module` is deleted, we have nothing to do: because we set it as owner of all `DaemonSets`, Kubernetes garbage
   collection will take care of deleting them.

1. We watch `Module`, owned `DaemonSet` and build objects as well as nodes to make sure that we are not missing any change
   in the cluster.

![Modules reconciliation](diagrams/reconciliation-module.png)

## DRA

A separate DRA reconciler watches `Module` resources that have `.spec.dra` set.

1. When a `Module` has `.spec.dra` configured, the DRA reconciler creates a DRA driver `DaemonSet` targeting nodes
   where the kernel module is loaded.

1. It also creates and manages cluster-scoped `DeviceClass` resources as declared in `.spec.dra.deviceClasses`.
   DeviceClasses are tracked via labels (`kmm.node.kubernetes.io/module.name` and
   `kmm.node.kubernetes.io/module.namespace`) since they are cluster-scoped while Modules are namespaced.

1. The reconciler also watches nodes and keeps a
   `kmm.node.kubernetes.io/<module-namespace>.<module-name>.dra-target` label on the schedulable nodes selected by the
   `Module`. The DRA `DaemonSet` requires that label on top of the kernel-module-ready one, so cordoning a node removes
   the DRA driver Pod before the kernel module is unloaded. The label is reconciled over both the nodes the `Module`
   selects and the nodes already carrying it, so narrowing the selector, or removing a selector label from a node, takes
   the label off too. Modules without a `.spec.moduleLoader` keep targeting `.spec.selector` directly, since they have
   no kernel module to unload. The device plugin `DaemonSet` works the same way through its own `device-plugin-target`
   label.

1. The reconciler watches `ResourceClaim` resources as well, and keeps the `dra-target` label on any node where a Pod
   still holds a claim allocated from this `Module`'s driver, whatever the node's taints and the `Module`'s selector
   say. kubelet calls the driver to unprepare those devices, so it has to outlive its consumers; the label is dropped
   once the last claim on the node is released.

1. When `spec.dra` is removed or the `Module` is deleted, the reconciler deletes all associated DRA `DaemonSet` and
   `DeviceClass` resources, and removes the `dra-target` label from every node carrying it.

1. During [ordered upgrades](../documentation/ordered_upgrade.md), a new DRA `DaemonSet` is created for the new module
   version. Once the old-version `DaemonSet` is no longer scheduled on any node, it is garbage-collected.
