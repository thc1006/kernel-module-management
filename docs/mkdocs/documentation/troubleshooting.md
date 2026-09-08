# Troubleshooting

## Reading logs

In the commands below, the value of `$namespace` depends on your [installation method](install.md).

### Operator

| Component | Command                                                                 |
|-----------|-------------------------------------------------------------------------|
| KMM       | `kubectl logs -fn "$namespace" deployments/kmm-operator-controller`     |
| KMM-Hub   | `kubectl logs -fn "$namespace" deployments/kmm-operator-hub-controller` |

### Webhook server

| Component | Command                                                                     |
|-----------|-----------------------------------------------------------------------------|
| KMM       | `kubectl logs -fn "$namespace" deployments/kmm-operator-webhook`     |
| KMM-Hub   | `kubectl logs -fn "$namespace" deployments/kmm-operator-hub-webhook` |

## Observing events

### Build & Sign

KMM publishes events whenever it starts a kmod image build or observes its outcome.  
Those events are attached to `Module` objects and are available at the very end of `kubectl describe module`:

```text
$> kubectl describe modules.kmm.sigs.x-k8s.io kmm-ci-a
[...]
Events:
  Type    Reason          Age                From  Message
  ----    ------          ----               ----  -------
  Normal  BuildCreated    2m29s              kmm   Build created for kernel 6.6.2-201.fc39.x86_64
  Normal  BuildSucceeded  63s                kmm   Build job succeeded for kernel 6.6.2-201.fc39.x86_64
  Normal  SignCreated     64s (x2 over 64s)  kmm   Sign created for kernel 6.6.2-201.fc39.x86_64
  Normal  SignSucceeded   57s                kmm   Sign job succeeded for kernel 6.6.2-201.fc39.x86_64
```

### Module load or unload

KMM publishes events whenever it successfully loads or unloads a kernel module on a node.  
Those events are attached to `Node` objects and are available at the very end of `kubectl describe node`:

```text
$> kubectl describe node my-node
[...]
Events:
  Type    Reason          Age    From  Message
  ----    ------          ----   ----  -------
[...]
  Normal  ModuleLoaded    4m17s  kmm   Module default/kmm-ci-a loaded into the kernel
  Normal  ModuleUnloaded  2s     kmm   Module default/kmm-ci-a unloaded from the kernel
```

## Deleting a Module

KMM holds a `Module` until the resources it created for it have gone, so a `Module` stays in `Terminating` until then.  
The finalizers left on it say what is still outstanding:

| Finalizer                                      | Waits for                                                                |
|------------------------------------------------|--------------------------------------------------------------------------|
| `kmm.node.kubernetes.io/module-finalizer`      | no `NodeModulesConfig` still listing the `Module` as in use              |
| `kmm.node.kubernetes.io/dra-cleanup`           | the DRA DaemonSets, their Pods and the `DeviceClass` objects             |
| `kmm.node.kubernetes.io/device-plugin-cleanup` | the device plugin DaemonSets, their Pods and the labels they set on nodes |

```text
$> kubectl get modules.kmm.sigs.x-k8s.io kmm-ci-a -o jsonpath='{.metadata.finalizers}'
["kmm.node.kubernetes.io/dra-cleanup"]
```

The operator names what it can still see on every pass:

```text
$> kubectl logs -fn "$namespace" deployments/kmm-operator-controller
[...]
"Waiting for the DRA resources to go before releasing the Module" [...] daemonSets=0 podsLeft=false deviceClasses=1
```

A DaemonSet is deleted in the foreground: it stays listed with a deletion timestamp until the Pods it owns have gone, which is not on its own a sign that the cleanup is stuck.  
Removing a finalizer by hand releases the `Module` but leaves those resources behind, with nothing left to collect them.
