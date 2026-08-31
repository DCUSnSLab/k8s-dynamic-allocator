#!/bin/sh

set +e

echo '[Diagnostics] Workloads'
kubectl get pods,deployments,daemonsets,services,persistentvolumeclaims \
    -n "${PRODUCTION_NAMESPACE}" -o wide

echo '[Diagnostics] Deployments'
kubectl describe deployment/controller \
    -n "${PRODUCTION_NAMESPACE}"
kubectl describe deployment/compute-general \
    -n "${PRODUCTION_NAMESPACE}"
kubectl describe deployment/controller-queue-redis \
    -n "${PRODUCTION_NAMESPACE}"
kubectl describe deployment/swlabssh \
    -n "${PRODUCTION_NAMESPACE}"
kubectl describe daemonset/fluent-bit \
    -n "${PRODUCTION_NAMESPACE}"

echo '[Diagnostics] Recent events'
kubectl get events -n "${PRODUCTION_NAMESPACE}" \
    --sort-by=.metadata.creationTimestamp | tail -n 100

echo '[Diagnostics] Controller logs'
kubectl logs -n "${PRODUCTION_NAMESPACE}" \
    -l app=controller --all-containers=true --prefix=true --tail=150

echo '[Diagnostics] Compute logs'
kubectl logs -n "${PRODUCTION_NAMESPACE}" \
    -l app=warm-pod-pool --all-containers=true --prefix=true --tail=150

echo '[Diagnostics] Redis logs'
kubectl logs -n "${PRODUCTION_NAMESPACE}" \
    -l app=controller-queue-redis --all-containers=true --prefix=true --tail=150

echo '[Diagnostics] swlabssh logs'
kubectl logs -n "${PRODUCTION_NAMESPACE}" \
    -l kubessh=swlabssh --all-containers=true --prefix=true --tail=150

echo '[Diagnostics] Fluent Bit logs'
kubectl logs -n "${PRODUCTION_NAMESPACE}" \
    -l app=fluent-bit --all-containers=true --prefix=true --tail=150

exit 0
