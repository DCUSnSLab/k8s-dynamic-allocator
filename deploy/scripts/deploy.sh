#!/bin/sh

set -eu

namespace="${DEPLOY_NAMESPACE}"
storage_class="${DEPLOY_STORAGE_CLASS}"
overlay="${DEPLOY_OVERLAY}"
stage_label="${DEPLOY_STAGE_LABEL}"
pool_available_min="${POOL_AVAILABLE_MIN}"
pool_total_max="${POOL_TOTAL_MAX}"
pool_annotation_r='k8s-dynamic-allocator/pool-available-min'
pool_annotation_n='k8s-dynamic-allocator/pool-total-max'
rendered="${WORKSPACE}/.kda-deploy-${BUILD_NUMBER}.yaml"
kustomization="${overlay}/kustomization.yaml"
kustomization_backup="${WORKSPACE}/.kda-deploy-kustomization-${BUILD_NUMBER}.bak"
all_resources="${WORKSPACE}/.kda-deploy-resources-${BUILD_NUMBER}.txt"
staged_resources="${WORKSPACE}/.kda-deploy-staged-resources-${BUILD_NUMBER}.txt"

for required_command in kubectl sed grep sort diff wc tr; do
    if ! command -v "${required_command}" >/dev/null 2>&1; then
        echo "[PreflightFailed] required command is missing: ${required_command}"
        exit 1
    fi
done

# Mirror the controller's own fail-closed rule (0 <= R <= N) so a bad build
# parameter is rejected before anything is applied to the cluster.
for policy_value in "${pool_available_min}" "${pool_total_max}"; do
    case "${policy_value}" in
        ''|*[!0-9]*)
            echo "[PreflightFailed] pool policy must be non-negative integers: R=${pool_available_min} N=${pool_total_max}"
            exit 1
            ;;
    esac
done
if [ "${pool_available_min}" -gt "${pool_total_max}" ]; then
    echo "[PreflightFailed] pool policy must satisfy R <= N: R=${pool_available_min} N=${pool_total_max}"
    exit 1
fi

cleanup_workspace() {
    if [ -f "${kustomization_backup}" ]; then
        mv "${kustomization_backup}" "${kustomization}"
    fi
    rm -f \
        "${rendered}" \
        "${all_resources}" \
        "${staged_resources}"
}
trap cleanup_workspace 0

require_can_i() {
    verb="$1"
    resource="$2"
    scope_args="$3"
    answer=$(kubectl auth can-i "${verb}" "${resource}" ${scope_args})
    if [ "${answer}" != "yes" ]; then
        echo "[PreflightFailed] kubectl auth can-i ${verb} ${resource} ${scope_args}: ${answer}"
        exit 1
    fi
}

verify_existing_pvc_storage_class() {
    pvc="$1"
    existing_class=$(kubectl get pvc "${pvc}" -n "${namespace}" \
        --ignore-not-found \
        -o jsonpath='{.spec.storageClassName}')
    if [ -n "${existing_class}" ] && [ "${existing_class}" != "${storage_class}" ]; then
        echo "[PreflightFailed] pvc/${pvc} uses storageClass=${existing_class}, expected=${storage_class}"
        exit 1
    fi
}

apply_stage() {
    stage="$1"
    echo "[Deploy] stage=${stage}"
    kubectl apply \
        -n "${namespace}" \
        -f "${rendered}" \
        -l "${stage_label}=${stage}"
}

echo '[Preflight] Verify cluster prerequisites'
kubectl get namespace "${namespace}" >/dev/null
kubectl get storageclass "${storage_class}" >/dev/null
kubectl get secret compute-ssh-key -n "${namespace}" >/dev/null
kubectl get configmap compute-public-key -n "${namespace}" >/dev/null

private_key_present=$(kubectl get secret compute-ssh-key \
    -n "${namespace}" \
    -o go-template='{{if index .data "id_rsa"}}yes{{end}}')
secret_public_key_present=$(kubectl get secret compute-ssh-key \
    -n "${namespace}" \
    -o go-template='{{if index .data "id_rsa.pub"}}yes{{end}}')
public_key_present=$(kubectl get configmap compute-public-key \
    -n "${namespace}" \
    -o go-template='{{if index .data "id_rsa.pub"}}yes{{end}}')
test "${private_key_present}" = yes
test "${secret_public_key_present}" = yes
test "${public_key_present}" = yes

require_can_i get deployments.apps "-n ${namespace}"
require_can_i create deployments.apps "-n ${namespace}"
require_can_i patch deployments.apps "-n ${namespace}"
require_can_i create daemonsets.apps "-n ${namespace}"
require_can_i patch daemonsets.apps "-n ${namespace}"
require_can_i watch deployments.apps "-n ${namespace}"
require_can_i watch daemonsets.apps "-n ${namespace}"
require_can_i get pods "-n ${namespace}"
require_can_i list pods "-n ${namespace}"
require_can_i create pods/exec "-n ${namespace}"
require_can_i get services "-n ${namespace}"
require_can_i get endpoints "-n ${namespace}"
require_can_i create clusterroles.rbac.authorization.k8s.io ""
require_can_i patch clusterroles.rbac.authorization.k8s.io ""
require_can_i create clusterrolebindings.rbac.authorization.k8s.io ""
require_can_i patch clusterrolebindings.rbac.authorization.k8s.io ""

verify_existing_pvc_storage_class controller-queue-redis-data
verify_existing_pvc_storage_class logs-pvc

test -f "${kustomization}"
cp "${kustomization}" "${kustomization_backup}"
sed -i "s|newTag: latest|newTag: \"${IMAGE_TAG}\"|g" "${kustomization}"

if grep -Eq '^[[:space:]]*newTag:[[:space:]]*"?latest"?[[:space:]]*$' "${kustomization}"; then
    echo '[RenderFailed] overlay still contains newTag: latest'
    exit 1
fi

kubectl kustomize "${overlay}" > "${rendered}"
test -s "${rendered}"
grep -F "${CONTROLLER_IMAGE}" "${rendered}" >/dev/null
grep -F "${SWLABSSH_IMAGE}" "${rendered}" >/dev/null

echo '[Validate] Client render and deploy-stage coverage'
kubectl apply --dry-run=client \
    -n "${namespace}" \
    -f "${rendered}" \
    -o name \
    | sort -u > "${all_resources}"

test -s "${all_resources}"

# The workloads the ordered rollout below waits on must exist in every
# environment. Anything else may be added or removed without editing this
# script; the stage-coverage check keeps such additions honest.
for required_resource in \
    deployment.apps/controller \
    deployment.apps/controller-queue-redis \
    deployment.apps/swlabssh \
    daemonset.apps/fluent-bit \
    service/controller-service \
    service/swlabssh
do
    if ! grep -Fxq "${required_resource}" "${all_resources}"; then
        echo "[ValidateFailed] required resource is missing from the render: ${required_resource}"
        cat "${all_resources}"
        exit 1
    fi
done

generated_config_count=$(grep -Ec '^configmap/fluent-bit-config-[a-z0-9]{10}$' "${all_resources}" || true)
if [ "${generated_config_count}" -ne 1 ]; then
    echo '[ValidateFailed] expected exactly one generated fluent-bit ConfigMap'
    cat "${all_resources}"
    exit 1
fi

# Every rendered resource must carry exactly one supported deploy-stage label.
# This is the invariant that matters: a newly added resource without a stage
# label would silently never be applied.
: > "${staged_resources}"
for stage in bootstrap logging redis controller swlabssh; do
    kubectl apply --dry-run=client \
        -n "${namespace}" \
        -f "${rendered}" \
        -l "${stage_label}=${stage}" \
        -o name \
        | grep -v '^$' >> "${staged_resources}"
done
sort -o "${staged_resources}" "${staged_resources}"

staged_total=$(wc -l < "${staged_resources}" | tr -d ' ')
staged_unique=$(sort -u "${staged_resources}" | wc -l | tr -d ' ')
if [ "${staged_total}" -ne "${staged_unique}" ]; then
    echo '[ValidateFailed] a resource carries more than one deploy-stage label'
    sort "${staged_resources}" | uniq -d
    exit 1
fi

if ! diff -u "${all_resources}" "${staged_resources}"; then
    echo '[ValidateFailed] every rendered resource must have exactly one supported deploy-stage label'
    exit 1
fi

echo '[Validate] Server-side dry run'
kubectl apply --dry-run=server \
    -n "${namespace}" \
    -f "${rendered}" >/dev/null

apply_stage bootstrap

# Logging goes first so Fluent Bit is tailing before the workloads it collects
# start. Its tail input has no Read_from_Head, so anything logged before the
# DaemonSet is up is not collected.
apply_stage logging
kubectl rollout status daemonset/fluent-bit \
    -n "${namespace}" --timeout=5m

apply_stage redis
kubectl rollout status deployment/controller-queue-redis \
    -n "${namespace}" --timeout=5m

apply_stage controller
kubectl rollout status deployment/controller \
    -n "${namespace}" --timeout=5m

echo '[Deploy] Wait for controller-managed compute-general Deployment'
attempt=0
while ! kubectl get deployment/compute-general -n "${namespace}" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "${attempt}" -ge 60 ]; then
        echo '[DeployFailed] deployment/compute-general was not created within 120 seconds'
        exit 1
    fi
    sleep 2
done

# R/N live on the Deployment's metadata annotations. The controller watches
# them and never overwrites an operator-supplied value, so applying the build
# parameters here is enough - no controller restart and no pod churn.
echo "[Deploy] Apply pool policy R=${pool_available_min} N=${pool_total_max}"
kubectl annotate deployment/compute-general \
    "${pool_annotation_r}=${pool_available_min}" \
    "${pool_annotation_n}=${pool_total_max}" \
    -n "${namespace}" --overwrite

kubectl set image deployment/compute-general \
    compute-agent="${COMPUTE_POD_IMAGE}" \
    -n "${namespace}"
kubectl rollout status deployment/compute-general \
    -n "${namespace}" --timeout=5m

echo '[Smoke] Wait for shared pool policy to become active'
attempt=0
until kubectl exec -n "${namespace}" deployment/controller -- \
    python -c '
import json
import sys
import urllib.request

expected_r = int(sys.argv[1])
expected_n = int(sys.argv[2])

with urllib.request.urlopen(
    "http://127.0.0.1:9001/api/pool/status/",
    timeout=5,
) as response:
    status = json.load(response)

general = (status.get("pools") or {}).get("general") or {}
ready = (
    status.get("policy_ready") is True
    and general.get("policy_valid") is True
    and general.get("policy_cached") is True
    and general.get("R") == expected_r
    and general.get("N") == expected_n
)
sys.exit(0 if ready else 1)
' "${pool_available_min}" "${pool_total_max}" >/dev/null
do
    attempt=$((attempt + 1))
    if [ "${attempt}" -ge 40 ]; then
        echo '[SmokeFailed] shared pool policy was not active within 120 seconds'
        kubectl exec -n "${namespace}" deployment/controller -- \
            python -c '
import json
import urllib.request

with urllib.request.urlopen(
    "http://127.0.0.1:9001/api/pool/status/",
    timeout=5,
) as response:
    print(json.dumps(json.load(response), sort_keys=True))
' || true
        exit 1
    fi
    sleep 3
done

apply_stage swlabssh
kubectl rollout status deployment/swlabssh \
    -n "${namespace}" --timeout=5m

echo '[Smoke] Verify deployed images and service readiness'
actual_controller_image=$(kubectl get deployment/controller \
    -n "${namespace}" \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="controller")].image}')
actual_compute_image=$(kubectl get deployment/compute-general \
    -n "${namespace}" \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="compute-agent")].image}')
actual_swlabssh_image=$(kubectl get deployment/swlabssh \
    -n "${namespace}" \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="swlabssh")].image}')

test "${actual_controller_image}" = "${CONTROLLER_IMAGE}"
test "${actual_compute_image}" = "${COMPUTE_POD_IMAGE}"
test "${actual_swlabssh_image}" = "${SWLABSSH_IMAGE}"

actual_pool_available_min=$(kubectl get deployment/compute-general \
    -n "${namespace}" \
    -o go-template="{{index .metadata.annotations \"${pool_annotation_r}\"}}")
actual_pool_total_max=$(kubectl get deployment/compute-general \
    -n "${namespace}" \
    -o go-template="{{index .metadata.annotations \"${pool_annotation_n}\"}}")
test "${actual_pool_available_min}" = "${pool_available_min}"
test "${actual_pool_total_max}" = "${pool_total_max}"

runtime_compute_image=$(kubectl exec -n "${namespace}" \
    deployment/controller -- \
    sh -c 'printf "%s" "$COMPUTE_POD_IMAGE"')
runtime_user_image=$(kubectl exec -n "${namespace}" \
    deployment/swlabssh -- \
    sh -c 'printf "%s" "$USER_POD_IMAGE"')
runtime_user_storage_class=$(kubectl exec -n "${namespace}" \
    deployment/swlabssh -- \
    sh -c 'printf "%s" "$USER_POD_STORAGE_CLASS"')
test "${runtime_compute_image}" = "${COMPUTE_POD_IMAGE}"
test "${runtime_user_image}" = "${USER_POD_IMAGE}"
test "${runtime_user_storage_class}" = "${storage_class}"

ssh_node_port=$(kubectl get service/swlabssh \
    -n "${namespace}" \
    -o jsonpath='{.spec.ports[?(@.name=="ssh")].nodePort}')
test "${ssh_node_port}" = "${DEPLOY_SSH_PORT}"

redis_pod=$(kubectl get pods -n "${namespace}" \
    -l app=controller-queue-redis \
    -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n "${namespace}" "${redis_pod}" -- redis-cli ping \
    | grep -Fx PONG >/dev/null

kubectl exec -n "${namespace}" deployment/controller -- \
    python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9001/api/health/', timeout=5).read()"

controller_endpoints=$(kubectl get endpoints controller-service \
    -n "${namespace}" \
    -o jsonpath='{.subsets[*].addresses[*].ip}')
swlabssh_endpoints=$(kubectl get endpoints swlabssh \
    -n "${namespace}" \
    -o jsonpath='{.subsets[*].addresses[*].ip}')
test -n "${controller_endpoints}"
test -n "${swlabssh_endpoints}"

fluent_bit_desired=$(kubectl get daemonset/fluent-bit \
    -n "${namespace}" \
    -o jsonpath='{.status.desiredNumberScheduled}')
fluent_bit_ready=$(kubectl get daemonset/fluent-bit \
    -n "${namespace}" \
    -o jsonpath='{.status.numberReady}')
test "${fluent_bit_desired}" -gt 0
test "${fluent_bit_ready}" -eq "${fluent_bit_desired}"

echo "[Success] image tag=${IMAGE_TAG}"
echo "[Success] pool policy R=${pool_available_min} N=${pool_total_max}"
echo "[Success] SSH endpoint=${DEPLOY_SSH_HOST}:${DEPLOY_SSH_PORT}"
