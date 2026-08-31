#!/bin/sh

set -eu

command -v docker >/dev/null 2>&1 || {
    echo '[VerifyFailed] required command is missing: docker'
    exit 1
}

for image in \
    "${CONTROLLER_IMAGE}" \
    "${COMPUTE_POD_IMAGE}" \
    "${USER_POD_IMAGE}" \
    "${SWLABSSH_IMAGE}"
do
    docker image inspect "${image}" >/dev/null
done

echo '[Verify] Controller image and compute manifest contract'
docker run --rm -i \
    -e EXPECTED_COMPUTE_POD_IMAGE="${COMPUTE_POD_IMAGE}" \
    "${CONTROLLER_IMAGE}" \
    python - <<'PY'
import os
import sys

import yaml

sys.path.insert(0, "/app/rest_api")

from config import settings
from services.compute.manifest_images import override_compute_agent_image

expected_image = os.environ["EXPECTED_COMPUTE_POD_IMAGE"]
assert settings.COMPUTE_POD_IMAGE == expected_image

for path in (
    "/app/manifests/compute-general.yaml",
    "/app/manifests/cold_start/compute-general-pod.yaml",
):
    with open(path, encoding="utf-8") as manifest_file:
        workload = yaml.safe_load(manifest_file)

    override_compute_agent_image(workload, settings.COMPUTE_POD_IMAGE)
    if workload["kind"] == "Deployment":
        pod_spec = workload["spec"]["template"]["spec"]
    else:
        pod_spec = workload["spec"]

    compute_agent = next(
        container
        for container in pod_spec["containers"]
        if container["name"] == "compute-agent"
    )
    assert compute_agent["image"] == expected_image

print("Controller compute image runtime contract: OK")
PY

echo '[Verify] swlabssh image and new User Pod/PVC contract'
docker run --rm -i \
    -e K8S_NAMESPACE="${PRODUCTION_NAMESPACE}" \
    -e USER_POD_STORAGE_CLASS="${PRODUCTION_STORAGE_CLASS}" \
    -e EXPECTED_USER_POD_IMAGE="${USER_POD_IMAGE}" \
    "${SWLABSSH_IMAGE}" \
    python - <<'PY'
import os
import runpy

import kubernetes.config

kubernetes.config.load_incluster_config = lambda: None
kubernetes.config.load_kube_config = lambda: None

from kubessh.pod import UserPod

expected_image = os.environ["EXPECTED_USER_POD_IMAGE"]
expected_namespace = os.environ["K8S_NAMESPACE"]
expected_storage_class = os.environ["USER_POD_STORAGE_CLASS"]

assert os.environ["USER_POD_IMAGE"] == expected_image
config = runpy.run_path("/dcusshk8s/kda_config.py")
assert config["DEFAULT_NAMESPACE"] == expected_namespace

manager = UserPod("jenkins-contract", expected_namespace)
pod = manager.make_pod_spec()
init_images = {
    container.name: container.image
    for container in (pod.spec.init_containers or [])
}
container_images = {
    container.name: container.image
    for container in (pod.spec.containers or [])
}
assert init_images["init-setup"] == expected_image
assert container_images["shell"] == expected_image

pvc = manager.make_pvc_spec(manager.pvc_templates[0])
assert pvc.spec.storage_class_name == expected_storage_class
print("User Pod/PVC runtime contract: OK")
PY
