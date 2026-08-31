"""Runtime image overrides for controller-owned compute manifests."""

from typing import Dict


COMPUTE_AGENT_CONTAINER_NAME = "compute-agent"


def override_compute_agent_image(workload: Dict, image: str) -> None:
    """Set the compute-agent image and reject malformed templates."""
    if not isinstance(workload, dict):
        raise ValueError("Compute manifest must be a mapping")

    image = (image or "").strip()
    if not image:
        raise ValueError("COMPUTE_POD_IMAGE must not be empty")

    kind = workload.get("kind")
    if kind == "Deployment":
        pod_spec = (
            workload.get("spec", {})
            .get("template", {})
            .get("spec", {})
        )
    elif kind == "Pod":
        pod_spec = workload.get("spec", {})
    else:
        raise ValueError(
            f"Unsupported compute manifest kind for image override: {kind!r}"
        )

    containers = pod_spec.get("containers") or []
    matches = [
        container
        for container in containers
        if isinstance(container, dict)
        and container.get("name") == COMPUTE_AGENT_CONTAINER_NAME
    ]
    if len(matches) != 1:
        raise ValueError(
            "Compute manifest must define exactly one compute-agent container"
        )

    matches[0]["image"] = image
