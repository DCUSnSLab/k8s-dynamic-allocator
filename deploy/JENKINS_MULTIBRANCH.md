# Jenkins Multibranch production deployment

The current pipeline deploys only the production cluster. The test cluster is
not configured yet.

## Jenkins prerequisites

- Jenkins plugin: `Lockable Resources`
- Harbor credential ID: `harbor`
- The Jenkins agent must provide `docker`, `kubectl`, and standard POSIX shell
  tools.
- The agent's existing Kubernetes identity must be authorized for the target
  cluster. The pipeline does not install or switch kubeconfig credentials.
- The following production resources must already exist:
  - Namespace `kda-test`
  - StorageClass `normal-r3`
  - Secret `compute-ssh-key` with `id_rsa` and `id_rsa.pub`
  - ConfigMap `compute-public-key` with `id_rsa.pub`

The pipeline verifies these resources but never creates, replaces, or deletes
them.

The lock name `kda-deploy-production` is used directly by the Jenkinsfile; the
Lockable Resources plugin can create this named lock when it is first used, so
no separate Jenkins resource entry is required.

## Create the Multibranch Pipeline

1. In Jenkins, select **New Item** and then **Multibranch Pipeline**.
2. Add this Git repository under **Branch Sources**.
3. Keep **Script Path** set to `Jenkinsfile`.
4. Save, then run **Scan Multibranch Pipeline Now**.
5. Open the desired branch job and click **Build Now**.

Only a build started by a Jenkins user builds images and deploys production.
SCM/indexing-triggered builds perform checkout and metadata preparation only,
so discovering a branch cannot deploy it automatically.

## Pipeline behavior

Each manual build creates the immutable tag `BUILD_NUMBER-GIT_SHA7` and also
pushes `latest`. Kubernetes always receives the immutable tag.

```text
compute_pod + user_pod       (parallel build)
           ↓
controller + swlabssh        (parallel build)
           ↓
verify all four image/runtime contracts
           ↓
push immutable tags, then update latest
           ↓
lock: kda-deploy-production
           ↓
preflight + render + deploy-stage coverage + dry-run
           ↓
bootstrap → logging → Redis → Controller → Compute → swlabssh
           ↓
rollout and smoke verification
```

The Controller image records the matching `compute_pod` image, and the
swlabssh image records the matching `user_pod` image. Jenkins also updates the
existing Controller-managed `compute-general` Deployment's `compute-agent`
image. Nothing is pushed to Harbor until all four local images and their
embedded runtime contracts pass verification.

The deployment uses these production values:

- Namespace: `kda-test`
- StorageClass: `normal-r3`
- SSH endpoint: `203.250.35.87:30622`
- Pool policy: build parameters `POOL_AVAILABLE_MIN` (R, default `2`) and
  `POOL_TOTAL_MAX` (N, default `5`)

## Pool policy (R/N)

R is the number of warm Compute Pods kept immediately allocatable; N is the
upper bound on available + assigned Compute Pods. When the total reaches N,
backfill stops and further requests stay queued.

The values are supplied per build by the `POOL_AVAILABLE_MIN` and
`POOL_TOTAL_MAX` parameters. The pipeline rejects anything that violates
`0 <= R <= N` during preflight, writes the values onto the
Controller-managed `compute-general` Deployment as annotations, and then waits
for the Controller to report the same values through `/api/pool/status/`.

The policy is also adjustable on a running cluster without a redeploy:

```bash
kubectl annotate deployment/compute-general -n kda-test --overwrite k8s-dynamic-allocator/pool-available-min=3 k8s-dynamic-allocator/pool-total-max=8
```

The Controller watches the `app=warm-pod-pool` Deployments and reloads the
policy on every change, so no restart is needed and no Pod is recreated - only
`replicas` is reconciled. A manual change survives a Controller restart because
the Controller writes these annotations only when both of them and both
identity labels are absent. The next Jenkins deployment, however, reapplies its
own parameter values.

No Namespace, SSH key, PVC, assigned Compute Pod, or existing User Pod is
deleted. Controller, available Compute Pods, and swlabssh use Kubernetes
rollouts. An SSH session attached to a replaced swlabssh Pod can still be
disconnected.

On deployment failure, the pipeline stops without rollback and prints workload
status, events, and recent logs. Existing data-bearing PVCs are preserved.

## Repository notes

- Everything that is deployed lives under `deploy/`. The Kustomize base is
  `deploy/base` and environment overlays are `deploy/overlays/<environment>`;
  the production overlay is `deploy/overlays/production`.
- The base is an explicit resource list. Test manifests
  (`deploy/dcusshk8s-ssh-test.yaml`), Secret templates (`deploy/secrets/`),
  Compute runtime templates, and NetworkPolicy are deliberately excluded.
- The ordered deployment and failure diagnostics are implemented in
  `deploy/scripts/` and are invoked once by the Jenkinsfile.
- Validation does not pin an exact resource list. It requires the workloads the
  rollout waits on, exactly one generated Fluent Bit ConfigMap, and that every
  rendered resource carries exactly one `deploy-stage` label. Adding a resource
  therefore needs a stage label, not a script edit.
- `dcusshk8s` is a Git submodule. Changes under it must be committed in that
  repository first, followed by the updated submodule pointer in this
  repository.
