# backrest-operator: Concourse migration inventory

Concourse pipeline `backrest-operator` on team `ci` is the **primary CI/CD path** for this repo.

**Tracked branch:** `master`

---

## Human sign-off

| Reviewer | Date | Approved |
|----------|------|----------|
| | | ☐ |

---

## Review list

| Capability | Concourse job | Trigger | Notes |
|------------|---------------|---------|-------|
| Unit tests | `unit-test` | auto on push | — |
| Helm lint | `helm-lint` | auto on push | — |
| Build images | `build-images` | auto after test + lint | — |
| Non-prod deploy | `deploy-ci` | auto after build-images | R4; Helm upgrade PRQ cluster; `DEPLOY_SOURCE: ci` |
| Release tag push | `release` | auto on `app-git-tags` `v*` | R9-a (grandfather job name) |
| Manual release bump | `release-manual` | manual | R9-b |

### Custom ops (manual-only)

| Job | Operator action |
|-----|-----------------|
| `release` | Auto on external git tag `v*` push |
| `release-manual` | `fly -t ci trigger-job -j backrest-operator/release-manual` |

---

## Release (R9)

| Path | Job | Trigger | Notes |
|------|-----|---------|-------|
| R9-a Tag push | `release` | auto on `app-git-tags` `v*` | Images + Helm OCI chart + GitHub Release |
| R9-b Manual bump | `release-manual` | manual only | bump tag + `release.sh` with `RELEASE_VERSION` |

Semver bump: latest stable `vMAJOR.MINOR.PATCH` → `vMAJOR.(MINOR+1).0`; no stable tags → `v0.1.0`.

---

## Deploy concurrency (R8-deploy)

Pipeline declares `serial_groups: [deploy]` for `deploy-ci`.

---

## Production deploy (R10)

**Not applicable** — no dual manual prod deploy jobs (`deploy-*-from-build` / `deploy-*-from-tag`).

The only auto-deploy target (PRQ infra / `backup.prq-infra.net`) is served by job **`deploy-ci`**. Dual manual prod jobs are not required until a separate production environment with manual-only policy appears (R3/R10).
