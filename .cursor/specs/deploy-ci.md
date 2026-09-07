# deploy-ci — auto-deploy после build-images (R4)

## Goal

Добавить Concourse job **`deploy-ci`** в pipeline `backrest-operator`, который после успешного **`build-images`** автоматически выкатывает operator + MCP в PRQ-кластер через Helm, с тегами образов `sha-<12hex>` от коммита сборки. Убрать ручной `helm upgrade` после каждого merge в `master`.

Соответствие **R4** ([infra/docs/ci.md](https://github.com/Reactive-Network/infra/blob/main/docs/ci.md)): auto deploy с `passed: [build-images]` + `trigger: true` на том же `get: app-git`.

## Non-goals

- **R10 prod deploy** (`deploy-production-from-build`, `deploy-production-from-tag`) — **вне scope**; в `MIGRATION.md` R10 остаётся **N/A** (нет отдельного prod-окружения / manual-only prod path).
- Деплой infra-overlay манифестов из `infra/kubernetes/helm-charts/backrest-operator/` (oauth2-proxy, `BackrestCluster`, ingress redirect, `PVCBackup`, Storj repo) — по-прежнему bootstrap/ops через `apply.sh`, не в CI job.
- Изменение release jobs (`release`, `release-manual`) и OCI chart publish.
- PR CI, Slack hooks (inject из `ci-shared` без изменений).
- Переименование `serial_groups: [deploy]` в `deploy-backrest-operator` (grandfather exception как у catalyst).

## Current behavior

| Факт | Источник |
|------|----------|
| Pipeline: `set-self`, `unit-test`, `helm-lint`, `build-images`, `release`, `release-manual` | `concourse/pipelines/backrest-operator.yml` |
| `deployer-image` уже объявлен, но не используется | тот же файл |
| `build-images` пушит `ghcr.io/reactive-network/backrest-operator` и `backrest-mcp` с тегами `latest`, `master`, `sha-<short>` | `concourse/scripts/image-metadata.sh` |
| **`SHA_SHORT` сейчас 7 hex** (`git rev-parse --short=7`) — **не** 12 как у catalyst | `image-metadata.sh` |
| Ручной деплой: `infra/kubernetes/helm-charts/backrest-operator/apply.sh` — `helm upgrade --install backrest-operator` с `-f .values.yaml`, затем `kubectl apply` CR/ingress/oauth2 | infra overlay |
| Live values: namespace `backrest`, `fullnameOverride: br-operator`, image tags `sha-*`, MCP ingress `backrest.mcp.prq-infra.net`, `mcp.requireAuth: false` | `infra/.../.values.yaml` |
| Chart CRDs в `charts/backrest-operator/crds/` (6 CRD); chart README рекомендует `kubectl apply -f crds/` отдельно | chart |
| `MIGRATION.md`: R10 **Not applicable**; deploy job отсутствует | `concourse/MIGRATION.md` |
| Kubeconfig для deploy: KCM secret `kubeconfig-prq`, param `((kubeconfig-prq))` — паттерн catalyst | catalyst pipeline + infra `concourse/README.md` |

## Proposed behavior

### Pipeline (`concourse/pipelines/backrest-operator.yml`)

1. Добавить top-level:

```yaml
serial_groups:
  - deploy
```

2. Обновить **groups**:
   - `ci`: добавить `deploy-ci` (после `build-images`).
   - Отдельную group `deploy` **не** создавать (нет R10 jobs).

3. Новый job **`deploy-ci`**:

```yaml
  - name: deploy-ci
    serial_groups: [deploy]
    max_in_flight: 1
    timeout: 30m
    plan:
      - in_parallel:
          steps:
            - get: app-git
              passed: [build-images]
              trigger: true
            - get: ci-shared
              passed: [set-self]
            - get: deployer-image
      - task: deploy
        image: deployer-image
        file: app-git/concourse/tasks/backrest-deploy.yml
        input_mapping:
          app-git: app-git
        params:
          DEPLOY_SOURCE: ci
          KUBECONFIG_CONTENT: ((kubeconfig-prq))
```

**R4 checklist:** `passed: [build-images]` + `trigger: true` только на `app-git`; `ci-shared` без trigger; `deployer-image` без passed (как catalyst).

**R8-deploy:** `serial_groups: [deploy]`, `max_in_flight: 1`.

**BP1:** `timeout: 30m` на job `deploy-ci` (Helm `--wait` + rollout может занимать >10m).

### Task (`concourse/tasks/backrest-deploy.yml`)

Тонкая обёртка по образцу `catalyst-operator/concourse/tasks/catalyst-deploy.yml`:

| Param | Default / CI value |
|-------|-------------------|
| `DEPLOY_SOURCE` | `ci` (required) |
| `IMAGE_TAG` | `""` (override для будущих manual/R10 jobs) |
| `KUBECONFIG_CONTENT` | from pipeline |
| `HELM_RELEASE` | `backrest-operator` |
| `NAMESPACE` | `backrest` |
| `CHART_PATH` | `charts/backrest-operator` |

Полный skeleton task YAML:

```yaml
---
platform: linux

inputs:
  - name: app-git

params:
  DEPLOY_SOURCE: ""
  IMAGE_TAG: ""
  KUBECONFIG_CONTENT: ""
  HELM_RELEASE: backrest-operator
  NAMESPACE: backrest
  CHART_PATH: charts/backrest-operator

run:
  path: bash
  args:
    - concourse/scripts/backrest-deploy.sh
  dir: app-git
```

Image repository задаётся в chart `values.yaml` (`operator.image.repository`, `mcp.image.repository`); deploy меняет только **tags** через `--set`.

### Script (`concourse/scripts/backrest-deploy.sh`)

Один entry point; логика **не** в task YAML / heredoc.

**Kubeconfig** (как `catalyst-deploy.sh`):
- Требовать `KUBECONFIG_CONTENT`.
- Записать в `mktemp`, `chmod 600`, `trap` cleanup.
- **Не** логировать содержимое kubeconfig; **не** `set -x` с секретами в env.

**Git checkout** (как `catalyst-deploy.sh` / `helm-lint.sh`):

```bash
git config --global --add safe.directory "$(pwd)"
```

**Tag resolution** (`DEPLOY_SOURCE=ci` — единственный режим в этом PR):

```bash
COMMIT_SHA="$(git rev-parse HEAD)"
TAG="sha-$(echo "${COMMIT_SHA}" | cut -c1-12)"
```

Для `ci` не использовать floating tags (`latest`, `master`).

**CRDs** (idempotent, до Helm):

```bash
kubectl apply -f "${CHART_PATH}/crds/"
```

**Helm pre-flight + upgrade** — CI **не** делает `helm install` / `upgrade --install`. Release должен существовать после bootstrap (`infra/.../apply.sh`).

Pre-flight (fail-fast):

```bash
if ! helm status "${HELM_RELEASE}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  echo "ERROR: Helm release '${HELM_RELEASE}' not found in namespace '${NAMESPACE}'." >&2
  echo "Bootstrap first: infra/kubernetes/helm-charts/backrest-operator/apply.sh" >&2
  exit 1
fi
```

Upgrade — сохранить cluster-specific values из release history, обновить только image tags:

```bash
helm upgrade "${HELM_RELEASE}" "${CHART_PATH}" \
  --namespace "${NAMESPACE}" \
  --reuse-values \
  --set "operator.image.tag=${TAG}" \
  --set "mcp.image.tag=${TAG}" \
  --wait --timeout 10m \
  --skip-crds
```

| Решение | Обоснование |
|---------|-------------|
| **`helm upgrade` без `--install`** | CI не создаёт release с chart defaults; greenfield — только ops bootstrap |
| Pre-flight `helm status` | Явный fail-fast вместо неявного install с пустыми values |
| `--reuse-values` | Infra overlay (ingress MCP, `fullnameOverride`, RBAC flags, monitoring) живёт в release history; CI не дублирует `infra/.values.yaml` в app repo |
| `--skip-crds` | CRDs уже применены через `kubectl apply`; Helm не управляет CRD upgrades |
| **Не** `-f` values file из app repo | Избежать drift и затирания PRQ-specific config |

**Prerequisite:** Helm release `backrest-operator` в namespace `backrest` уже существует (bootstrap через `infra/.../apply.sh`).

**Rollout verification:**

```bash
kubectl rollout status deployment/br-operator -n backrest --timeout=5m
kubectl rollout status deployment/br-operator-mcp -n backrest --timeout=5m
```

Имена deployment — из `fullnameOverride: br-operator` в live values.

**Out of scope для скрипта:** `kubectl apply` oauth2 / BackrestCluster / backup CRs из infra overlay.

### Image tag alignment (build ↔ deploy)

Deploy ожидает тег **`sha-<12hex>`** (fleet/catalyst convention).

Сейчас `image-metadata.sh` пушит **`sha-<7hex>`**. **Requirement:** в том же PR (или сразу перед первым smoke deploy-ci) обновить `image-metadata.sh`:

```bash
SHA_SHORT="$(git rev-parse HEAD | cut -c1-12)"
```

и оставить теги `sha-${SHA_SHORT}` для operator + MCP (non-release path). Release path (`v*`) без изменений.

Без этого deploy-ci будет ссылаться на несуществующий образ в GHCR.

### MIGRATION.md

Обновить `concourse/MIGRATION.md`:

**Review list** — новая строка:

| Capability | Concourse job | Trigger | Notes |
|------------|---------------|---------|-------|
| Non-prod deploy | `deploy-ci` | auto after build-images | R4; Helm upgrade PRQ cluster; `DEPLOY_SOURCE: ci` |

**Production deploy (R10)** — оставить **Not applicable** с уточнением:

> Единственный auto-deploy target (PRQ infra / `backup.prq-infra.net`) обслуживается job **`deploy-ci`**. Dual manual prod jobs (`deploy-*-from-build` / `deploy-*-from-tag`) не требуются, пока не появится отдельное production environment с manual-only policy (R3/R10).

**Deploy concurrency (R8-deploy)** — новая секция (как catalyst):

> Pipeline declares `serial_groups: [deploy]` for `deploy-ci`.

**Custom ops** — не добавлять (job auto-only).

## Requirements

- **R1:** Изменения только в `concourse/**` app repo; `set-self` после merge.
- **R3/R4:** `deploy-ci` auto (`trigger: true`); prod deploy jobs отсутствуют.
- **R4:** `get: app-git` с `passed: [build-images]` + `trigger: true`.
- **R7:** `KUBECONFIG_CONTENT: ((kubeconfig-prq))` — secret в KCM `concourse`, не в `vars/sample.yml`.
- **R8-deploy:** `serial_groups: [deploy]`, `max_in_flight: 1` на `deploy-ci`.
- **BP1:** `timeout: 30m` на job `deploy-ci`.
- **R10:** N/A — не добавлять prod deploy jobs в этом PR.
- **Readable tasks:** task YAML → один `backrest-deploy.sh`; без nested bash / inline heredoc logic.
- **Secrets:** never log kubeconfig; temp file + trap.
- **webhook_token:** остаётся на resource level (`app-git`); не изобретать новые git-resource keys.

## Acceptance criteria

- [ ] **AC1:** В `backrest-operator.yml` есть `serial_groups: [deploy]`, job `deploy-ci` с R4 trigger chain после `build-images`, и **`timeout: 30m`** (BP1).
- [ ] **AC2:** `deploy-ci` в group `ci`; `deployer-image` используется.
- [ ] **AC3:** `concourse/tasks/backrest-deploy.yml` (с `platform`, `inputs`, `params`) + `concourse/scripts/backrest-deploy.sh` соответствуют readable-tasks правилам; скрипт вызывает `git config --global --add safe.directory`.
- [ ] **AC4:** Deploy применяет CRDs (`kubectl apply -f charts/backrest-operator/crds/`) и делает **`helm upgrade`** (без `--install`) с `--reuse-values` + `--set` image tags.
- [ ] **AC5:** Image tag для `DEPLOY_SOURCE=ci` = `sha-$(git rev-parse HEAD | cut -c1-12)`; build pipeline пушит тот же тег.
- [ ] **AC6:** Rollout wait для `br-operator` и `br-operator-mcp` в namespace `backrest`.
- [ ] **AC7:** `MIGRATION.md` обновлён (review list, R8-deploy, R10 clarification).
- [ ] **AC8:** `vars/sample.yml` **не** содержит kubeconfig (только webhook tokens / GHCR placeholders как сейчас).
- [ ] **AC9:** После push: `fly check-resource` → `fly trigger-job set-self` → smoke `deploy-ci` на green `build-images` обновляет образы в cluster.
- [ ] **AC10:** Если Helm release `backrest-operator` отсутствует в namespace `backrest`, скрипт завершается с exit ≠ 0 и сообщением про `infra/.../apply.sh` — **без** `helm install` / `upgrade --install`.

## Files likely to change

| Path | Why |
|------|-----|
| `concourse/pipelines/backrest-operator.yml` | `serial_groups`, `deploy-ci` job, groups |
| `concourse/tasks/backrest-deploy.yml` | New task wrapper |
| `concourse/scripts/backrest-deploy.sh` | Deploy logic |
| `concourse/scripts/image-metadata.sh` | Align SHA tag to 12 hex (build ↔ deploy) |
| `concourse/MIGRATION.md` | Review list, R8, R10 note |

**Не менять в этом PR:** `infra/kubernetes/helm-charts/backrest-operator/*` (bootstrap overlay остаётся ops path).

## Test plan

1. **Local script dry review:** убедиться, что `backrest-deploy.sh` не echo'ит `KUBECONFIG_CONTENT`.
2. **Pipeline validate:** merge → `set-self` green на ci.prq-infra.net.
3. **Tag alignment:** после `build-images` в GHCR есть `backrest-operator:sha-<12>` и `backrest-mcp:sha-<12>` для того же commit.
4. **Smoke deploy-ci:**
   - Push trivial commit → дождаться `build-images` green → `deploy-ci` auto-trigger.
   - `kubectl -n backrest get deploy br-operator br-operator-mcp -o jsonpath='{..image}'` — tags match commit sha-12.
   - Pods Ready; MCP ingress отвечает (optional curl smoke).
5. **CRD idempotency:** повторный deploy-ci не flapping CRDs.
6. **Concurrency:** два быстрых push — второй deploy queued (`max_in_flight: 1`), не parallel.
7. **Fail-fast:** на кластере без release `backrest-operator` в `backrest` — job red, в логах ссылка на `apply.sh`, Helm install не выполняется.

## Risks and open questions

| Item | Type | Notes |
|------|------|-------|
| **Первый deploy без prior `apply.sh`** | Risk (mitigated) | Pre-flight `helm status` + `helm upgrade` без `--install`; AC10. |
| **7 vs 12 hex tags** | Risk (mitigated) | Требует правки `image-metadata.sh` в том же PR. |
| **CRD schema drift** | Risk | `kubectl apply` обновит CRDs; при breaking CRD change нужен ops review вне auto-deploy. |
| **Helm `--reuse-values` + chart template changes** | Risk | Новые required values в chart без default могут не попасть в release; решается отдельным manual `apply.sh` при major chart bumps. |
| **Единственный cluster = prod?** | Open question (non-blocking) | Org классифицирует PRQ backup как non-prod auto target; R10 N/A до появления второго env или manual prod policy. |
| **Committed `concourse/values/prq-ci.yaml`** | Open question (non-blocking) | Альтернатива `--reuse-values` для greenfield; **не** включать без запроса — дублирует infra overlay. |
