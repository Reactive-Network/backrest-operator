#!/usr/bin/env bash
set -euo pipefail

DEPLOY_SOURCE="${DEPLOY_SOURCE:?DEPLOY_SOURCE is required}"
HELM_RELEASE="${HELM_RELEASE:-backrest-operator}"
NAMESPACE="${NAMESPACE:-backrest}"
CHART_PATH="${CHART_PATH:-charts/backrest-operator}"
IMAGE_TAG="${IMAGE_TAG:-}"

if [ -z "${KUBECONFIG_CONTENT:-}" ]; then
  echo "ERROR: KUBECONFIG_CONTENT is required" >&2
  exit 1
fi

KUBECONFIG_FILE="$(mktemp)"
trap 'rm -f "${KUBECONFIG_FILE}"' EXIT
printf '%s' "${KUBECONFIG_CONTENT}" > "${KUBECONFIG_FILE}"
chmod 600 "${KUBECONFIG_FILE}"
export KUBECONFIG="${KUBECONFIG_FILE}"

git config --global --add safe.directory "$(pwd)"

resolve_tag() {
  case "${DEPLOY_SOURCE}" in
    ci)
      if [ -n "${IMAGE_TAG}" ]; then
        TAG="${IMAGE_TAG}"
      else
        COMMIT_SHA="$(git rev-parse HEAD)"
        TAG="sha-$(echo "${COMMIT_SHA}" | cut -c1-12)"
      fi
      echo "Deploying operator + MCP with tag ${TAG} to namespace ${NAMESPACE}"
      ;;
    *)
      echo "ERROR: unsupported DEPLOY_SOURCE '${DEPLOY_SOURCE}'" >&2
      exit 1
      ;;
  esac
}

resolve_tag

kubectl apply -f "${CHART_PATH}/crds/"

if ! helm status "${HELM_RELEASE}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  echo "ERROR: Helm release '${HELM_RELEASE}' not found in namespace '${NAMESPACE}'." >&2
  echo "Bootstrap first: infra/kubernetes/helm-charts/backrest-operator/apply.sh" >&2
  exit 1
fi

helm upgrade "${HELM_RELEASE}" "${CHART_PATH}" \
  --namespace "${NAMESPACE}" \
  --reuse-values \
  --set "operator.image.tag=${TAG}" \
  --set "mcp.image.tag=${TAG}" \
  --wait --timeout 10m \
  --skip-crds

kubectl rollout status deployment/br-operator -n "${NAMESPACE}" --timeout=5m
kubectl rollout status deployment/br-operator-mcp -n "${NAMESPACE}" --timeout=5m

echo "Deploy complete: ${TAG}"
