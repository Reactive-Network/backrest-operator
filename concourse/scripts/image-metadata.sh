#!/usr/bin/env bash
# Image/chart tags (mirrors .github/workflows/ci.yaml publish metadata step).
set -euo pipefail

git config --global --add safe.directory "$(pwd)" 2>/dev/null || true

OWNER="${GHCR_OWNER:-reactive-network}"
OWNER="$(echo "${OWNER}" | tr '[:upper:]' '[:lower:]')"
SHA_SHORT="$(git rev-parse HEAD | cut -c1-12)"
if [ -n "${RELEASE_VERSION:-}" ]; then
  REF="${RELEASE_VERSION}"
elif tag="$(git describe --tags --exact-match 2>/dev/null)"; then
  REF="${tag}"
elif [ -n "${GIT_BRANCH:-}" ]; then
  REF="${GIT_BRANCH}"
else
  REF="$(git rev-parse --abbrev-ref HEAD)"
  if [ "${REF}" = "HEAD" ]; then
    REF="master"
  fi
fi

IS_RELEASE=false
CHART_VERSION=""
OPERATOR_TAGS=()
MCP_TAGS=()

if echo "${REF}" | grep -qE '^v[0-9]'; then
  VERSION="${REF#v}"
  IS_RELEASE=true
  CHART_VERSION="${VERSION}"
  OPERATOR_TAGS+=("ghcr.io/${OWNER}/backrest-operator:v${VERSION}")
  OPERATOR_TAGS+=("ghcr.io/${OWNER}/backrest-operator:latest")
  OPERATOR_TAGS+=("ghcr.io/${OWNER}/backrest-operator:sha-${SHA_SHORT}")
  MCP_TAGS+=("ghcr.io/${OWNER}/backrest-mcp:v${VERSION}")
  MCP_TAGS+=("ghcr.io/${OWNER}/backrest-mcp:latest")
  MCP_TAGS+=("ghcr.io/${OWNER}/backrest-mcp:sha-${SHA_SHORT}")
else
  CHART_VERSION="0.0.0-${SHA_SHORT}"
  OPERATOR_TAGS+=("ghcr.io/${OWNER}/backrest-operator:latest")
  OPERATOR_TAGS+=("ghcr.io/${OWNER}/backrest-operator:sha-${SHA_SHORT}")
  OPERATOR_TAGS+=("ghcr.io/${OWNER}/backrest-operator:${REF}")
  MCP_TAGS+=("ghcr.io/${OWNER}/backrest-mcp:latest")
  MCP_TAGS+=("ghcr.io/${OWNER}/backrest-mcp:sha-${SHA_SHORT}")
  MCP_TAGS+=("ghcr.io/${OWNER}/backrest-mcp:${REF}")
fi

export OWNER SHA_SHORT REF IS_RELEASE CHART_VERSION
export OPERATOR_TAGS MCP_TAGS

echo "owner=${OWNER}"
echo "sha_short=${SHA_SHORT}"
echo "chart_version=${CHART_VERSION}"
echo "is_release=${IS_RELEASE}"
printf 'operator_tag=%s\n' "${OPERATOR_TAGS[@]}"
printf 'mcp_tag=%s\n' "${MCP_TAGS[@]}"
