#!/usr/bin/env bash
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
# Initialize the JuiceFS volume used only by backend=hybrid.

set -euo pipefail
PREFIX="${CUBE_INSTALL_PREFIX:-/usr/local/services/cubetoolbox}"
ENV_FILE="${PREFIX}/.one-click.env"

log() { printf '[cube-hybrid-init] %s\n' "$*"; }
die() { printf '[cube-hybrid-init] ERROR: %s\n' "$*" >&2; exit 1; }
[[ "$(id -u)" -eq 0 ]] || die "run as root"
[[ -f "${ENV_FILE}" ]] || die "missing ${ENV_FILE}"
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a
[[ "${ONE_CLICK_DEPLOY_ROLE:-control}" != "compute" ]] || die "run this script on the control node"
[[ "${ONE_CLICK_ENABLE_HYBRID:-0}" == "1" ]] || die "set ONE_CLICK_ENABLE_HYBRID=1 in ${ENV_FILE}"
command -v docker >/dev/null 2>&1 || die "docker is not installed"

if [[ -z "${CUBE_HYBRID_META_URL:-}" && -n "${CUBE_SANDBOX_NODE_IP:-}" ]]; then
  CUBE_HYBRID_META_URL="redis://${CUBE_SANDBOX_NODE_IP}:6381/0"
  log "using metadata URL derived from CUBE_SANDBOX_NODE_IP"
fi
if [[ -z "${CUBE_HYBRID_BUCKET:-}" && "${CUBE_SANDBOX_MINIO_ENABLED:-0}" == "1" \
    && -n "${CUBE_S3_ENDPOINT:-}" ]]; then
  CUBE_HYBRID_BUCKET="${CUBE_S3_ENDPOINT%/}/cube-hybrid"
  log "using bucket derived from bundled MinIO endpoint"
fi

start_local_redis_if_configured() {
  local meta_url="$1"
  local redis_password redis_host redis_port
  if [[ "${meta_url}" =~ ^redis://:([^@]+)@([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)/[0-9]+$ ]]; then
    redis_password="${BASH_REMATCH[1]}"
    redis_host="${BASH_REMATCH[2]}"
    redis_port="${BASH_REMATCH[3]}"
  elif [[ "${meta_url}" =~ ^redis://([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):([0-9]+)/[0-9]+$ \
      && -n "${CUBE_HYBRID_META_PASSWORD:-}" ]]; then
    redis_password="${CUBE_HYBRID_META_PASSWORD}"
    redis_host="${BASH_REMATCH[1]}"
    redis_port="${BASH_REMATCH[2]}"
  else
    return 0
  fi
  ip -o -4 addr show 2>/dev/null | awk '{print $4}' | cut -d/ -f1 |
    grep -qxF "${redis_host}" || return 0

  local container="cube-sandbox-hybrid-redis"
  local volume="cube-sandbox-hybrid-redis-data"
  local image="${CUBE_SANDBOX_REDIS_IMAGE:-cube-sandbox-image.tencentcloudcr.com/opensource/redis:7-alpine}"
  docker volume create "${volume}" >/dev/null
  if docker container inspect "${container}" >/dev/null 2>&1; then
    docker start "${container}" >/dev/null
  else
    log "starting dedicated Hybrid JuiceFS Redis on ${redis_host}:${redis_port}"
    REDIS_PASSWORD="${redis_password}" docker run -d --restart unless-stopped --name "${container}" \
      --env REDIS_PASSWORD \
      -p "${redis_host}:${redis_port}:6379" \
      -v "${volume}:/data" \
      "${image}" sh -c 'exec redis-server --appendonly yes --requirepass "$REDIS_PASSWORD"' >/dev/null
  fi
  for _ in $(seq 1 30); do
    docker exec "${container}" redis-cli -a "${redis_password}" ping 2>/dev/null |
      grep -qx PONG && return 0
    sleep 1
  done
  die "dedicated Hybrid Redis did not become ready; check docker logs ${container}"
}

image_int="cube-sandbox-int.tencentcloudcr.com/cube-sandbox/juicefs:ce-v1.4.1"
image_cn="cube-sandbox-cn.tencentcloudcr.com/cube-sandbox/juicefs:ce-v1.4.1"
if [[ -n "${CUBE_HYBRID_JFS_IMAGE:-}" ]]; then
  image="${CUBE_HYBRID_JFS_IMAGE}"
elif [[ "${MIRROR:-}" == "cn" ]]; then
  image="${image_cn}"
else
  image="${image_int}"
fi
name="${CUBE_HYBRID_VOLUME_NAME:-cube-hybrid}"
storage="${CUBE_HYBRID_STORAGE:-s3}"
bucket="${CUBE_HYBRID_BUCKET:-}"
access_key="${CUBE_HYBRID_ACCESS_KEY:-${CUBE_S3_ACCESS_KEY_ID:-}}"
secret_key="${CUBE_HYBRID_SECRET_KEY:-${CUBE_S3_SECRET_ACCESS_KEY:-}}"
: "${CUBE_HYBRID_META_URL:?set CUBE_HYBRID_META_URL in ${ENV_FILE}}"
if [[ -n "${CUBE_HYBRID_META_PASSWORD:-}" \
   && "${CUBE_HYBRID_META_URL}" =~ ^[A-Za-z][A-Za-z0-9+.-]*://[^/@]*:[^/@]+@ ]]; then
  die "do not set both CUBE_HYBRID_META_PASSWORD and a password in CUBE_HYBRID_META_URL"
fi
[[ -n "${bucket}" ]] || die "set CUBE_HYBRID_BUCKET in ${ENV_FILE}"
if [[ -n "${access_key}" && -z "${secret_key}" ]] || [[ -z "${access_key}" && -n "${secret_key}" ]]; then
  die "object-storage access key and secret key must be set together"
fi

start_local_redis_if_configured "${CUBE_HYBRID_META_URL}"
meta_env_args=()
if [[ -n "${CUBE_HYBRID_META_PASSWORD:-}" ]]; then
  export META_PASSWORD="${CUBE_HYBRID_META_PASSWORD}"
  meta_env_args+=(--env META_PASSWORD)
fi
log "initializing Hybrid JuiceFS volume ${name} when absent"
ACCESS_KEY="${access_key}" SECRET_KEY="${secret_key}" \
docker run --rm --env ACCESS_KEY --env SECRET_KEY "${meta_env_args[@]}" \
  --entrypoint juicefs "${image}" format \
  --no-update \
  --storage "${storage}" \
  --bucket "${bucket}" \
  --block-size "${CUBE_HYBRID_BLOCK_SIZE:-1M}" \
  --compress "${CUBE_HYBRID_COMPRESS:-lz4}" \
  "${CUBE_HYBRID_META_URL}" "${name}"
docker run --rm "${meta_env_args[@]}" --entrypoint juicefs "${image}" \
  status "${CUBE_HYBRID_META_URL}" >/dev/null
log "Hybrid JuiceFS volume is ready"
