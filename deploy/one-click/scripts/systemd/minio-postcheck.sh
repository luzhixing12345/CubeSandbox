#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"
# MinIO is opt-in via CUBE_SANDBOX_MINIO_ENABLED. When it is off the local
# container is never started (see up-support.sh), so there is nothing to
# health-check. Skip rather than block on a missing container and then
# trigger Restart=on-failure.
if [[ "${CUBE_SANDBOX_MINIO_ENABLED:-1}" != "1" ]]; then
  exit 0
fi
wait_for_container_health "${CUBE_SANDBOX_MINIO_CONTAINER:-cube-sandbox-minio}" 40 2 || die "minio container not ready"
# Confirm the S3 API is serving, not just that the container process exists.
minio_port="${CUBE_SANDBOX_MINIO_API_PORT:-9000}"
minio_bind="${CUBE_SANDBOX_MINIO_API_BIND:-${CUBE_SANDBOX_NODE_IP:-127.0.0.1}}"
wait_for_http "http://${minio_bind}:${minio_port}/minio/health/live" 40 2 \
  || die "minio API did not become ready on ${minio_bind}:${minio_port}"

# The blobstore never creates buckets (neither CubeTemplateCenter's artifact
# client nor CubeMaster's does; only CubeOps and the s3lvol/hybrid helpers opt
# in). Without this step a fresh install has no CUBE_S3_BUCKET, and the first
# template build fails with "The specified bucket does not exist" AFTER pulling
# and building the ext4, so the whole job is wasted.
ensure_bucket() {
  local bucket="$1"
  [[ -n "${bucket}" ]] || return 0
  local container="${CUBE_SANDBOX_MINIO_CONTAINER:-cube-sandbox-minio}"
  local endpoint="http://127.0.0.1:${minio_port}"
  local host_alias="http://${CUBE_SANDBOX_MINIO_ROOT_USER}:${CUBE_SANDBOX_MINIO_ROOT_PASSWORD}@127.0.0.1:${minio_port}"
  # `mc` ships inside the MinIO image and is the cheapest way to get an
  # idempotent MakeBucket; it is a no-op when the bucket already exists.
  if docker exec -e "MC_HOST_cube=${host_alias}" "${container}" mc mb --ignore-existing "cube/${bucket}" >/dev/null 2>&1; then
    log "S3 bucket ${bucket} is ready at ${endpoint}"
    return 0
  fi
  log "WARN: could not ensure S3 bucket ${bucket} at ${endpoint}; template artifact uploads will fall back to local disk"
  return 0
}

ensure_bucket "${CUBE_S3_BUCKET:-${CUBE_SANDBOX_MINIO_BUCKET:-cube-volumes}}"
ensure_bucket "${CUBE_S3LVOL_BUCKET:-}"
ensure_bucket "${CUBE_OPS_S3_BUCKET:-cube-ops}"
