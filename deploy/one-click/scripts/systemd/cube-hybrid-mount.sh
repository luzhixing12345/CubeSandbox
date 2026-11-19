#!/usr/bin/env bash
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
# Mount Hybrid's private JuiceFS volume and run its nbdkit overlay server.

set -euo pipefail

JFS_IMAGE_INT_DEFAULT="cube-sandbox-int.tencentcloudcr.com/cube-sandbox/juicefs:ce-v1.4.1"
JFS_IMAGE_CN_DEFAULT="cube-sandbox-cn.tencentcloudcr.com/cube-sandbox/juicefs:ce-v1.4.1"
if [[ -n "${CUBE_HYBRID_JFS_IMAGE:-}" ]]; then
  JFS_IMAGE="${CUBE_HYBRID_JFS_IMAGE}"
elif [[ "${MIRROR:-}" == "cn" ]]; then
  JFS_IMAGE="${JFS_IMAGE_CN_DEFAULT}"
else
  JFS_IMAGE="${JFS_IMAGE_INT_DEFAULT}"
fi

CONTAINER_NAME="${CUBE_HYBRID_JFS_CONTAINER_NAME:-cube-sandbox-hybrid-jfs}"
MOUNT_PATH="${CUBE_HYBRID_REMOTE_DIR:-/data/cubelet/storage/hybrid/remote}"
LOCAL_DIR="${CUBE_HYBRID_LOCAL_DIR:-/data/cubelet/storage/hybrid/local}"
CACHE_DIR="${CUBE_HYBRID_CACHE_DIR:-/data/cubelet/hybrid-jfs-cache}"
CACHE_SIZE_MB="${CUBE_HYBRID_CACHE_SIZE_MB:-102400}"
# Hybrid publishes asynchronously itself. Shared-image fsync must wait for
# object storage before Cubelet reports the package ready.
EXTRA_OPTS="${CUBE_HYBRID_MOUNT_OPTS:---writeback=false}"
SOCKET_PATH="${CUBE_HYBRID_SOCKET_PATH:-/run/cubestor.sock}"
NBDKIT_BIN="${CUBE_HYBRID_NBDKIT_PATH:-/usr/local/bin/nbdkit}"
CUBESTOR_PLUGIN="${CUBE_HYBRID_CUBESTOR_PLUGIN_PATH:-/usr/local/lib/cube-sandbox/nbdkit-cubestor-plugin.so}"
READ_OPTS=""
[[ " ${EXTRA_OPTS} " == *" --prefetch"* ]] || READ_OPTS+=" --prefetch 16"
[[ " ${EXTRA_OPTS} " == *" --max-fuse-io"* ]] || READ_OPTS+=" --max-fuse-io 1M"
[[ " ${EXTRA_OPTS} " == *" --cache-large-write"* ]] || READ_OPTS+=" --cache-large-write"

log() { printf '[cube-hybrid] %s\n' "$*"; }
die() { printf '[cube-hybrid] ERROR: %s\n' "$*" >&2; exit 1; }

is_juicefs_mount() {
  findmnt -rn -T "${MOUNT_PATH}" -o FSTYPE 2>/dev/null | grep -qx 'fuse.juicefs'
}

ensure_shared_mountpoint() {
  if ! mountpoint -q "${MOUNT_PATH}"; then
    mount --bind "${MOUNT_PATH}" "${MOUNT_PATH}"
  fi
  mount --make-shared "${MOUNT_PATH}"
}

stop_components() {
  set +e
  [[ -n "${NBDKIT_PID:-}" ]] && kill "${NBDKIT_PID}" >/dev/null 2>&1
  docker stop --time 30 "${CONTAINER_NAME}" >/dev/null 2>&1
  [[ -n "${JFS_PID:-}" ]] && wait "${JFS_PID}" >/dev/null 2>&1
  if is_juicefs_mount; then
    fusermount -uz "${MOUNT_PATH}" 2>/dev/null || umount -l "${MOUNT_PATH}" 2>/dev/null
  fi
  mountpoint -q "${MOUNT_PATH}" && umount "${MOUNT_PATH}" >/dev/null 2>&1
  rm -f "${SOCKET_PATH}"
}

do_start() {
  local option
  for option in ${EXTRA_OPTS}; do
    case "${option}" in
      --writeback=false|--writeback=False|--writeback=FALSE|--writeback=0|--writeback=f|--writeback=F) ;;
      --writeback|--writeback=*) die "Hybrid requires JuiceFS writeback disabled; set CUBE_HYBRID_MOUNT_OPTS=--writeback=false" ;;
    esac
  done
  trap stop_components EXIT INT TERM
  if [[ -z "${CUBE_HYBRID_META_URL:-}" ]]; then
    if [[ "${ONE_CLICK_DEPLOY_ROLE:-control}" == "compute" && -n "${ONE_CLICK_CONTROL_PLANE_IP:-}" ]]; then
      CUBE_HYBRID_META_URL="redis://${ONE_CLICK_CONTROL_PLANE_IP}:6381/0"
    elif [[ "${ONE_CLICK_DEPLOY_ROLE:-control}" != "compute" && -n "${CUBE_SANDBOX_NODE_IP:-}" ]]; then
      CUBE_HYBRID_META_URL="redis://${CUBE_SANDBOX_NODE_IP}:6381/0"
    fi
  fi
  [[ -n "${CUBE_HYBRID_META_URL:-}" ]] || die "CUBE_HYBRID_META_URL is not set"
  command -v docker >/dev/null 2>&1 || die "docker is not installed"
  command -v modprobe >/dev/null 2>&1 || die "modprobe is not installed"
  [[ -c /dev/fuse ]] || die "/dev/fuse is not available"
  [[ -x "${NBDKIT_BIN}" ]] || die "nbdkit is not executable: ${NBDKIT_BIN}"
  [[ -f "${CUBESTOR_PLUGIN}" ]] || die "cubestor plugin is missing: ${CUBESTOR_PLUGIN}"
  [[ "${CACHE_SIZE_MB}" =~ ^[0-9]+$ ]] || die "CUBE_HYBRID_CACHE_SIZE_MB must be an integer"

  modprobe nbd nbds_max="${CUBE_HYBRID_NBDS_MAX:-64}" max_part="${CUBE_HYBRID_NBD_MAX_PART:-8}"
  mkdir -p "${LOCAL_DIR}" "${MOUNT_PATH}" "${CACHE_DIR}" "$(dirname "${SOCKET_PATH}")"
  [[ "$(findmnt -rn -T "${LOCAL_DIR}" -o FSTYPE)" == "xfs" ]] \
    || die "${LOCAL_DIR} must be backed by XFS"

  is_juicefs_mount && {
    fusermount -uz "${MOUNT_PATH}" 2>/dev/null || umount -l "${MOUNT_PATH}" 2>/dev/null || true
  }
  ensure_shared_mountpoint
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true

  local -a meta_env_args=()
  if [[ -n "${CUBE_HYBRID_META_PASSWORD:-}" ]]; then
    export META_PASSWORD="${CUBE_HYBRID_META_PASSWORD}"
    meta_env_args+=(--env META_PASSWORD)
  fi
  log "mounting JuiceFS at ${MOUNT_PATH}"
  # shellcheck disable=SC2086 # READ_OPTS and EXTRA_OPTS are option lists.
  docker run --rm --name "${CONTAINER_NAME}" \
    --device /dev/fuse \
    --cap-add SYS_ADMIN \
    "${meta_env_args[@]}" \
    --mount "type=bind,source=${MOUNT_PATH},target=${MOUNT_PATH},bind-propagation=rshared" \
    --mount "type=bind,source=${CACHE_DIR},target=${CACHE_DIR}" \
    --entrypoint juicefs \
    "${JFS_IMAGE}" mount \
    --no-usage-report \
    --cache-dir "${CACHE_DIR}" \
    --cache-size "${CACHE_SIZE_MB}" \
    ${READ_OPTS} \
    ${EXTRA_OPTS} \
    "${CUBE_HYBRID_META_URL}" "${MOUNT_PATH}" &
  JFS_PID=$!

  for _ in $(seq 1 "${CUBE_HYBRID_MOUNT_WAIT_SECONDS:-30}"); do
    is_juicefs_mount && break
    kill -0 "${JFS_PID}" 2>/dev/null || die "JuiceFS mount container exited"
    sleep 1
  done
  is_juicefs_mount || die "${MOUNT_PATH} is not a JuiceFS mount"

  rm -f "${SOCKET_PATH}"
  log "starting nbdkit cubestor socket ${SOCKET_PATH}"
  "${NBDKIT_BIN}" -f -U "${SOCKET_PATH}" "${CUBESTOR_PLUGIN}" \
    "local-dir=${LOCAL_DIR}" "shared-dir=${MOUNT_PATH}" &
  NBDKIT_PID=$!
  for _ in $(seq 1 50); do
    [[ -S "${SOCKET_PATH}" ]] && break
    kill -0 "${NBDKIT_PID}" 2>/dev/null || die "nbdkit exited before creating ${SOCKET_PATH}"
    sleep 0.1
  done
  [[ -S "${SOCKET_PATH}" ]] || die "nbdkit did not create ${SOCKET_PATH}"
  log "Hybrid data plane is ready"

  local rc=0
  wait -n "${JFS_PID}" "${NBDKIT_PID}" || rc=$?
  die "Hybrid child process exited unexpectedly (status=${rc})"
}

case "${1:-}" in
  start) do_start ;;
  stop) stop_components ;;
  *) die "usage: $0 start|stop" ;;
esac
