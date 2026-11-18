#!/usr/bin/env bash
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
# Verify the Hybrid JuiceFS mount, nbdkit socket, and an end-to-end NBD volume.

set -euo pipefail
PREFIX="${CUBE_INSTALL_PREFIX:-/usr/local/services/cubetoolbox}"
ENV_FILE="${PREFIX}/.one-click.env"

log() { printf '[cube-hybrid-check] %s\n' "$*"; }
die() { printf '[cube-hybrid-check] ERROR: %s\n' "$*" >&2; exit 1; }
[[ "$(id -u)" -eq 0 ]] || die "run as root"
[[ -f "${ENV_FILE}" ]] || die "missing ${ENV_FILE}"
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

local_dir="${CUBE_HYBRID_LOCAL_DIR:-/data/cubelet/storage/hybrid/local}"
remote_dir="${CUBE_HYBRID_REMOTE_DIR:-/data/cubelet/storage/hybrid/remote}"
socket_path="${CUBE_HYBRID_SOCKET_PATH:-/run/cubestor.sock}"
cubestorcli="${CUBE_HYBRID_CUBESTORCLI_PATH:-/usr/local/bin/cubestorcli}"
name=".cube-hybrid-check-$$"
device=""

cleanup() {
  set +e
  [[ -n "${device}" ]] && "${cubestorcli}" detach --device "${device}" >/dev/null 2>&1
  "${cubestorcli}" delete --dir "${local_dir}" --dir "${remote_dir}" --name "${name}" >/dev/null 2>&1
}
trap cleanup EXIT

[[ "${ONE_CLICK_ENABLE_HYBRID:-0}" == "1" ]] \
  || die "ONE_CLICK_ENABLE_HYBRID is not enabled"
systemctl is-active --quiet cube-sandbox-hybrid.service \
  || die "cube-sandbox-hybrid.service is not active"
findmnt -rn -T "${remote_dir}" -o FSTYPE | grep -qx 'fuse.juicefs' \
  || die "${remote_dir} is not a JuiceFS mount"
[[ "$(findmnt -rn -T "${local_dir}" -o FSTYPE)" == "xfs" ]] \
  || die "${local_dir} is not backed by XFS"
[[ -S "${socket_path}" ]] || die "missing nbdkit socket ${socket_path}"
[[ -x "${cubestorcli}" ]] || die "cubestorcli is not executable: ${cubestorcli}"

"${cubestorcli}" create --size 4M --dir "${remote_dir}" --name "${name}" >/dev/null
out="$("${cubestorcli}" attach --timeout 10 --unix "${socket_path}" --name "${name}")"
device="$(awk '/^device / {print $2}' <<<"${out}")"
[[ -n "${device}" && -b "${device}" ]] || die "cubestorcli returned no block device: ${out}"
printf 'cube-hybrid-ok\n' | dd of="${device}" bs=4K conv=fsync,notrunc status=none
readback="$(dd if="${device}" bs=15 count=1 status=none)"
[[ "${readback}" == "cube-hybrid-ok" ]] || die "NBD read/write verification failed"
"${cubestorcli}" flush --device "${device}" >/dev/null
"${cubestorcli}" detach --device "${device}" >/dev/null
device=""
"${cubestorcli}" check --local-dir "${local_dir}" --shared-dir "${remote_dir}" --name "${name}" >/dev/null \
  || log "local data is dirty as expected before a publish/sync operation"

log "JuiceFS mount: ${remote_dir}"
log "XFS local:     ${local_dir}"
log "nbdkit socket: ${socket_path}"
log "end-to-end NBD create/write/read/flush/detach passed"
