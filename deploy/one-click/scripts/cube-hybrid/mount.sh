#!/usr/bin/env bash
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
# Enable or disable the node-local Hybrid JuiceFS + nbdkit data plane.

set -euo pipefail
PREFIX="${CUBE_INSTALL_PREFIX:-/usr/local/services/cubetoolbox}"
ENV_FILE="${PREFIX}/.one-click.env"
CUBELET_CONFIG="${PREFIX}/Cubelet/config/config.toml"

log() { printf '[cube-hybrid-mount] %s\n' "$*"; }
die() { printf '[cube-hybrid-mount] ERROR: %s\n' "$*" >&2; exit 1; }
[[ "$(id -u)" -eq 0 ]] || die "run as root"
[[ -f "${ENV_FILE}" ]] || die "missing ${ENV_FILE}"
[[ -f "${CUBELET_CONFIG}" ]] || die "missing ${CUBELET_CONFIG}"
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

action="${1:-mount}"
case "${action}" in mount|unmount) ;; *) die "usage: $0 [mount|unmount]" ;; esac

local_dir="${CUBE_HYBRID_LOCAL_DIR:-/data/cubelet/storage/hybrid/local}"
remote_dir="${CUBE_HYBRID_REMOTE_DIR:-/data/cubelet/storage/hybrid/remote}"
socket_path="${CUBE_HYBRID_SOCKET_PATH:-/run/cubestor.sock}"
cubestorcli_path="${CUBE_HYBRID_CUBESTORCLI_PATH:-/usr/local/bin/cubestorcli}"

patch_cubelet_hybrid() {
  local enabled="$1"
  python3 - "${CUBELET_CONFIG}" "${enabled}" "${cubestorcli_path}" \
    "${socket_path}" "${local_dir}" "${remote_dir}" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
values = {
    "enable": sys.argv[2],
    "cubestorcli_path": f'"{sys.argv[3]}"',
    "socket_path": f'"{sys.argv[4]}"',
    "local_dir": f'"{sys.argv[5]}"',
    "shared_dir": f'"{sys.argv[6]}"',
}
header = '[plugins."io.cubelet.internal.v1.storage".cow.hybrid]'
lines = path.read_text(encoding="utf-8").splitlines()
try:
    start = next(i for i, line in enumerate(lines) if line.strip() == header)
except StopIteration:
    raise SystemExit(f"missing {header} in {path}")
end = next(
    (i for i in range(start + 1, len(lines)) if lines[i].lstrip().startswith("[")),
    len(lines),
)
section = [
    line for line in lines[start + 1:end]
    if not re.match(r"^\s*cubestor_path\s*=", line)
]
lines[start + 1:end] = section
end = start + 1 + len(section)
indent = "    "
seen = set()
for i in range(start + 1, end):
    match = re.match(r"^(\s*)([A-Za-z0-9_]+)\s*=", lines[i])
    if not match or match.group(2) not in values:
        continue
    key = match.group(2)
    lines[i] = f"{match.group(1)}{key} = {values[key]}"
    seen.add(key)
missing = [f"{indent}{key} = {value}" for key, value in values.items() if key not in seen]
lines[end:end] = missing
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
}

command -v python3 >/dev/null 2>&1 || die "python3 is required"
if [[ "${action}" == "unmount" ]]; then
  patch_cubelet_hybrid false
  systemctl restart cube-sandbox-cubelet.service
  systemctl disable --now cube-sandbox-hybrid.service
  log "Hybrid storage is disabled"
  exit 0
fi

[[ "${ONE_CLICK_ENABLE_HYBRID:-0}" == "1" ]] \
  || die "set ONE_CLICK_ENABLE_HYBRID=1 in ${ENV_FILE}"
[[ -x "${cubestorcli_path}" ]] || die "cubestorcli is not executable: ${cubestorcli_path}"
mkdir -p "${local_dir}" "${remote_dir}"
patch_cubelet_hybrid true
systemctl daemon-reload
systemctl enable cube-sandbox-hybrid.service
systemctl restart cube-sandbox-hybrid.service
systemctl is-active --quiet cube-sandbox-hybrid.service \
  || die "cube-sandbox-hybrid.service did not become active"
for _ in $(seq 1 50); do
  [[ -S "${socket_path}" ]] && break
  sleep 0.1
done
[[ -S "${socket_path}" ]] || die "Hybrid socket was not created: ${socket_path}"
systemctl restart cube-sandbox-cubelet.service
log "Hybrid storage is ready: local=${local_dir} remote=${remote_dir} socket=${socket_path}"
