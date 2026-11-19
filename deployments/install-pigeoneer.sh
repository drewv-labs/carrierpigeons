#!/usr/bin/env bash
set -Eeuo pipefail

VERSION="${PIGEONEER_VERSION:-v0.2.0}"
REPO="${PIGEONEER_REPO:-drewv-labs/carrierpigeons}"
NODE_ID="${PIGEONEER_NODE_ID:-$(hostname)}"
BROKER_URL="${PIGEONEER_BROKER_URL:-tcp://pigeoncoop.local:1883}"
NODE_TAGS="${PIGEONEER_TAGS:-}"
MQTT_USER="${PIGEONEER_MQTT_USERNAME:-}"
MQTT_PASS="${PIGEONEER_MQTT_PASSWORD:-}"
SKIP_CHECKSUM="${PIGEONEER_SKIP_CHECKSUM:-false}"
ENABLE_GPU_GROUPS="${PIGEONEER_ENABLE_GPU_GROUPS:-false}"

if [[ ! "${NODE_ID}" =~ ^[a-zA-Z0-9._-]+$ ]]; then
    echo "Invalid NODE_ID '${NODE_ID}': must match ^[a-zA-Z0-9._-]+$" >&2
    exit 1
fi

echo "[*] Detecting architecture for ${NODE_ID}..."
ARCH="$(uname -m)"
case "${ARCH}" in
    riscv64)       BIN_NAME="pigeoneer-linux-riscv64"; GO_ARCH="riscv64" ;;
    aarch64|arm64) BIN_NAME="pigeoneer-linux-arm64";   GO_ARCH="arm64" ;;
    x86_64|amd64)  BIN_NAME="pigeoneer-linux-amd64";   GO_ARCH="amd64" ;;
    *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac

BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

echo "[*] Downloading Pigeoneer ${VERSION} (${GO_ARCH}) from ${BASE_URL}/${BIN_NAME}..."
curl -fsSL -o "${TMP_DIR}/${BIN_NAME}" "${BASE_URL}/${BIN_NAME}"

if [[ "${SKIP_CHECKSUM}" != "true" ]]; then
    echo "[*] Verifying SHA-256 release checksum..."
    curl -fsSL -o "${TMP_DIR}/checksums.txt" "${BASE_URL}/checksums.txt"
    (cd "${TMP_DIR}" && awk -v bin="${BIN_NAME}" '$2 == bin || $2 == "*"bin { found=1; print } END { if (!found) exit 1 }' checksums.txt | sha256sum -c -)
fi

# Atomic install avoids ETXTBSY when upgrading a live running daemon
install -m 0755 "${TMP_DIR}/${BIN_NAME}" /usr/local/bin/pigeoneer.new
mv -f /usr/local/bin/pigeoneer.new /usr/local/bin/pigeoneer

if ! id -u pigeoneer >/dev/null 2>&1; then
    echo "[*] Provisioning dedicated unprivileged 'pigeoneer' system account..."
    useradd --system --no-create-home --shell /usr/sbin/nologin pigeoneer
fi
for grp in video render; do
    if getent group "${grp}" >/dev/null 2>&1; then
        usermod -aG "${grp}" pigeoneer || true
    fi
done

install -d -m 0750 -o root -g pigeoneer /etc/pigeons

if [[ ! -f /etc/pigeons/pigeoneer.toml ]]; then
    cat <<'EOF' > /etc/pigeons/pigeoneer.toml
poll_interval  = "5s"
spool_capacity = 10000

[observers]
os  = true
net = true
gpu = true
npu = true

[runners]
enabled              = true
allow_ephemeral      = false
allowed_interpreters = ["sh", "bash", "python3"]
max_timeout_sec      = 300
EOF
    chown root:pigeoneer /etc/pigeons/pigeoneer.toml
    chmod 0640 /etc/pigeons/pigeoneer.toml
fi

# Pre-create environment file with 0640 permissions before writing secrets to eliminate any world-readable window.
# Format values as systemd EnvironmentFile= double-quoted strings (escaping only backslashes and double quotes).
systemd_env_quote() {
    local val="$1"
    val="${val//\\/\\\\}"
    val="${val//\"/\\\"}"
    printf '"%s"' "${val}"
}

install -m 0640 -o root -g pigeoneer /dev/null /etc/pigeons/pigeoneer.env
{
    printf 'PIGEONEER_NODE_ID=%s\n' "$(systemd_env_quote "${NODE_ID}")"
    printf 'PIGEONEER_BROKER_URL=%s\n' "$(systemd_env_quote "${BROKER_URL}")"
    printf 'PIGEONEER_TAGS=%s\n' "$(systemd_env_quote "${NODE_TAGS:-${GO_ARCH}}")"
    printf 'PIGEONEER_MQTT_USERNAME=%s\n' "$(systemd_env_quote "${MQTT_USER}")"
    printf 'PIGEONEER_MQTT_PASSWORD=%s\n' "$(systemd_env_quote "${MQTT_PASS}")"
} > /etc/pigeons/pigeoneer.env

SUPP_GROUPS_DIRECTIVE="# SupplementaryGroups=video render  # Set PIGEONEER_ENABLE_GPU_GROUPS=true if DRM/sysfs GPU/NPU access is restricted"
if [[ "${ENABLE_GPU_GROUPS}" == "true" ]]; then
    SUPP_GROUPS_LIST=()
    for grp in video render; do
        if getent group "${grp}" >/dev/null 2>&1; then
            SUPP_GROUPS_LIST+=("${grp}")
        fi
    done
    if [[ "${#SUPP_GROUPS_LIST[@]}" -gt 0 ]]; then
        SUPP_GROUPS_DIRECTIVE="SupplementaryGroups=${SUPP_GROUPS_LIST[*]}"
    fi
fi

echo "[*] Configuring hardened systemd service..."
cat <<EOF > /etc/systemd/system/pigeoneer.service
[Unit]
Description=CarrierPigeons Edge Observer & Runner Daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pigeoneer
Group=pigeoneer
${SUPP_GROUPS_DIRECTIVE}
EnvironmentFile=/etc/pigeons/pigeoneer.env
ExecStart=/usr/local/bin/pigeoneer --config /etc/pigeons/pigeoneer.toml
Restart=always
RestartSec=5
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
EOF

echo "[*] Enabling and starting Pigeoneer..."
systemctl daemon-reload
systemctl enable --now pigeoneer

echo "[+] Node ${NODE_ID} (${GO_ARCH}) is online and reporting to ${BROKER_URL}."
