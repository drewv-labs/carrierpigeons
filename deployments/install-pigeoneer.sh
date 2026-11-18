#!/usr/bin/env bash
set -Eeuo pipefail

VERSION="${PIGEONEER_VERSION:-v0.2.0}"
REPO="${PIGEONEER_REPO:-drewv-labs/carrierpigeons}"
NODE_ID="${PIGEONEER_NODE_ID:-$(hostname)}"
BROKER_URL="${PIGEONEER_BROKER_URL:-tcp://pigeoncoop.local:1883}"
NODE_TAGS="${PIGEONEER_TAGS:-}"

echo "[*] Detecting architecture for ${NODE_ID}..."
ARCH="$(uname -m)"
case "${ARCH}" in
    riscv64)       BIN_NAME="pigeoneer-linux-riscv64" ;;
    aarch64|arm64) BIN_NAME="pigeoneer-linux-arm64" ;;
    x86_64|amd64)  BIN_NAME="pigeoneer-linux-amd64" ;;
    *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac

DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${BIN_NAME}"

echo "[*] Downloading Pigeoneer ${VERSION} (${ARCH}) from ${DOWNLOAD_URL}..."
curl -fsSL -o /usr/local/bin/pigeoneer "${DOWNLOAD_URL}"
chmod +x /usr/local/bin/pigeoneer

mkdir -p /etc/pigeons
if [[ ! -f /etc/pigeons/pigeoneer.toml ]]; then
cat <<EOF > /etc/pigeons/pigeoneer.toml
node_id        = "${NODE_ID}"
tags           = ["${ARCH}"]
poll_interval  = "5s"
spool_capacity = 10000
broker_url     = "${BROKER_URL}"
EOF
fi

echo "[*] Configuring Systemd Service..."
cat <<EOF > /etc/systemd/system/pigeoneer.service
[Unit]
Description=CarrierPigeons Edge Observer & Runner Daemon
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/pigeoneer --config /etc/pigeons/pigeoneer.toml
Restart=always
RestartSec=5
Environment="PIGEONEER_NODE_ID=${NODE_ID}"
Environment="PIGEONEER_BROKER_URL=${BROKER_URL}"
Environment="PIGEONEER_TAGS=${NODE_TAGS}"

[Install]
WantedBy=multi-user.target
EOF

echo "[*] Enabling and starting Pigeoneer..."
systemctl daemon-reload
systemctl enable --now pigeoneer

echo "[✔] Node ${NODE_ID} (${ARCH}) is online and reporting to ${BROKER_URL}."
