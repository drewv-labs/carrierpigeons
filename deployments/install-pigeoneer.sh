#!/usr/bin/env bash
set -Eeuo pipefail

VERSION="${PIGEONEER_VERSION:-v1.0.0}"
REPO="${PIGEONEER_REPO:-drewv-labs/carrierpigeons}"
NODE_ID="${PIGEONEER_NODE_ID:-$(hostname)}"
BROKER_URL="${PIGEONEER_BROKER_URL:-tcp://pigeoncoop.local:1883}"

echo "[*] Detecting architecture for ${NODE_ID}..."
ARCH="$(uname -m)"
case "${ARCH}" in
    aarch64|arm64) BIN_NAME="pigeoneer-linux-arm64" ;;
    riscv64)       BIN_NAME="pigeoneer-linux-riscv64" ;;
    x86_64|amd64)  BIN_NAME="pigeoneer-linux-amd64" ;;
    *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac

DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${BIN_NAME}"

echo "[*] Downloading Pigeoneer ${VERSION} from GitHub (${DOWNLOAD_URL})..."
curl -fsSL -o /usr/local/bin/pigeoneer "${DOWNLOAD_URL}"
chmod +x /usr/local/bin/pigeoneer

echo "[*] Configuring Systemd Service..."
cat <<EOF > /etc/systemd/system/pigeoneer.service
[Unit]
Description=Carrier Pigeons Edge Telemetry Daemon
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/pigeoneer
Restart=always
RestartSec=5
Environment="PIGEONEER_NODE_ID=${NODE_ID}"
Environment="PIGEONEER_BROKER_URL=${BROKER_URL}"

[Install]
WantedBy=multi-user.target
EOF

echo "[*] Enabling and starting Pigeoneer..."
systemctl daemon-reload
systemctl enable --now pigeoneer

echo "[✔] Node ${NODE_ID} is online and reporting to ${BROKER_URL}."
