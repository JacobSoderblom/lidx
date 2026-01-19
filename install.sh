#!/bin/sh
# Install lidx — code indexer with MCP server for LLM-assisted code navigation
# Usage: curl -fsSL https://raw.githubusercontent.com/JacobSoderblom/lidx/main/install.sh | bash
set -eu

REPO="JacobSoderblom/lidx"
INSTALL_DIR="$HOME/.local/bin"

echo "==> Installing lidx"

# Detect OS and architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Linux)  OS_TAG="unknown-linux-gnu" ;;
    Darwin) OS_TAG="apple-darwin" ;;
    *)      echo "Unsupported OS: $OS"; exit 1 ;;
esac

case "$ARCH" in
    x86_64|amd64)  ARCH_TAG="x86_64" ;;
    arm64|aarch64) ARCH_TAG="aarch64" ;;
    *)             echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

TARGET="${ARCH_TAG}-${OS_TAG}"
DOWNLOAD_URL="https://github.com/${REPO}/releases/latest/download/lidx-${TARGET}.tar.gz"

echo "==> Detected platform: ${TARGET}"
echo "==> Downloading from: ${DOWNLOAD_URL}"

# Download and extract
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

curl -fsSL "$DOWNLOAD_URL" -o "$TMPDIR/lidx.tar.gz"
tar -xzf "$TMPDIR/lidx.tar.gz" -C "$TMPDIR"

# Install binary
mkdir -p "$INSTALL_DIR"
mv "$TMPDIR/lidx" "$INSTALL_DIR/lidx"
chmod +x "$INSTALL_DIR/lidx"

# Check PATH
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
    echo ""
    echo "WARNING: $INSTALL_DIR is not on your PATH."
    echo "Add it to your shell profile:"
    echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
    echo ""
fi

# Verify
if command -v lidx >/dev/null 2>&1; then
    echo "==> Installed $(lidx --version)"
else
    echo "==> Installed lidx to $INSTALL_DIR/lidx"
fi

echo ""
echo "Add .lidx to your repo's .gitignore:"
echo ""
echo '  echo ".lidx" >> .gitignore'
echo ""
echo "Add lidx to your repo's .mcp.json:"
echo ""
echo '  {'
echo '    "mcpServers": {'
echo '      "lidx": {'
echo '        "command": "lidx",'
echo '        "args": ["mcp-serve", "--repo", "."]'
echo '      }'
echo '    }'
echo '  }'
echo ""
