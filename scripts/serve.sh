#!/bin/bash
# Serve daily-stack on port 8080
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR" || exit 1

LOCAL_IP=$(ipconfig getifaddr en0 2>/dev/null || echo "<unknown>")
echo "=== Daily Stack ==="
echo "  Local:  http://localhost:8080"
echo "  iPhone: http://${LOCAL_IP}:8080"
echo ""
python3 -m http.server 8080
