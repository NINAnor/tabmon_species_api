#!/bin/bash

set -o errexit
set -o pipefail

# Start rclone mount in background
echo "Starting rclone mount..."
rclone mount nirds3:bencretois-ns8129k-proj-tabmon /data \
    --allow-other \
    --allow-non-empty \
    --vfs-cache-mode writes \
    --log-level INFO \
    --daemon

# Wait a moment for mount to be ready
sleep 3

# Verify mount is working
if [ -d "/data" ]; then
    echo "Rclone mount successful, /data directory exists"
    ls -la /data | head -10
else
    echo "ERROR: Rclone mount failed"
    exit 1
fi

# Start your API application
echo "Starting API application..."
exec uv run streamlit run src/dashboard.py
