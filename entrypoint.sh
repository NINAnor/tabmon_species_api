#!/bin/bash

set -o errexit
set -o pipefail

# Create local data directory for validation responses
mkdir -p /app/data

# Start your API application
exec uv run streamlit run src/dashboard.py
