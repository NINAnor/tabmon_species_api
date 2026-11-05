#!/bin/bash

set -o errexit
set -o pipefail

# APP selects which app to run: "expert" (default) or "citizen"
APP="${APP:-expert}"

case "$APP" in
  expert|citizen) ;;
  *) echo "Unknown APP '$APP' (expected 'expert' or 'citizen')" >&2; exit 1 ;;
esac

exec uv run streamlit run "src/${APP}_app.py"
