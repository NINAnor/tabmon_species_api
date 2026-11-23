#!/bin/bash

set -o errexit
set -o pipefail

# APP selects which app to run: "expert" or "citizen". It is required so a
# deployment can never silently serve the wrong app.
case "${APP:-}" in
  expert|citizen) ;;
  *) echo "APP must be set to 'expert' or 'citizen' (got '${APP:-}')" >&2; exit 1 ;;
esac

exec uv run streamlit run "src/${APP}_app.py"
