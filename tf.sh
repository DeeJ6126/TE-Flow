#!/usr/bin/env bash
set -e
APP=$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")
export TEFLOW_SETTINGS="${TEFLOW_SETTINGS:-$APP/settings.yaml}"
export PYTHONPATH="$APP${PYTHONPATH:+:$PYTHONPATH}"
exec "${TEFLOW_PYTHON:-python}" -c 'from teflow.cli import shortcut_main; raise SystemExit(shortcut_main())' "$@"
