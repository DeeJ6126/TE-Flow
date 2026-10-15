#!/usr/bin/env bash
set -e
if [ -f /home/codex/server-213-setup/proxy.env ]; then
    source /home/codex/server-213-setup/proxy.env
fi
export TEFLOW_SETTINGS=/root/teflow/settings.yaml
export PATH=/root/miniconda3/envs/TE/bin:$PATH
exec /root/miniconda3/envs/TE/bin/teflow "$@"
