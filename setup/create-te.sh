#!/usr/bin/env bash
set -euo pipefail
umask 002
APP=$(dirname -- "$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")")
TE_PREFIX=${TEFLOW_ENV_PREFIX:-$HOME/miniconda3/envs/TE}
MAMBA_BIN="$HOME/.local/bin/micromamba"
STATUS="$APP/setup/install-status.json"
trap 'code=$?; printf "{\"exit_code\":%d,\"finished\":true}\n" "$code" > "$STATUS"' EXIT
if [[ -n ${TEFLOW_PROXY_ENV:-} ]]; then
    source "$TEFLOW_PROXY_ENV"
fi
export NO_PROXY="${NO_PROXY:-127.0.0.1,localhost},mirrors.zju.edu.cn"
export no_proxy="$NO_PROXY"
export MAMBA_ROOT_PREFIX="${MAMBA_ROOT_PREFIX:-$HOME/.cache/teflow-mamba}"
mkdir -p "$APP/setup" "$(dirname -- "$MAMBA_BIN")" "$MAMBA_ROOT_PREFIX"
if [[ ! -x "$MAMBA_BIN" ]]; then
    curl -fsSL --connect-timeout 15 --max-time 180 --retry 2 \
        https://micro.mamba.pm/api/micromamba/linux-64/latest \
        -o "$APP/setup/micromamba.tar.bz2"
    tar -xjf "$APP/setup/micromamba.tar.bz2" -C "$APP/setup" bin/micromamba
    install -m 755 "$APP/setup/bin/micromamba" "$MAMBA_BIN"
fi
"$MAMBA_BIN" --version
"$MAMBA_BIN" --no-rc create -y -p "$TE_PREFIX" \
    --strict-channel-priority \
    -c https://mirrors.zju.edu.cn/anaconda/cloud/conda-forge \
    -c https://mirrors.zju.edu.cn/anaconda/cloud/bioconda \
    'python=3.12' 'iseq=1.9.9' 'star=2.7.11b' fastp pysam pandas pip 'setuptools>=69'
export PATH="$TE_PREFIX/bin:$PATH"
python -m pip install --index-url https://mirrors.zju.edu.cn/pypi/web/simple \
    'snakemake==9.27.0' 'snakemake-interface-common==1.22.0' \
    'SQLAlchemy==2.0.36' 'pydantic==2.12.5' 'pulp==3.3.2' \
    'TEtranscripts==2.2.4' 'textual==6.12.0' 'psutil>=5.9' pytest pytest-asyncio
python -m pip install --no-deps --no-build-isolation -e "$APP"
chmod 755 "$APP/launch.sh"
bash "$APP/setup/install-shortcut.sh"
python -m pip check
python --version
iseq --version
fastp --version
STAR --version
TEcount --version
snakemake --version
python -m pip list --format=freeze --exclude teflow > "$APP/setup/pip-lock.txt"
"$MAMBA_BIN" --no-rc list -p "$TE_PREFIX" --explicit \
    > "$APP/setup/conda-explicit.txt"
echo TE_ENVIRONMENT_READY
