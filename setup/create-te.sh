#!/usr/bin/env bash
set -euo pipefail
umask 002
APP=/root/teflow
TE_PREFIX=/root/miniconda3/envs/TE
STATUS="$APP/setup/install-status.json"
trap 'code=$?; printf "{\"exit_code\":%d,\"finished\":true}\n" "$code" > "$STATUS"' EXIT
if [[ -f /home/codex/server-213-setup/proxy.env ]]; then
    source /home/codex/server-213-setup/proxy.env
fi
export NO_PROXY="${NO_PROXY:-127.0.0.1,localhost},mirrors.zju.edu.cn"
export no_proxy="$NO_PROXY"
export MAMBA_ROOT_PREFIX=/home/codex/.cache/teflow-mamba
mkdir -p "$APP/setup" /home/codex/.local/bin "$MAMBA_ROOT_PREFIX"
if [[ ! -x /home/codex/.local/bin/micromamba ]]; then
    curl -fsSL --connect-timeout 15 --max-time 180 --retry 2 \
        https://micro.mamba.pm/api/micromamba/linux-64/latest \
        -o "$APP/setup/micromamba.tar.bz2"
    tar -xjf "$APP/setup/micromamba.tar.bz2" -C "$APP/setup" bin/micromamba
    install -m 755 "$APP/setup/bin/micromamba" /home/codex/.local/bin/micromamba
fi
/home/codex/.local/bin/micromamba --version
/home/codex/.local/bin/micromamba --no-rc create -y -p "$TE_PREFIX" \
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
python -m pip freeze > "$APP/setup/pip-lock.txt"
/home/codex/.local/bin/micromamba --no-rc list -p "$TE_PREFIX" --explicit \
    > "$APP/setup/conda-explicit.txt"
mkdir -p "$TE_PREFIX/etc/conda/activate.d"
printf '%s\n' 'if [ -f /home/codex/server-213-setup/proxy.env ]; then source /home/codex/server-213-setup/proxy.env; fi' \
    > "$TE_PREFIX/etc/conda/activate.d/teflow-proxy.sh"
echo TE_ENVIRONMENT_READY
