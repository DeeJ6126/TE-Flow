#!/root/miniconda3/envs/TE/bin/python
"""Enter a repository token interactively; keep it outside the source tree."""
from __future__ import annotations

import getpass
import os
from pathlib import Path
import shlex
import subprocess
import sys


REPOSITORY = Path('/root/teflow')
CREDENTIALS = Path('/home/codex/.git-credentials')
URL = 'https://github.com/DeeJ6126/TE-Flow.git'


def main() -> int:
    if os.geteuid() != 0:
        print('请在 213 服务器的 root 终端中运行此命令。')
        return 1
    if not sys.stdin.isatty():
        print('请在交互终端输入 Token。')
        return 1
    # Load the already configured HTTP proxy for the Git connection.
    proxy_file = Path('/home/codex/server-213-setup/proxy.env')
    allowed = {'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy',
               'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY'}
    if proxy_file.is_file():
        for line in proxy_file.read_text().splitlines():
            tokens = shlex.split(line, comments=True)
            if tokens and tokens[0] == 'export':
                tokens = tokens[1:]
            for token in tokens:
                name, sep, value = token.partition('=')
                if sep and name in allowed:
                    os.environ[name] = value
    print('仓库：DeeJ6126/TE-Flow')
    print('提交身份：DeeJ6126 <1498607332@qq.com>')
    print('凭据将交给 Git 保存到 /home/codex/.git-credentials（权限 600）。')
    secret = getpass.getpass('粘贴 GitHub Token 后按回车（输入不显示）：').strip()
    if not secret or any(char in secret for char in '\r\n\0'):
        print('Token 为空或格式无效，请重新运行。')
        return 1
    credential = ('protocol=https\nhost=github.com\npath=DeeJ6126/TE-Flow.git\n'
                  'username=DeeJ6126\npassword=' + secret + '\n\n')
    command = ['git', '-c', 'credential.helper=',
               '-c', 'credential.helper=store --file=' + str(CREDENTIALS),
               '-c', 'credential.useHttpPath=true', 'credential', 'approve']
    result = subprocess.run(command, input=credential, capture_output=True, text=True)
    if result.returncode:
        print('Git 凭据保存失败：' + result.stderr.replace(secret, '[hidden]').strip())
        return 1
    CREDENTIALS.chmod(0o600)
    environment = dict(os.environ, GIT_TERMINAL_PROMPT='0', GIT_ASKPASS='/bin/false')
    try:
        result = subprocess.run(['git', '-c', 'safe.directory=' + str(REPOSITORY),
                                 '-C', str(REPOSITORY), 'ls-remote', '--heads', URL],
                                env=environment, capture_output=True, text=True, timeout=25)
    except subprocess.TimeoutExpired:
        print('Token 已保存；连接 GitHub 超时，请检查现有代理连接。')
        return 1
    if result.returncode:
        print('Token 已保存，但仓库访问未通过。请检查 Token 的仓库授权及有效期。')
        print(result.stderr.replace(secret, '[hidden]').strip())
        return 1
    branches = [line.split('\t', 1)[1] for line in result.stdout.splitlines() if '\t' in line]
    print('仓库连接成功。现有分支：' + (', '.join(branches) if branches else '空仓库'))
    print('认证已配置；尚未提交或推送代码。')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
