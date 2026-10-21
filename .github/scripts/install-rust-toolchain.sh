#!/usr/bin/env bash
# Install the repository's requested components without removing shared toolchains.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
command -v rustup >/dev/null
python3 - <<'PY'
import os
from pathlib import Path
import subprocess
import tomllib

with open('rust-toolchain.toml', 'rb') as source:
    toolchain = tomllib.load(source)['toolchain']
channel = toolchain['channel']
if not isinstance(channel, str) or not channel or channel.startswith('-'):
    raise SystemExit('Invalid toolchain channel')
args = ['rustup', 'toolchain', 'install', channel, '--profile', toolchain.get('profile', 'minimal')]
for component in dict.fromkeys([*toolchain.get('components', []), 'rustfmt', 'clippy']):
    args.extend(['--component', component])
for target in toolchain.get('targets', []):
    args.extend(['--target', target])
# A host may share rustup storage between runners; serialize installations only.
home = Path(os.environ.get('RUSTUP_HOME', Path.home() / '.rustup'))
home.mkdir(parents=True, exist_ok=True)
import fcntl
with open(home / 'grpc-rs-ci.lock', 'w') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    subprocess.run(args, check=True)
if os.environ.get('GITHUB_ENV'):
    with open(os.environ['GITHUB_ENV'], 'a', encoding='utf-8') as output:
        if '\n' in channel or '\r' in channel:
            raise SystemExit('Invalid toolchain channel')
        output.write(f'RUSTUP_TOOLCHAIN={channel}\n')
subprocess.run(['rustup', 'run', channel, 'rustc', '--version'], check=True)
PY
