#!/usr/bin/env bash
# Keep build and temporary files in a directory owned by this job invocation.
set -euo pipefail

: "${GITHUB_WORKSPACE:?Run this check inside a GitHub Actions job}"
: "${RUNNER_WORKSPACE:?Runner workspace is required}"
: "${RUNNER_TEMP:?Runner temporary directory is required}"
: "${GITHUB_ENV:?GitHub environment file is required}"

python3 - <<'PY'
import os
from pathlib import Path
import tempfile

workspace = Path(os.environ['GITHUB_WORKSPACE']).resolve(strict=True)
runner = Path(os.environ['RUNNER_WORKSPACE']).resolve(strict=True)
if workspace == runner or not workspace.is_relative_to(runner):
    raise SystemExit(f'Checkout {workspace} must be inside runner workspace {runner}')
if not (workspace / '.git').exists():
    raise SystemExit('The runner workspace is not a Git checkout')
root = Path(os.environ['RUNNER_TEMP']).resolve(strict=True)
owned = Path(tempfile.mkdtemp(prefix='grpc-rs-ci.', dir=root))
temporary = owned / 'tmp'
temporary.mkdir()
values = {
    'GRPC_CI_RUN_DIR': owned,
    'TMPDIR': temporary,
    'CARGO_TARGET_DIR': owned / 'target',
}
with open(os.environ['GITHUB_ENV'], 'a', encoding='utf-8') as output:
    for key, value in values.items():
        if '\n' in str(value) or '\r' in str(value):
            raise SystemExit('CI paths must not contain line breaks')
        output.write(f'{key}={value}\n')
print(f'Isolated build directory: {owned}')
PY
