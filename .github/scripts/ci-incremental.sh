#!/usr/bin/env bash
# Governance always covers the complete checkout; codec gates check separate feature graphs.
set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(git rev-parse --show-toplevel)"

python3 "$script_dir/ci-governance.py"
python3 "$script_dir/test-ci-governance.py"

# Audit the actual resolved dependency graph, including optional codec dependencies.
python3 - <<'PY'
import json
import re
import subprocess

metadata = json.loads(subprocess.check_output([
    'cargo', 'metadata', '--locked', '--format-version', '1', '--all-features'
]))
if not metadata.get('resolve'):
    raise SystemExit('Cargo metadata did not return a resolved dependency graph')
resolved = {node['id'] for node in metadata['resolve']['nodes']}
forbidden = re.compile(r'^(?:tokio|futures|async|smol)(?:-|$)')
violations = sorted({package['name'] for package in metadata['packages']
                     if package['id'] in resolved and forbidden.match(package['name'])})
if violations:
    raise SystemExit('Forbidden async dependencies in resolved graph: ' + ', '.join(violations))
print('Resolved dependency graph contains no forbidden async packages')
PY

cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features

# Select the facade as the root: other workspace packages must not activate its codecs.
for features in none fory capnp; do
  flags=(--locked -p grpc --no-default-features)
  if [ "$features" != none ]; then
    flags+=(--features "$features")
  fi
  echo "Checking facade feature selection: $features"
  if [ "$features" = fory ]; then
    tree="$(cargo tree "${flags[@]}" --edges normal,build --prefix none --format '{p}')"
    if printf '%s\n' "$tree" | python3 -c '
import re, sys
packages = {line.split()[0] for line in sys.stdin if line.strip()}
prost = sorted(name for name in packages if re.match(r"^prost(?:-|$)", name))
if prost:
    raise SystemExit("Fory-only graph includes protobuf packages: " + ", ".join(prost))
'; then
      echo 'Fory-only graph excludes prost packages'
    else
      exit 1
    fi
  fi
  cargo clippy "${flags[@]}" --all-targets -- -D warnings
  cargo test "${flags[@]}"
done

git diff --check
echo 'Workspace, dependency policy, and codec feature gates passed'
