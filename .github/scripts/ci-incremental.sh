#!/usr/bin/env bash
# Governance always covers the complete checkout; codec gates check separate feature graphs.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

python3 - <<'PY'
from pathlib import Path
import re
import subprocess
import tomllib

paths = sorted(set(subprocess.check_output(
    ['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z']
).decode().rstrip('\0').split('\0')))
sections = {'dependencies', 'dev-dependencies', 'build-dependencies'}
forbidden = re.compile(r'^(?:tokio|futures|async|smol)(?:-|$)')
key_atom = r'''(?:"(?:[^"\\]|\\.)*"|'[^']*'|[A-Za-z0-9_-]+)'''
key_assignment = re.compile(r'^(' + key_atom + r'(?:\s*\.\s*' + key_atom + r')*)\s*=\s*(.*)$')
assignment = re.compile(r'''^(?:"(?:[^"\\]|\\.)*"|'[^']*'|[A-Za-z0-9_-]+)\s*=\s*\{''')
errors = []
source_suffixes = {'.rs', '.c', '.cc', '.cpp', '.h', '.hpp', '.sh', '.py'}

def marker_path(node, path=()):
    if isinstance(node, dict):
        if '__ci_marker__' in node:
            return path
        for key, value in node.items():
            result = marker_path(value, (*path, key))
            if result is not None:
                return result
    elif isinstance(node, list):
        for value in node:
            result = marker_path(value, path)
            if result is not None:
                return result
    return None

def inspect_dependencies(node, manifest):
    if not isinstance(node, dict):
        return
    for key, value in node.items():
        if key in sections and isinstance(value, dict):
            for alias, dependency in value.items():
                package = dependency.get('package', alias) if isinstance(dependency, dict) else alias
                if not isinstance(dependency, dict):
                    errors.append(f'{manifest}: dependency {alias} must use an inline table')
                if forbidden.match(alias) or forbidden.match(package):
                    errors.append(f'{manifest}: forbidden async dependency {alias} ({package})')
        elif isinstance(value, dict):
            inspect_dependencies(value, manifest)

for name in paths:
    path = Path(name)
    if not path.is_file() or path.is_symlink():
        continue
    if path.suffix in source_suffixes:
        with path.open('rb') as source:
            lines = sum(1 for _ in source)
        if lines > 800:
            errors.append(f'{path}: {lines} lines exceeds the 800-line source limit')
    if path.name != 'Cargo.toml':
        continue
    text = path.read_text()
    document = tomllib.loads(text)
    inspect_dependencies(document, path)
    section = ()
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('['):
            section = marker_path(tomllib.loads(line + '\n__ci_marker__ = true'))
            if any(part in sections for part in section[:-1]):
                errors.append(f'{path}:{number}: dependency subtables must use inline tables')
        else:
            if section and section[-1] in sections and not assignment.match(line):
                errors.append(f'{path}:{number}: dependencies must use key = {{ ... }} inline tables')
            match = key_assignment.match(line)
            if match:
                key = marker_path(tomllib.loads(match[1] + ' = {__ci_marker__ = true}'))
                full_key = (*section, *key)
                for index, part in enumerate(full_key):
                    if part in sections and index < len(full_key) - 1:
                        if len(full_key) != index + 2 or not match[2].startswith('{'):
                            errors.append(f'{path}:{number}: dependency entries must be inline tables')
if errors:
    raise SystemExit('\n'.join(errors))
if not Path('Cargo.toml').is_file():
    raise SystemExit('Root Cargo.toml is missing; Cargo gates cannot run')
print('Source size, dependency syntax, and manifest async policy passed')
PY

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
