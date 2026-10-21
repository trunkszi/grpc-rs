#!/usr/bin/env python3
"""Validate every tracked or nonignored file before running Cargo gates."""
from pathlib import Path
import re
import subprocess
import tomllib

paths = sorted(set(subprocess.check_output(
    ['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z']
).decode().rstrip('\0').split('\0')))
tracked_links = {
    entry.split('\t', 1)[1]
    for entry in subprocess.check_output(['git', 'ls-files', '--stage', '-z'])
    .decode().rstrip('\0').split('\0')
    if entry.startswith('120000 ')
}
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
    if path.is_symlink() or name in tracked_links:
        errors.append(f'{path}: symlinks are forbidden in governed checkout files')
        continue
    if not path.is_file():
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
