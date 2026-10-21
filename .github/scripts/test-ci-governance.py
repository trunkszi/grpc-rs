#!/usr/bin/env python3
"""Exercise the production governance entry point using isolated Git checkouts."""
from pathlib import Path
import subprocess
import tempfile
import unittest

GOVERNANCE = Path(__file__).with_name('ci-governance.py').resolve()


class GovernanceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.git('init', '--quiet')
        (self.root / 'Cargo.toml').write_text(
            '[package]\nname = "fixture"\nversion = "0.1.0"\n'
            '[dependencies]\nbytes = { version = "1" }\n'
        )
        (self.root / 'valid.rs').write_text('// valid\n' * 800)
        self.git('add', '.')

    def git(self, *args):
        return subprocess.run(
            ['git', *args], cwd=self.root, check=True, capture_output=True
        )

    def run_governance(self):
        return subprocess.run(
            ['python3', str(GOVERNANCE)], cwd=self.root,
            capture_output=True, text=True
        )

    def assert_rejected(self, name):
        result = self.run_governance()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(f'{name}: symlinks are forbidden', result.stderr)

    def test_regular_files_pass(self):
        result = self.run_governance()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('manifest async policy passed', result.stdout)

    def test_tracked_source_symlinks_rejected(self):
        with tempfile.TemporaryDirectory() as external:
            target = Path(external) / 'oversized-source'
            target.write_text('// oversized\n' * 801)
            for suffix in ('.rs', '.c', '.cc', '.cpp', '.h', '.hpp', '.sh', '.py'):
                with self.subTest(suffix=suffix):
                    name = f'linked{suffix}'
                    path = self.root / name
                    path.symlink_to(target)
                    self.git('add', name)
                    self.assert_rejected(name)
                    self.git('rm', '--force', name)

    def test_tracked_manifest_symlink_rejected(self):
        target = self.root / 'manifest-target'
        (self.root / 'Cargo.toml').rename(target)
        (self.root / 'Cargo.toml').symlink_to(target.name)
        self.git('add', '.')
        self.assert_rejected('Cargo.toml')

    def test_tracked_dangling_symlink_rejected(self):
        (self.root / 'dangling.rs').symlink_to('missing-source')
        self.git('add', 'dangling.rs')
        self.assert_rejected('dangling.rs')

    def test_tracked_non_source_symlink_rejected(self):
        (self.root / 'linked.txt').symlink_to('valid.rs')
        self.git('add', 'linked.txt')
        self.assert_rejected('linked.txt')

    def test_untracked_symlink_rejected(self):
        (self.root / 'untracked.rs').symlink_to('valid.rs')
        self.assert_rejected('untracked.rs')

    def test_index_symlink_replaced_with_regular_file_rejected(self):
        path = self.root / 'indexed.rs'
        path.symlink_to('valid.rs')
        self.git('add', 'indexed.rs')
        path.unlink()
        path.write_text('// regular worktree replacement\n')
        self.assert_rejected('indexed.rs')

    def test_oversized_regular_source_still_rejected(self):
        (self.root / 'oversized.rs').write_text('// oversized\n' * 801)
        result = self.run_governance()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('801 lines exceeds the 800-line source limit', result.stderr)


if __name__ == '__main__':
    unittest.main()
