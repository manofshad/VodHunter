import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which("sh"), "requires a POSIX shell")
class TestPublicRollingStartup(unittest.TestCase):
    def run_startup(self, migration_exit_code=0):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = root / "bin"
            commands.mkdir()
            trace = root / "trace"
            template = root / "template"
            template.write_text("nginx configuration\n")

            scripts = {
                "alembic": (
                    'printf "migration:%s:%s\\n" "$*" "$DATABASE_URL" >> "$TRACE"\n'
                    f"exit {migration_exit_code}\n"
                ),
                "envsubst": 'echo configure >> "$TRACE"\ncat\n',
                "nginx": 'echo validate >> "$TRACE"\n',
                "server": 'printf "server:%s\\n" "$*" >> "$TRACE"\n',
            }
            for name, body in scripts.items():
                command = commands / name
                command.write_text("#!/bin/sh\n" + body)
                command.chmod(0o755)

            # Redirect container-only paths into the isolated test directory.
            script = (ROOT_DIR / "docker/public-rolling/entrypoint.sh").read_text()
            script = script.replace(
                "/etc/nginx/templates/default.conf.template", str(template)
            ).replace("/etc/nginx/conf.d/default.conf", str(root / "nginx.conf"))
            entrypoint = root / "entrypoint.sh"
            entrypoint.write_text(script)
            result = subprocess.run(
                ["sh", str(entrypoint), "server", "--serve"],
                cwd=root,
                env={
                    **os.environ,
                    "PATH": str(commands) + os.pathsep + os.environ["PATH"],
                    "TRACE": str(trace),
                    "DATABASE_URL": "postgresql://test-db/vodhunter",
                },
                capture_output=True,
                text=True,
                timeout=10,
            )
            return result, trace.read_text().splitlines() if trace.exists() else []

    def test_migrates_database_before_starting_server(self):
        result, events = self.run_startup()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            events,
            [
                "migration:upgrade head:postgresql://test-db/vodhunter",
                "configure",
                "validate",
                "server:--serve",
            ],
        )

    def test_migration_failure_prevents_server_startup(self):
        result, events = self.run_startup(migration_exit_code=23)
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertEqual(events, ["migration:upgrade head:postgresql://test-db/vodhunter"])


if __name__ == "__main__":
    unittest.main()
