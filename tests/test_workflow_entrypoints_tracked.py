import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ENTRYPOINTS = [
    "scripts/check_env.py",
    "scripts/probe_backup_source.py",
    "scripts/daily_job.py",
    "scripts/verify_supabase_load.py",
]


def test_workflow_entrypoints_are_tracked_by_git() -> None:
    for relative_path in WORKFLOW_ENTRYPOINTS:
        result = subprocess.run(
            ["git", "ls-files", "--error-unmatch", relative_path],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, relative_path
