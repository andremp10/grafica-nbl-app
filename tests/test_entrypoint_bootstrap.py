from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENTRYPOINTS = [
    ROOT / "scripts" / "check_env.py",
    ROOT / "scripts" / "daily_job.py",
    ROOT / "scripts" / "probe_backup_source.py",
    ROOT / "scripts" / "verify_supabase_load.py",
    ROOT / "etl" / "run.py",
]


def test_direct_entrypoints_bootstrap_project_root_for_scripts_imports() -> None:
    for path in ENTRYPOINTS:
        source = path.read_text(encoding="utf-8", errors="replace")
        if "from scripts." not in source:
            continue
        assert "PROJECT_ROOT = Path(__file__).resolve().parent.parent" in source, path.as_posix()
        assert "sys.path.insert(0, str(PROJECT_ROOT))" in source, path.as_posix()
