from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path

from dotenv import load_dotenv

from src.adapters.mysql_docker_loader import ensure_mysql_up, reset_and_import_dump
from src.pipelines.reconcile_counts import DbConfig, reconcile_counts
from src.utils.logging_setup import setup_logging
from src.utils.paths import ensure_dir, find_repo_root


def _pick_sql_file(sql_arg: str | None, *, repo_root: Path) -> Path:
    if sql_arg:
        p = Path(sql_arg).expanduser().resolve()
        if not p.exists() or p.suffix.lower() != ".sql":
            raise SystemExit(f"--sql inválido: {p}")
        return p

    sql_dir = repo_root / "sql_input"
    if not sql_dir.exists():
        raise SystemExit("Pasta `sql_input/` não existe.")

    files = sorted([p for p in sql_dir.iterdir() if p.is_file() and p.suffix.lower() == ".sql"])
    if len(files) == 0:
        raise SystemExit("Nenhum arquivo .sql encontrado em `sql_input/`.")
    if len(files) > 1:
        raise SystemExit("Existe mais de 1 arquivo .sql em `sql_input/`. Deixe apenas 1 ou use --sql.")
    return files[0].resolve()


def _required_env(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        raise SystemExit(f"Variável de ambiente obrigatória ausente: {name}")
    return v


def main() -> None:
    parser = argparse.ArgumentParser(description="MySQL dump (.sql) -> Supabase/Postgres ETL")
    parser.add_argument("--sql", help="Caminho do dump .sql (se omitido, usa o único .sql em sql_input/).")
    parser.add_argument("--skip-mysql-load", action="store_true", help="Não carrega o dump no MySQL (assume MySQL já pronto).")
    parser.add_argument("--skip-etl", action="store_true", help="Não roda o ETL (apenas carrega dump e/ou valida).")
    parser.add_argument("--skip-validate", action="store_true", help="Não roda validação (reconcile counts).")
    parser.add_argument("--verbose", action="store_true", help="Log verboso.")
    args = parser.parse_args()

    repo_root = find_repo_root()

    # Load local .env if present (NOT committed)
    load_dotenv(repo_root / ".env")
    load_dotenv()

    run_tag = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = ensure_dir(repo_root / "output")
    log_file = out_dir / f"run_{run_tag}.log"
    logger = setup_logging(log_file=log_file, verbose=bool(args.verbose))

    sql_path = _pick_sql_file(args.sql, repo_root=repo_root)
    logger.info(f"SQL dump: {sql_path.name}")

    mysql_db = os.getenv("MYSQL_DATABASE", "legacy")

    if not args.skip_mysql_load:
        # Ensure dump is inside sql_input so docker can read it (mounted read-only)
        sql_input_dir = ensure_dir(repo_root / "sql_input")
        if sql_path.parent != sql_input_dir:
            raise SystemExit("Para carregar via Docker automaticamente, coloque o .sql dentro de `sql_input/` ou use --skip-mysql-load.")

        logger.info("Subindo MySQL via Docker...")
        ensure_mysql_up(repo_root=repo_root)
        logger.info("Importando dump no MySQL (reset do schema local)...")
        reset_and_import_dump(repo_root=repo_root, sql_filename=sql_path.name, mysql_db=mysql_db)
        logger.info("MySQL pronto.")

    if not args.skip_etl:
        logger.info("Rodando ETL (MySQL -> Supabase)...")
        from src.etl.etl_v8 import run_etl_v8

        run_etl_v8(repo_root=repo_root, logger=logger)
        logger.info("ETL finalizado.")

    if not args.skip_validate:
        logger.info("Rodando validação (reconcile counts)...")
        cfg = DbConfig(
            mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            mysql_port=int(os.getenv("MYSQL_PORT", "3307")),
            mysql_user=os.getenv("MYSQL_USER", "root"),
            mysql_password=os.getenv("MYSQL_PASSWORD", "root"),
            mysql_database=mysql_db,
            pg_host=_required_env("PG_HOST"),
            pg_port=int(os.getenv("PG_PORT", "5432")),
            pg_dbname=os.getenv("PG_DBNAME", "postgres"),
            pg_user=_required_env("PG_USER"),
            pg_password=_required_env("PG_PASSWORD"),
            pg_sslmode=os.getenv("PG_SSLMODE", "require"),
        )
        mapping_path = repo_root / "config" / "column_mapping.json"
        if not mapping_path.exists():
            raise SystemExit(f"Mapping não encontrado: {mapping_path}")

        out_json, out_md = reconcile_counts(mapping_path=mapping_path, out_dir=out_dir, tag=run_tag, cfg=cfg)
        logger.info(f"Reconciliation JSON: {out_json}")
        logger.info(f"Reconciliation MD: {out_md}")

    logger.info(f"Log: {log_file}")


if __name__ == "__main__":
    main()

