from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DbConfig:
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str

    pg_host: str
    pg_port: int
    pg_dbname: str
    pg_user: str
    pg_password: str
    pg_sslmode: str


def _mysql_connect(cfg: DbConfig):
    import mysql.connector

    return mysql.connector.connect(
        host=cfg.mysql_host,
        port=cfg.mysql_port,
        user=cfg.mysql_user,
        password=cfg.mysql_password,
        database=cfg.mysql_database,
        use_pure=True,
    )


def _pg_connect(cfg: DbConfig):
    import psycopg2

    return psycopg2.connect(
        host=cfg.pg_host,
        port=int(cfg.pg_port),
        dbname=cfg.pg_dbname,
        user=cfg.pg_user,
        password=cfg.pg_password,
        sslmode=cfg.pg_sslmode,
    )


def reconcile_counts(*, mapping_path: Path, out_dir: Path, tag: str, cfg: DbConfig) -> tuple[Path, Path]:
    mappings = json.loads(mapping_path.read_text(encoding="utf-8"))
    tables = sorted(mappings.keys())

    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / f"reconciliation_counts_{tag}.json"
    out_md = out_dir / f"reconciliation_counts_{tag}.md"

    mysql = _mysql_connect(cfg)
    pg = _pg_connect(cfg)
    try:
        mcur = mysql.cursor()
        pcur = pg.cursor()

        rows: list[dict[str, Any]] = []
        total_mysql = 0
        total_pg = 0

        for t in tables:
            mcur.execute(f"SELECT COUNT(*) FROM `{t}`")
            mc = int(mcur.fetchone()[0])
            pcur.execute(f'SELECT COUNT(*) FROM "{t}"')
            pc = int(pcur.fetchone()[0])
            diff = pc - mc
            rows.append(
                {
                    "table": t,
                    "mysql_count": mc,
                    "supabase_count": pc,
                    "diff": diff,
                    "status": "OK" if diff == 0 else "DIVERGENTE",
                }
            )
            total_mysql += mc
            total_pg += pc

        payload = {
            "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
            "tag": tag,
            "table_count": len(tables),
            "total_mysql": total_mysql,
            "total_supabase": total_pg,
            "total_diff": total_pg - total_mysql,
            "rows": rows,
        }
        out_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

        lines: list[str] = []
        lines.append("# Reconciliation (row counts)")
        lines.append("")
        lines.append(f"Generated at: {payload['generated_at']}")
        lines.append(f"Tag: `{tag}`")
        lines.append("")
        lines.append("| table | mysql_count | supabase_count | diff | status |")
        lines.append("| --- | ---:| ---:| ---:| --- |")
        for r in rows:
            lines.append(
                f"| {r['table']} | {r['mysql_count']} | {r['supabase_count']} | {r['diff']:+d} | {r['status']} |"
            )
        lines.append("")
        lines.append(f"Total mysql: {total_mysql}")
        lines.append(f"Total supabase: {total_pg}")
        lines.append(f"Total diff: {total_pg - total_mysql:+d}")
        lines.append("")
        out_md.write_text("\n".join(lines), encoding="utf-8")
    finally:
        try:
            mysql.close()
        except Exception:
            pass
        try:
            pg.close()
        except Exception:
            pass

    return out_json, out_md

