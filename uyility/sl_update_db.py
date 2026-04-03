# -*- coding: utf-8 -*-
"""
Update DB per "Situazione Lavori".

Obiettivo:
- Aggiornare record esistenti in jobs/job_meta partendo da un CSV.
- Matching su project_base_path o dl_base_path normalizzati.
- Opzione: se non trova match, può creare un nuovo job.

Schema di riferimento: jobs + job_meta. :contentReference[oaicite:9]{index=9}
"""

from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"


def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_upper(value: Any) -> str:
    return _norm_text(value).upper()


def _norm_path_text(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return ""
    text = text.replace("/", "\\")
    while "\\\\" in text:
        text = text.replace("\\\\", "\\")
    return text.strip().lower()


def _normalize_project_mode(value: Any) -> str:
    mode = _norm_upper(value) or "GTN"
    allowed = {"GTN", "ALTRA_DITTA", "PROGETTO_NON_PREVISTO"}
    return mode if mode in allowed else "GTN"


def _normalize_permits_mode(value: Any) -> str:
    mode = _norm_upper(value) or "REQUIRED"
    allowed = {"REQUIRED", "NOT_REQUIRED"}
    return mode if mode in allowed else "REQUIRED"


def _normalize_cartesio_delivery_scope(value: Any) -> str:
    scope = _norm_upper(value) or "NONE"
    allowed = {"NONE", "PRG", "COS", "ACC"}
    return scope if scope in allowed else "NONE"


def _normalize_psc_status(value: Any) -> str:
    status = _norm_upper(value) or "NOT_SET"
    allowed = {"NOT_SET", "OK", "MISSING", "UNKNOWN"}
    return status if status in allowed else "NOT_SET"


def _utc_now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row

    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("PRAGMA busy_timeout = 30000;")
    cur.execute("PRAGMA journal_mode = DELETE;")
    conn.commit()
    return conn


def ensure_meta_row(conn: sqlite3.Connection, job_id: int) -> None:
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO job_meta (job_id) VALUES (?)", (int(job_id),))


def find_job_id(conn: sqlite3.Connection, project_base_path: str, dl_base_path: str) -> Optional[int]:
    pkey = _norm_path_text(project_base_path)
    dkey = _norm_path_text(dl_base_path)

    if not pkey and not dkey:
        return None

    cur = conn.cursor()

    if pkey and dkey:
        cur.execute(
            """
            SELECT id
            FROM jobs
            WHERE LOWER(REPLACE(COALESCE(project_base_path,''), '/', '\\')) = ?
               OR LOWER(REPLACE(COALESCE(dl_base_path,''), '/', '\\')) = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (pkey, dkey),
        )
    elif pkey:
        cur.execute(
            """
            SELECT id
            FROM jobs
            WHERE LOWER(REPLACE(COALESCE(project_base_path,''), '/', '\\')) = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (pkey,),
        )
    else:
        cur.execute(
            """
            SELECT id
            FROM jobs
            WHERE LOWER(REPLACE(COALESCE(dl_base_path,''), '/', '\\')) = ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (dkey,),
        )

    hit = cur.fetchone()
    return int(hit["id"]) if hit else None


def create_job(conn: sqlite3.Connection, payload: Dict[str, str]) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO jobs (
            project_base_path,
            dl_base_path,
            project_distretto_anno,
            project_name,
            project_mode,
            dl_distretto_anno,
            dl_name,
            dl_insert_date,
            general_notes,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        (
            _norm_text(payload.get("project_base_path", "")),
            _norm_text(payload.get("dl_base_path", "")),
            _norm_text(payload.get("project_distretto_anno", "")),
            _norm_text(payload.get("project_name", "")),
            _normalize_project_mode(payload.get("project_mode", "")),
            _norm_text(payload.get("dl_distretto_anno", "")),
            _norm_text(payload.get("dl_name", "")),
            _norm_text(payload.get("dl_insert_date", "")),
            _norm_text(payload.get("general_notes", "")),
        ),
    )
    job_id = int(cur.lastrowid)
    ensure_meta_row(conn, job_id)
    cur.execute(
        """
        UPDATE job_meta
        SET
            permits_mode = ?,
            cartesio_delivery_scope = ?,
            project_tracciamento_manual_path = ?,
            psc_path = ?,
            psc_status = ?,
            todo_json = ?,
            permits_checklist_json = ?,
            permits_notes = ?
        WHERE job_id = ?
        """,
        (
            _normalize_permits_mode(payload.get("permits_mode", "")),
            _normalize_cartesio_delivery_scope(payload.get("cartesio_delivery_scope", "")),
            _norm_text(payload.get("project_tracciamento_manual_path", "")),
            _norm_text(payload.get("psc_path", "")),
            _normalize_psc_status(payload.get("psc_status", "")),
            _norm_text(payload.get("todo_json", "")),
            _norm_text(payload.get("permits_checklist_json", "")),
            _norm_text(payload.get("permits_notes", "")),
            job_id,
        ),
    )
    return job_id


def update_job(conn: sqlite3.Connection, job_id: int, payload: Dict[str, str]) -> None:
    cur = conn.cursor()

    # jobs: aggiorna solo i campi presenti (non vuoti) per evitare wipe accidentali
    job_fields = {
        "project_base_path": _norm_text(payload.get("project_base_path", "")),
        "dl_base_path": _norm_text(payload.get("dl_base_path", "")),
        "project_distretto_anno": _norm_text(payload.get("project_distretto_anno", "")),
        "project_name": _norm_text(payload.get("project_name", "")),
        "project_mode": _normalize_project_mode(payload.get("project_mode", "")) if _norm_text(payload.get("project_mode")) else "",
        "dl_distretto_anno": _norm_text(payload.get("dl_distretto_anno", "")),
        "dl_name": _norm_text(payload.get("dl_name", "")),
        "dl_insert_date": _norm_text(payload.get("dl_insert_date", "")),
        "general_notes": _norm_text(payload.get("general_notes", "")),
    }

    set_parts = []
    params = []

    for key, value in job_fields.items():
        if value == "":
            continue
        set_parts.append(f"{key} = ?")
        params.append(value)

    if set_parts:
        params.append(int(job_id))
        cur.execute(
            f"""
            UPDATE jobs
            SET {", ".join(set_parts)}, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            tuple(params),
        )

    ensure_meta_row(conn, job_id)

    # job_meta: stesso criterio “non vuoto”
    meta_updates = {
        "permits_mode": _normalize_permits_mode(payload.get("permits_mode", "")) if _norm_text(payload.get("permits_mode")) else "",
        "cartesio_delivery_scope": _normalize_cartesio_delivery_scope(payload.get("cartesio_delivery_scope", "")) if _norm_text(payload.get("cartesio_delivery_scope")) else "",
        "project_tracciamento_manual_path": _norm_text(payload.get("project_tracciamento_manual_path", "")),
        "psc_path": _norm_text(payload.get("psc_path", "")),
        "psc_status": _normalize_psc_status(payload.get("psc_status", "")) if _norm_text(payload.get("psc_status")) else "",
        "todo_json": _norm_text(payload.get("todo_json", "")),
        "permits_checklist_json": _norm_text(payload.get("permits_checklist_json", "")),
        "permits_notes": _norm_text(payload.get("permits_notes", "")),
    }

    meta_set = []
    meta_params = []
    for key, value in meta_updates.items():
        if value == "":
            continue
        meta_set.append(f"{key} = ?")
        meta_params.append(value)

    if meta_set:
        meta_params.append(int(job_id))
        cur.execute(
            f"""
            UPDATE job_meta
            SET {", ".join(meta_set)}
            WHERE job_id = ?
            """,
            tuple(meta_params),
        )


def read_csv(csv_path: Path) -> Tuple[int, list[dict[str, str]]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(r or {}) for r in reader]
        return len(rows), rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update DB - Situazione Lavori")
    parser.add_argument("--db", required=True, help="Path al file .sqlite")
    parser.add_argument("--csv", required=True, help="Path al CSV di input")
    parser.add_argument("--create-missing", action="store_true", help="Se un job non viene trovato, lo crea.")
    parser.add_argument("--log-level", default="INFO", help="DEBUG|INFO|WARNING|ERROR")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format=LOG_FORMAT)

    db_path = Path(str(args.db))
    csv_path = Path(str(args.csv))
    if not csv_path.is_file():
        logging.error("CSV non trovato: %s", csv_path)
        return 2

    conn = open_db(db_path)
    updated = 0
    created = 0
    skipped = 0

    try:
        _, rows = read_csv(csv_path)

        for payload in rows:
            project_base_path = _norm_text(payload.get("project_base_path", ""))
            dl_base_path = _norm_text(payload.get("dl_base_path", ""))

            job_id = find_job_id(conn, project_base_path, dl_base_path)
            if job_id is None:
                if args.create_missing:
                    new_id = create_job(conn, payload)
                    logging.info("Creato job id=%s (missing) | PRG=%s | DL=%s", new_id, project_base_path, dl_base_path)
                    created += 1
                else:
                    logging.warning("Skip: job non trovato | PRG=%s | DL=%s", project_base_path, dl_base_path)
                    skipped += 1
                continue

            update_job(conn, job_id, payload)
            logging.info("Aggiornato job id=%s", job_id)
            updated += 1

        conn.commit()
        logging.info("Update completato | updated=%s | created=%s | skipped=%s", updated, created, skipped)
        return 0

    except Exception:
        logging.exception("Errore update DB")
        conn.rollback()
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())