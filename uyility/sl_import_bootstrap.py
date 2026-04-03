# -*- coding: utf-8 -*-
"""
Bootstrap import per "Situazione Lavori".

Obiettivo:
- Popolare il DB bypassando la GUI, rispettando lo schema esistente:
  - jobs
  - job_meta
  - job_audit_events (opzionale ma utile per cronologia)
- Omette volutamente il campo "commessa" (non previsto nello schema mostrato).

Schema di riferimento:
- Tabella jobs + job_meta + audit tables. :contentReference[oaicite:2]{index=2}
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"


# ----------------------------
# Normalizzazioni difensive
# ----------------------------

def _norm_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_upper(value: Any) -> str:
    return _norm_text(value).upper()


def _norm_path_text(value: Any) -> str:
    """
    Normalizzazione “povera” ma stabile per confronto path.
    Nota: nel progetto esiste una path_compare_key più completa, ma qui
    evitiamo dipendenze. :contentReference[oaicite:3]{index=3}
    """
    text = _norm_text(value)
    if not text:
        return ""
    # uniforma separatori e case
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
    # nello schema: default 'NOT_SET' :contentReference[oaicite:4]{index=4}
    status = _norm_upper(value) or "NOT_SET"
    allowed = {"NOT_SET", "OK", "MISSING", "UNKNOWN"}
    # allowed è prudente: se vuoi, amplia in futuro
    return status if status in allowed else "NOT_SET"


def _utc_now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


@dataclass
class JobRow:
    # jobs
    project_base_path: str = ""
    dl_base_path: str = ""
    project_distretto_anno: str = ""
    project_name: str = ""
    project_mode: str = "GTN"
    dl_distretto_anno: str = ""
    dl_name: str = ""
    dl_insert_date: str = ""
    general_notes: str = ""

    # job_meta
    permits_mode: str = "REQUIRED"
    cartesio_delivery_scope: str = "NONE"
    project_tracciamento_manual_path: str = ""
    psc_path: str = ""
    psc_status: str = "NOT_SET"
    todo_json: str = ""
    permits_checklist_json: str = ""
    permits_notes: str = ""


# ----------------------------
# Lettura input (CSV)
# ----------------------------

def read_csv_rows(csv_path: Path) -> List[JobRow]:
    rows: List[JobRow] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            record = dict(raw or {})

            job = JobRow(
                project_base_path=_norm_text(record.get("project_base_path")),
                dl_base_path=_norm_text(record.get("dl_base_path")),
                project_distretto_anno=_norm_text(record.get("project_distretto_anno")),
                project_name=_norm_text(record.get("project_name")),
                project_mode=_normalize_project_mode(record.get("project_mode")),
                dl_distretto_anno=_norm_text(record.get("dl_distretto_anno")),
                dl_name=_norm_text(record.get("dl_name")),
                dl_insert_date=_norm_text(record.get("dl_insert_date")),
                general_notes=_norm_text(record.get("general_notes")),
                permits_mode=_normalize_permits_mode(record.get("permits_mode")),
                cartesio_delivery_scope=_normalize_cartesio_delivery_scope(record.get("cartesio_delivery_scope")),
                project_tracciamento_manual_path=_norm_text(record.get("project_tracciamento_manual_path")),
                psc_path=_norm_text(record.get("psc_path")),
                psc_status=_normalize_psc_status(record.get("psc_status")),
                todo_json=_norm_text(record.get("todo_json")),
                permits_checklist_json=_norm_text(record.get("permits_checklist_json")),
                permits_notes=_norm_text(record.get("permits_notes")),
            )
            rows.append(job)

    return rows


# ----------------------------
# DB helpers (SQLite diretto)
# ----------------------------

def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row

    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")
    cur.execute("PRAGMA busy_timeout = 30000;")
    # WAL su rete non va usato: nel progetto è DELETE. :contentReference[oaicite:5]{index=5}
    cur.execute("PRAGMA journal_mode = DELETE;")
    conn.commit()
    return conn


def ensure_meta_row(conn: sqlite3.Connection, job_id: int) -> None:
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO job_meta (job_id) VALUES (?)", (int(job_id),))


def insert_audit_event(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    action_kind: str,
    source_kind: str,
    origin_method: str,
    summary: str,
    initiated_by: str = "",
    machine_name: str = "",
    context_json: str = "",
) -> None:
    """
    Inserisce un evento in job_audit_events (no changes table per semplicità).
    Tabelle audit presenti nello schema. :contentReference[oaicite:6]{index=6}
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO job_audit_events (
            job_id, event_ts, action_kind, source_kind,
            initiated_by, machine_name, origin_method, summary, context_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(job_id),
            _utc_now_iso(),
            _norm_text(action_kind),
            _norm_text(source_kind),
            _norm_text(initiated_by),
            _norm_text(machine_name),
            _norm_text(origin_method),
            _norm_text(summary),
            _norm_text(context_json),
        ),
    )


def find_existing_job_id(conn: sqlite3.Connection, row: JobRow) -> Optional[int]:
    """
    Strategia: match su project_base_path o dl_base_path normalizzati.
    NB: non c’è vincolo UNIQUE nello schema, quindi qui scegliamo il primo match.
    :contentReference[oaicite:7]{index=7}
    """
    pkey = _norm_path_text(row.project_base_path)
    dkey = _norm_path_text(row.dl_base_path)

    if not pkey and not dkey:
        return None

    cur = conn.cursor()

    if pkey and dkey:
        cur.execute(
            """
            SELECT id, project_base_path, dl_base_path
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


def insert_job(conn: sqlite3.Connection, row: JobRow) -> int:
    """
    Inserisce su jobs e job_meta.
    Colonne jobs e job_meta sono nello schema. :contentReference[oaicite:8]{index=8}
    """
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
            _norm_text(row.project_base_path),
            _norm_text(row.dl_base_path),
            _norm_text(row.project_distretto_anno),
            _norm_text(row.project_name),
            _normalize_project_mode(row.project_mode),
            _norm_text(row.dl_distretto_anno),
            _norm_text(row.dl_name),
            _norm_text(row.dl_insert_date),
            _norm_text(row.general_notes),
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
            _normalize_permits_mode(row.permits_mode),
            _normalize_cartesio_delivery_scope(row.cartesio_delivery_scope),
            _norm_text(row.project_tracciamento_manual_path),
            _norm_text(row.psc_path),
            _normalize_psc_status(row.psc_status),
            _norm_text(row.todo_json),
            _norm_text(row.permits_checklist_json),
            _norm_text(row.permits_notes),
            job_id,
        ),
    )

    insert_audit_event(
        conn,
        job_id,
        action_kind="CREATE_JOB",
        source_kind="system",
        origin_method="bootstrap_import",
        summary="Creato lavoro via import iniziale",
    )

    return job_id


def run_import(
    conn: sqlite3.Connection,
    rows: Iterable[JobRow],
    *,
    skip_existing: bool = True,
) -> Tuple[int, int]:
    created = 0
    skipped = 0

    for row in rows:
        if not row.project_base_path and not row.dl_base_path:
            logging.warning("Riga ignorata: manca sia project_base_path sia dl_base_path.")
            skipped += 1
            continue

        existing_id = find_existing_job_id(conn, row)
        if existing_id is not None and skip_existing:
            logging.info("Skip: job già presente (id=%s) per path %s / %s", existing_id, row.project_base_path, row.dl_base_path)
            skipped += 1
            continue

        job_id = insert_job(conn, row)
        logging.info("Creato job id=%s | PRG=%s | DL=%s", job_id, row.project_base_path, row.dl_base_path)
        created += 1

    conn.commit()
    return created, skipped


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bootstrap import - Situazione Lavori")
    parser.add_argument("--db", required=True, help="Path al file .sqlite (es. S:\\...\\situazione_lavori.sqlite)")
    parser.add_argument("--csv", required=True, help="Path al CSV di input")
    parser.add_argument("--no-skip-existing", action="store_true", help="Se impostato, NON salta i job già esistenti (attenzione ai duplicati).")
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
    try:
        rows = read_csv_rows(csv_path)
        created, skipped = run_import(conn, rows, skip_existing=not bool(args.no_skip_existing))
        logging.info("Import completato | creati=%s | saltati=%s", created, skipped)
        return 0
    except Exception:
        logging.exception("Errore import bootstrap")
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())