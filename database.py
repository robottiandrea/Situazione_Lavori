# -*- coding: utf-8 -*-
"""Gestione database SQLite per Situazione Lavori."""
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils import (
    DB_FILE,
    path_compare_key,
    SCAN_OVERRIDEABLE_FIELDS as SHARED_SCAN_OVERRIDEABLE_FIELDS,
    get_current_machine_name,
    get_current_user_name,
)


class DatabaseManager:
    """Layer minimale sopra SQLite con row factory dict-like."""

    SCAN_CACHE_VERSION = 1
    SCAN_OVERRIDEABLE_FIELDS = set(SHARED_SCAN_OVERRIDEABLE_FIELDS)

    def __init__(self, db_path: Path | str = DB_FILE) -> None:
        self.db_path = str(db_path)

        # Timeout più alto: se un altro utente sta scrivendo, aspetta un po'
        # invece di fallire subito con "database is locked".
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row

        self._configure_connection()
        self._init_db()

    def _configure_connection(self) -> None:
        """
        Configurazione prudente per uso condiviso leggero su cartella di rete.
        WAL su rete non va usato.
        """
        logging.info("Configurazione connessione DB: %s", self.db_path)
        cur = self.conn.cursor()

        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("PRAGMA busy_timeout = 30000;")
        cur.execute("PRAGMA journal_mode = DELETE;")

        self.conn.commit()

    def _commit(self) -> None:
        """
        Commit centralizzato con messaggio più chiaro in caso di lock.
        """
        try:
            self.conn.commit()
        except sqlite3.OperationalError as exc:
            logging.exception("Errore SQLite durante commit")
            if "locked" in str(exc).lower():
                raise RuntimeError(
                    "Il database è momentaneamente occupato da un altro utente. "
                    "Riprova tra qualche secondo."
                ) from exc
            raise

    def _init_db(self) -> None:
        logging.info("Inizializzazione DB: %s", self.db_path)
        cur = self.conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_base_path TEXT,
                dl_base_path TEXT,
                project_distretto_anno TEXT,
                project_name TEXT,
                project_mode TEXT NOT NULL DEFAULT 'GTN',
                dl_distretto_anno TEXT,
                dl_name TEXT,
                dl_insert_date TEXT,
                general_notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_meta (
                job_id INTEGER PRIMARY KEY,
                permits_mode TEXT NOT NULL DEFAULT 'REQUIRED',
                permits_checklist_json TEXT,
                permits_notes TEXT,
                cartesio_prg_status TEXT,
                cartesio_prg_notes TEXT,
                cartesio_prg_manual_code TEXT,
                rilievi_dl_status TEXT,
                rilievi_dl_notes TEXT,
                cartesio_cos_status TEXT,
                cartesio_cos_notes TEXT,
                cartesio_cos_manual_code TEXT,
                psc_path TEXT,
                psc_status TEXT DEFAULT 'NOT_SET',
                todo_json TEXT,
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_scan_cache (
                job_id INTEGER PRIMARY KEY,
                scan_json TEXT NOT NULL,
                permits_display TEXT,
                cartesio_prg_display TEXT,
                rilievi_dl_display TEXT,
                cartesio_cos_display TEXT,
                revisions_match TEXT,
                scanned_at TEXT,
                scan_version INTEGER DEFAULT 1,
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_scan_overrides (
                job_id INTEGER NOT NULL,
                field_key TEXT NOT NULL,
                override_value TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (job_id, field_key),
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                event_ts TEXT NOT NULL,
                action_kind TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                initiated_by TEXT,
                machine_name TEXT,
                origin_method TEXT,
                summary TEXT,
                context_json TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_audit_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                field_scope TEXT NOT NULL,
                field_key TEXT NOT NULL,
                old_value_json TEXT,
                new_value_json TEXT,
                old_value_text TEXT,
                new_value_text TEXT,
                FOREIGN KEY(event_id) REFERENCES job_audit_events(id) ON DELETE CASCADE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_audit_user_state (
                job_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                last_seen_event_id INTEGER,
                checked_at TEXT,
                PRIMARY KEY (job_id, user_name)
            )
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_job_audit_events_job_id_id
            ON job_audit_events(job_id, id DESC)
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_job_audit_changes_event_id
            ON job_audit_changes(event_id)
            """
        )

        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_job_audit_user_state_user
            ON job_audit_user_state(user_name, job_id)
            """
        )

        self._ensure_schema_updates()
        self._commit()

    def _ensure_schema_updates(self) -> None:
        """
        Migrazioni leggere dello schema per database già esistenti.
        """
        cur = self.conn.cursor()

        cur.execute("PRAGMA table_info(jobs)")
        jobs_columns = {str(row["name"]) for row in cur.fetchall()}

        if "project_mode" not in jobs_columns:
            logging.info("Aggiunta colonna jobs.project_mode")
            cur.execute("ALTER TABLE jobs ADD COLUMN project_mode TEXT NOT NULL DEFAULT 'GTN'")

        cur.execute("PRAGMA table_info(job_meta)")
        meta_columns = {str(row["name"]) for row in cur.fetchall()}

        required_meta_columns = {
            "permits_mode": "TEXT NOT NULL DEFAULT 'REQUIRED'",
            "psc_path": "TEXT",
            "psc_status": "TEXT DEFAULT 'NOT_SET'",
        }

        for column_name, column_sql in required_meta_columns.items():
            if column_name not in meta_columns:
                logging.info("Aggiunta colonna job_meta.%s", column_name)
                cur.execute(f"ALTER TABLE job_meta ADD COLUMN {column_name} {column_sql}")

    def close(self) -> None:
        try:
            if getattr(self, "conn", None):
                self.conn.close()
                logging.info("Connessione DB chiusa: %s", self.db_path)
        except Exception:
            logging.exception("Errore durante la chiusura del DB")

    # -------------------------------------------------------------------------
    # AUDIT / STATO CONTROLLO UTENTE
    # -------------------------------------------------------------------------

    def _audit_to_json(self, value: Any) -> str:
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        except TypeError:
            return json.dumps(str(value), ensure_ascii=False, sort_keys=True)

    def _audit_to_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value)

    def _build_audit_change(
        self,
        field_scope: str,
        field_key: str,
        old_value: Any,
        new_value: Any,
    ) -> Optional[Dict[str, str]]:
        old_json = self._audit_to_json(old_value)
        new_json = self._audit_to_json(new_value)

        if old_json == new_json:
            return None

        return {
            "field_scope": field_scope,
            "field_key": field_key,
            "old_value_json": old_json,
            "new_value_json": new_json,
            "old_value_text": self._audit_to_text(old_value),
            "new_value_text": self._audit_to_text(new_value),
        }

    def _collect_field_changes(
        self,
        field_scope: str,
        old_data: Dict[str, Any],
        new_data: Dict[str, Any],
        field_names: List[str],
    ) -> List[Dict[str, str]]:
        old_data = old_data or {}
        new_data = new_data or {}

        changes: List[Dict[str, str]] = []
        for field_name in field_names:
            change = self._build_audit_change(
                field_scope=field_scope,
                field_key=field_name,
                old_value=old_data.get(field_name),
                new_value=new_data.get(field_name),
            )
            if change:
                changes.append(change)
        return changes

    def _create_audit_event(
        self,
        cur: sqlite3.Cursor,
        job_id: int,
        action_kind: str,
        source_kind: str,
        origin_method: str,
        summary: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> int:
        cur.execute(
            """
            INSERT INTO job_audit_events (
                job_id,
                event_ts,
                action_kind,
                source_kind,
                initiated_by,
                machine_name,
                origin_method,
                summary,
                context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                datetime.now().isoformat(timespec="seconds"),
                action_kind,
                source_kind,
                get_current_user_name(),
                get_current_machine_name(),
                origin_method,
                summary,
                json.dumps(context or {}, ensure_ascii=False, sort_keys=True),
            ),
        )
        return int(cur.lastrowid)

    def _insert_audit_changes(
        self,
        cur: sqlite3.Cursor,
        event_id: int,
        changes: List[Dict[str, str]],
    ) -> None:
        if not changes:
            return

        cur.executemany(
            """
            INSERT INTO job_audit_changes (
                event_id,
                field_scope,
                field_key,
                old_value_json,
                new_value_json,
                old_value_text,
                new_value_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    event_id,
                    change["field_scope"],
                    change["field_key"],
                    change["old_value_json"],
                    change["new_value_json"],
                    change["old_value_text"],
                    change["new_value_text"],
                )
                for change in changes
            ],
        )

    def _build_scan_cache_changes(
        self,
        before_row: Dict[str, Any],
        scan_data: Dict[str, Any],
        permits_display: str,
        cartesio_prg_display: str,
        rilievi_dl_display: str,
        cartesio_cos_display: str,
        revisions_match: str,
    ) -> List[Dict[str, str]]:
        changes: List[Dict[str, str]] = []

        old_scan = dict(before_row.get("scan") or {})
        new_scan = dict(scan_data or {})

        all_scan_keys = sorted(set(old_scan.keys()) | set(new_scan.keys()))
        for scan_key in all_scan_keys:
            change = self._build_audit_change(
                field_scope="job_scan_cache",
                field_key=f"scan.{scan_key}",
                old_value=old_scan.get(scan_key),
                new_value=new_scan.get(scan_key),
            )
            if change:
                changes.append(change)

        display_fields = [
            ("permits_display", before_row.get("permits_display", ""), permits_display or ""),
            ("cartesio_prg_display", before_row.get("cartesio_prg_display", ""), cartesio_prg_display or ""),
            ("rilievi_dl_display", before_row.get("rilievi_dl_display", ""), rilievi_dl_display or ""),
            ("cartesio_cos_display", before_row.get("cartesio_cos_display", ""), cartesio_cos_display or ""),
            ("revisions_match", before_row.get("revisions_match", ""), revisions_match or "UNKNOWN"),
        ]

        for field_key, old_value, new_value in display_fields:
            change = self._build_audit_change(
                field_scope="job_scan_cache",
                field_key=field_key,
                old_value=old_value,
                new_value=new_value,
            )
            if change:
                changes.append(change)

        return changes

    def get_latest_audit_event_map(self, job_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        if not job_ids:
            return {}

        placeholders = ",".join("?" for _ in job_ids)
        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT
                e.job_id,
                e.id AS event_id,
                e.event_ts,
                e.source_kind,
                e.summary
            FROM job_audit_events e
            INNER JOIN (
                SELECT job_id, MAX(id) AS max_event_id
                FROM job_audit_events
                WHERE job_id IN ({placeholders})
                GROUP BY job_id
            ) latest
                ON latest.job_id = e.job_id
               AND latest.max_event_id = e.id
            """,
            job_ids,
        )

        result: Dict[int, Dict[str, Any]] = {}
        for row in cur.fetchall():
            result[int(row["job_id"])] = {
                "event_id": int(row["event_id"]),
                "event_ts": str(row["event_ts"] or ""),
                "source_kind": str(row["source_kind"] or ""),
                "summary": str(row["summary"] or ""),
            }
        return result

    def get_user_seen_event_map(self, job_ids: List[int], user_name: str) -> Dict[int, int]:
        if not job_ids:
            return {}

        placeholders = ",".join("?" for _ in job_ids)
        params: List[Any] = [user_name, *job_ids]

        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT job_id, last_seen_event_id
            FROM job_audit_user_state
            WHERE user_name = ?
              AND job_id IN ({placeholders})
            """,
            params,
        )

        result: Dict[int, int] = {}
        for row in cur.fetchall():
            result[int(row["job_id"])] = int(row["last_seen_event_id"] or 0)
        return result

    def fetch_job_history_events(
        self,
        job_id: int,
        limit: int = 300,
        source_kind: str = "",
    ) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()

        if source_kind:
            cur.execute(
                """
                SELECT *
                FROM job_audit_events
                WHERE job_id = ?
                  AND source_kind = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (job_id, source_kind, limit),
            )
        else:
            cur.execute(
                """
                SELECT *
                FROM job_audit_events
                WHERE job_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (job_id, limit),
            )

        return [dict(row) for row in cur.fetchall()]

    def fetch_job_history_changes(self, event_id: int) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM job_audit_changes
            WHERE event_id = ?
            ORDER BY id ASC
            """,
            (event_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    def mark_job_history_checked(self, job_id: int, user_name: str) -> None:
        latest_map = self.get_latest_audit_event_map([job_id])
        latest_event_id = int(latest_map.get(job_id, {}).get("event_id") or 0)

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO job_audit_user_state (
                job_id,
                user_name,
                last_seen_event_id,
                checked_at
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(job_id, user_name) DO UPDATE SET
                last_seen_event_id = excluded.last_seen_event_id,
                checked_at = excluded.checked_at
            """,
            (
                job_id,
                user_name,
                latest_event_id,
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        self._commit()

    # -------------------------------------------------------------------------
    # LETTURA DATI
    # -------------------------------------------------------------------------
    def fetch_jobs(self) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                j.*,
                m.*,
                c.scan_json,
                c.permits_display,
                c.cartesio_prg_display,
                c.rilievi_dl_display,
                c.cartesio_cos_display,
                c.revisions_match,
                c.scanned_at,
                c.scan_version
            FROM jobs j
            LEFT JOIN job_meta m ON m.job_id = j.id
            LEFT JOIN job_scan_cache c ON c.job_id = j.id
            ORDER BY j.updated_at DESC, j.id DESC
            """
        )
        rows = [dict(row) for row in cur.fetchall()]
        job_ids = [int(row["id"]) for row in rows]
        overrides_map = self._get_scan_overrides_map(job_ids)

        for row in rows:
            self._decode_json_fields(row)
            row["scan_overrides"] = overrides_map.get(int(row["id"]), {})

        return rows

    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                j.*,
                m.*,
                c.scan_json,
                c.permits_display,
                c.cartesio_prg_display,
                c.rilievi_dl_display,
                c.cartesio_cos_display,
                c.revisions_match,
                c.scanned_at,
                c.scan_version
            FROM jobs j
            LEFT JOIN job_meta m ON m.job_id = j.id
            LEFT JOIN job_scan_cache c ON c.job_id = j.id
            WHERE j.id = ?
            """,
            (job_id,),
        )
        row = cur.fetchone()
        if not row:
            return None

        data = dict(row)
        self._decode_json_fields(data)
        data["scan_overrides"] = self._get_scan_overrides_map([job_id]).get(job_id, {})
        return data

    def _get_scan_overrides_map(self, job_ids: List[int]) -> Dict[int, Dict[str, str]]:
        if not job_ids:
            return {}

        placeholders = ",".join("?" for _ in job_ids)
        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT job_id, field_key, override_value
            FROM job_scan_overrides
            WHERE job_id IN ({placeholders})
            """,
            job_ids,
        )

        result: Dict[int, Dict[str, str]] = {}
        for row in cur.fetchall():
            job_id = int(row["job_id"])
            result.setdefault(job_id, {})[str(row["field_key"])] = str(row["override_value"] or "")
        return result

    # -------------------------------------------------------------------------
    # DUPLICATI PATH
    # -------------------------------------------------------------------------

    def _find_job_id_by_path(
        self,
        field_name: str,
        path: str,
        exclude_job_id: int | None = None,
    ) -> Optional[int]:
        """
        Cerca un job esistente confrontando il path in modo normalizzato.
        field_name deve essere 'project_base_path' oppure 'dl_base_path'.
        """
        if field_name not in {"project_base_path", "dl_base_path"}:
            raise ValueError(f"Campo non supportato per ricerca duplicati: {field_name}")

        target_key = path_compare_key(path)
        if not target_key:
            return None

        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT id, {field_name}
            FROM jobs
            WHERE TRIM(COALESCE({field_name}, '')) <> ''
            """
        )

        for row in cur.fetchall():
            row_id = int(row["id"])
            if exclude_job_id is not None and row_id == exclude_job_id:
                continue

            existing_path = row[field_name]
            if path_compare_key(existing_path) == target_key:
                return row_id

        return None

    def exists_project_path(self, path: str, exclude_job_id: int | None = None) -> bool:
        return self._find_job_id_by_path("project_base_path", path, exclude_job_id) is not None

    def exists_dl_path(self, path: str, exclude_job_id: int | None = None) -> bool:
        return self._find_job_id_by_path("dl_base_path", path, exclude_job_id) is not None

    def _validate_unique_paths(self, payload: Dict[str, Any], exclude_job_id: int | None = None) -> None:
        project_path = payload.get("project_base_path", "")
        dl_path = payload.get("dl_base_path", "")

        existing_project_id = self._find_job_id_by_path(
            "project_base_path", project_path, exclude_job_id
        )
        if existing_project_id is not None:
            raise ValueError(
                f"Esiste già un lavoro con questo Path Base Progetto:\n{project_path}\n"
                f"(ID esistente: {existing_project_id})"
            )

        existing_dl_id = self._find_job_id_by_path(
            "dl_base_path", dl_path, exclude_job_id
        )
        if existing_dl_id is not None:
            raise ValueError(
                f"Esiste già un lavoro con questo Path Base DL:\n{dl_path}\n"
                f"(ID esistente: {existing_dl_id})"
            )

    # -------------------------------------------------------------------------
    # CRUD JOB
    # -------------------------------------------------------------------------

    def add_job(self, payload: Dict[str, Any]) -> int:
        self._validate_unique_paths(payload)
        logging.info("Inserimento nuovo job")

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (
                project_base_path, dl_base_path, project_distretto_anno, project_name,
                project_mode, dl_distretto_anno, dl_name, dl_insert_date, general_notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.get("project_base_path", ""),
                payload.get("dl_base_path", ""),
                payload.get("project_distretto_anno", ""),
                payload.get("project_name", ""),
                payload.get("project_mode", "GTN"),
                payload.get("dl_distretto_anno", ""),
                payload.get("dl_name", ""),
                payload.get("dl_insert_date", ""),
                payload.get("general_notes", ""),
            ),
        )
        job_id = int(cur.lastrowid)

        cur.execute(
            """
            INSERT OR REPLACE INTO job_meta (
                job_id, permits_mode, permits_checklist_json, permits_notes,
                cartesio_prg_status, cartesio_prg_notes, cartesio_prg_manual_code,
                rilievi_dl_status, rilievi_dl_notes,
                cartesio_cos_status, cartesio_cos_notes, cartesio_cos_manual_code,
                psc_path, psc_status,
                todo_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                payload.get("permits_mode", "REQUIRED"),
                json.dumps(payload.get("permits_checklist_json") or [], ensure_ascii=False),
                payload.get("permits_notes", ""),
                payload.get("cartesio_prg_status", "NON IMPOSTATO"),
                payload.get("cartesio_prg_notes", ""),
                payload.get("cartesio_prg_manual_code", ""),
                payload.get("rilievi_dl_status", "NON IMPOSTATO"),
                payload.get("rilievi_dl_notes", ""),
                payload.get("cartesio_cos_status", "NON IMPOSTATO"),
                payload.get("cartesio_cos_notes", ""),
                payload.get("cartesio_cos_manual_code", ""),
                payload.get("psc_path", ""),
                payload.get("psc_status", "NOT_SET"),
                json.dumps(payload.get("todo_json") or [], ensure_ascii=False),
            ),
        )

        self.save_scan_cache(
            job_id=job_id,
            scan_data={},
            permits_display="❌",
            cartesio_prg_display="❌",
            rilievi_dl_display="❌",
            cartesio_cos_display="❌",
            revisions_match="UNKNOWN",
            commit=False,
            audit_enabled=False,
        )

        created_row = self.get_job(job_id) or {}
        changes: List[Dict[str, str]] = []
        snapshot_change = self._build_audit_change(
            field_scope="jobs",
            field_key="__created__",
            old_value=None,
            new_value=created_row,
        )
        if snapshot_change:
            changes.append(snapshot_change)

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="CREATE",
            source_kind="manual",
            origin_method="add_job",
            summary="Creato nuovo lavoro",
            context={"job_id": job_id},
        )
        self._insert_audit_changes(cur, event_id, changes)

        self._commit()
        return job_id

    def update_job(self, job_id: int, payload: Dict[str, Any]) -> None:
        self._validate_unique_paths(payload, exclude_job_id=job_id)
        logging.info("Aggiornamento job %s", job_id)

        before_row = self.get_job(job_id)
        if not before_row:
            raise ValueError(f"Job non trovato: {job_id}")

        jobs_fields = [
            "project_base_path",
            "dl_base_path",
            "project_distretto_anno",
            "project_name",
            "project_mode",
            "dl_distretto_anno",
            "dl_name",
            "dl_insert_date",
            "general_notes",
        ]
        meta_fields = [
            "permits_mode",
            "permits_checklist_json",
            "permits_notes",
            "cartesio_prg_status",
            "cartesio_prg_notes",
            "cartesio_prg_manual_code",
            "rilievi_dl_status",
            "rilievi_dl_notes",
            "cartesio_cos_status",
            "cartesio_cos_notes",
            "cartesio_cos_manual_code",
            "psc_path",
            "psc_status",
            "todo_json",
        ]

        new_jobs_values = {field: payload.get(field, "") for field in jobs_fields}
        new_meta_values = {
            "permits_mode": payload.get("permits_mode", "REQUIRED"),
            "permits_checklist_json": payload.get("permits_checklist_json") or [],
            "permits_notes": payload.get("permits_notes", ""),
            "cartesio_prg_status": payload.get("cartesio_prg_status", "NON IMPOSTATO"),
            "cartesio_prg_notes": payload.get("cartesio_prg_notes", ""),
            "cartesio_prg_manual_code": payload.get("cartesio_prg_manual_code", ""),
            "rilievi_dl_status": payload.get("rilievi_dl_status", "NON IMPOSTATO"),
            "rilievi_dl_notes": payload.get("rilievi_dl_notes", ""),
            "cartesio_cos_status": payload.get("cartesio_cos_status", "NON IMPOSTATO"),
            "cartesio_cos_notes": payload.get("cartesio_cos_notes", ""),
            "cartesio_cos_manual_code": payload.get("cartesio_cos_manual_code", ""),
            "psc_path": payload.get("psc_path", ""),
            "psc_status": payload.get("psc_status", "NOT_SET"),
            "todo_json": payload.get("todo_json") or [],
        }

        changes = self._collect_field_changes("jobs", before_row, new_jobs_values, jobs_fields)
        changes.extend(self._collect_field_changes("job_meta", before_row, new_meta_values, meta_fields))

        if not changes:
            logging.info("update_job ignorato: nessuna differenza per job %s", job_id)
            return

        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE jobs SET
                project_base_path=?, dl_base_path=?, project_distretto_anno=?, project_name=?,
                project_mode=?, dl_distretto_anno=?, dl_name=?, dl_insert_date=?, general_notes=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            (
                payload.get("project_base_path", ""),
                payload.get("dl_base_path", ""),
                payload.get("project_distretto_anno", ""),
                payload.get("project_name", ""),
                payload.get("project_mode", "GTN"),
                payload.get("dl_distretto_anno", ""),
                payload.get("dl_name", ""),
                payload.get("dl_insert_date", ""),
                payload.get("general_notes", ""),
                job_id,
            ),
        )

        cur.execute(
            """
            INSERT OR REPLACE INTO job_meta (
                job_id, permits_mode, permits_checklist_json, permits_notes,
                cartesio_prg_status, cartesio_prg_notes, cartesio_prg_manual_code,
                rilievi_dl_status, rilievi_dl_notes,
                cartesio_cos_status, cartesio_cos_notes, cartesio_cos_manual_code,
                psc_path, psc_status,
                todo_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                payload.get("permits_mode", "REQUIRED"),
                json.dumps(payload.get("permits_checklist_json") or [], ensure_ascii=False),
                payload.get("permits_notes", ""),
                payload.get("cartesio_prg_status", "NON IMPOSTATO"),
                payload.get("cartesio_prg_notes", ""),
                payload.get("cartesio_prg_manual_code", ""),
                payload.get("rilievi_dl_status", "NON IMPOSTATO"),
                payload.get("rilievi_dl_notes", ""),
                payload.get("cartesio_cos_status", "NON IMPOSTATO"),
                payload.get("cartesio_cos_notes", ""),
                payload.get("cartesio_cos_manual_code", ""),
                payload.get("psc_path", ""),
                payload.get("psc_status", "NOT_SET"),
                json.dumps(payload.get("todo_json") or [], ensure_ascii=False),
            ),
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="UPDATE_BASE",
            source_kind="manual",
            origin_method="update_job",
            summary="Modifica dati lavoro",
            context={"job_id": job_id},
        )
        self._insert_audit_changes(cur, event_id, changes)

        self._commit()

    def delete_job(self, job_id: int) -> None:
        before_row = self.get_job(job_id)
        if not before_row:
            return

        cur = self.conn.cursor()

        snapshot_change = self._build_audit_change(
            field_scope="jobs",
            field_key="__deleted__",
            old_value=before_row,
            new_value=None,
        )
        changes = [snapshot_change] if snapshot_change else []

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="DELETE",
            source_kind="manual",
            origin_method="delete_job",
            summary="Eliminato lavoro",
            context={"job_id": job_id},
        )
        self._insert_audit_changes(cur, event_id, changes)

        cur.execute("DELETE FROM job_audit_user_state WHERE job_id = ?", (job_id,))
        cur.execute("DELETE FROM job_scan_overrides WHERE job_id = ?", (job_id,))
        cur.execute("DELETE FROM job_meta WHERE job_id = ?", (job_id,))
        cur.execute("DELETE FROM job_scan_cache WHERE job_id = ?", (job_id,))
        cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))

        self._commit()

    def update_meta_fields(self, job_id: int, **fields: Any) -> None:
        """
        Aggiorna solo alcuni campi di job_meta, senza toccare i campi base del job
        e senza passare da update_job().
        """
        if not fields:
            return

        allowed_fields = {
            "permits_mode",
            "permits_checklist_json",
            "permits_notes",
            "cartesio_prg_status",
            "cartesio_prg_notes",
            "cartesio_prg_manual_code",
            "rilievi_dl_status",
            "rilievi_dl_notes",
            "cartesio_cos_status",
            "cartesio_cos_notes",
            "cartesio_cos_manual_code",
            "psc_path",
            "psc_status",
            "todo_json",
        }
        json_fields = {"permits_checklist_json", "todo_json"}

        unknown = set(fields) - allowed_fields
        if unknown:
            raise ValueError(f"Campi meta non supportati: {sorted(unknown)}")

        before_row = self.get_job(job_id)
        if not before_row:
            logging.warning("update_meta_fields ignorato: job_id=%s non trovato", job_id)
            return

        normalized_new_values: Dict[str, Any] = {}
        for field_name, value in fields.items():
            if field_name in json_fields:
                normalized_new_values[field_name] = value or []
            else:
                normalized_new_values[field_name] = "" if value is None else value

        changes = self._collect_field_changes(
            field_scope="job_meta",
            old_data=before_row,
            new_data=normalized_new_values,
            field_names=list(fields.keys()),
        )
        if not changes:
            logging.info("update_meta_fields ignorato: nessuna differenza per job %s", job_id)
            return

        cur = self.conn.cursor()
        cur.execute("INSERT OR IGNORE INTO job_meta (job_id) VALUES (?)", (job_id,))

        assignments = []
        params = []

        for field_name, value in fields.items():
            assignments.append(f"{field_name} = ?")
            if field_name in json_fields:
                params.append(json.dumps(value or [], ensure_ascii=False))
            else:
                params.append("" if value is None else value)

        params.append(job_id)

        sql = f"""
            UPDATE job_meta
            SET {", ".join(assignments)}
            WHERE job_id = ?
        """
        cur.execute(sql, params)

        cur.execute(
            "UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="update_meta_fields",
            summary="Modifica metadati lavoro",
            context={"updated_fields": sorted(fields.keys())},
        )
        self._insert_audit_changes(cur, event_id, changes)

        self._commit()

    # -------------------------------------------------------------------------
    # OVERRIDE SCAN MANUALI
    # -------------------------------------------------------------------------

    def _validate_scan_override_field(self, field_key: str) -> None:
        if field_key not in self.SCAN_OVERRIDEABLE_FIELDS:
            raise ValueError(f"Campo override non supportato: {field_key}")

    def set_scan_override(self, job_id: int, field_key: str, override_value: str) -> None:
        self._validate_scan_override_field(field_key)

        value = (override_value or "").strip()
        if not value:
            raise ValueError("Il valore manuale non può essere vuoto. Usa il ripristino per tornare all'automatico.")

        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM jobs WHERE id = ?", (job_id,))
        if cur.fetchone() is None:
            raise ValueError(f"Job non trovato: {job_id}")

        cur.execute(
            """
            SELECT override_value
            FROM job_scan_overrides
            WHERE job_id = ? AND field_key = ?
            """,
            (job_id, field_key),
        )
        row = cur.fetchone()
        old_value = str(row["override_value"] or "").strip() if row else ""

        change = self._build_audit_change(
            field_scope="job_scan_overrides",
            field_key=field_key,
            old_value=old_value,
            new_value=value,
        )
        if not change:
            logging.info("set_scan_override ignorato: nessuna differenza per job %s campo %s", job_id, field_key)
            return

        cur.execute(
            """
            INSERT INTO job_scan_overrides (job_id, field_key, override_value, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(job_id, field_key) DO UPDATE SET
                override_value = excluded.override_value,
                updated_at = CURRENT_TIMESTAMP
            """,
            (job_id, field_key, value),
        )

        cur.execute(
            "UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="OVERRIDE_SET",
            source_kind="override",
            origin_method="set_scan_override",
            summary=f"Override manuale impostato: {field_key}",
            context={"field_key": field_key},
        )
        self._insert_audit_changes(cur, event_id, [change])

        self._commit()

    def clear_scan_override(self, job_id: int, field_key: str) -> None:
        self._validate_scan_override_field(field_key)

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT override_value
            FROM job_scan_overrides
            WHERE job_id = ? AND field_key = ?
            """,
            (job_id, field_key),
        )
        row = cur.fetchone()
        if row is None:
            logging.info("clear_scan_override ignorato: nessun override per job %s campo %s", job_id, field_key)
            return

        old_value = str(row["override_value"] or "").strip()
        change = self._build_audit_change(
            field_scope="job_scan_overrides",
            field_key=field_key,
            old_value=old_value,
            new_value="",
        )
        if not change:
            return

        cur.execute(
            "DELETE FROM job_scan_overrides WHERE job_id = ? AND field_key = ?",
            (job_id, field_key),
        )
        cur.execute(
            "UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="OVERRIDE_CLEAR",
            source_kind="override",
            origin_method="clear_scan_override",
            summary=f"Override manuale rimosso: {field_key}",
            context={"field_key": field_key},
        )
        self._insert_audit_changes(cur, event_id, [change])

        self._commit()

    # -------------------------------------------------------------------------
    # CACHE SCANSIONE
    # -------------------------------------------------------------------------

    def save_scan_cache(
        self,
        job_id: int,
        scan_data: Dict[str, Any],
        permits_display: str,
        cartesio_prg_display: str,
        rilievi_dl_display: str,
        cartesio_cos_display: str,
        revisions_match: str,
        commit: bool = True,
        audit_enabled: bool = True,
    ) -> None:
        before_row = self.get_job(job_id) or {}

        changes = self._build_scan_cache_changes(
            before_row=before_row,
            scan_data=scan_data,
            permits_display=permits_display,
            cartesio_prg_display=cartesio_prg_display,
            rilievi_dl_display=rilievi_dl_display,
            cartesio_cos_display=cartesio_cos_display,
            revisions_match=revisions_match,
        )

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO job_scan_cache (
                job_id,
                scan_json,
                permits_display,
                cartesio_prg_display,
                rilievi_dl_display,
                cartesio_cos_display,
                revisions_match,
                scanned_at,
                scan_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                json.dumps(scan_data or {}, ensure_ascii=False),
                permits_display or "",
                cartesio_prg_display or "",
                rilievi_dl_display or "",
                cartesio_cos_display or "",
                revisions_match or "UNKNOWN",
                datetime.now().isoformat(timespec="seconds"),
                self.SCAN_CACHE_VERSION,
            ),
        )

        if changes:
            cur.execute(
                "UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (job_id,),
            )

            if audit_enabled:
                event_id = self._create_audit_event(
                    cur=cur,
                    job_id=job_id,
                    action_kind="SCAN",
                    source_kind="scan",
                    origin_method="save_scan_cache",
                    summary=f"Scansione automatica con {len(changes)} variazioni",
                    context={"job_id": job_id},
                )
                self._insert_audit_changes(cur, event_id, changes)

        if commit:
            self._commit()

    def delete_scan_cache(self, job_id: int, commit: bool = True) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM job_scan_cache WHERE job_id = ?", (job_id,))
        if commit:
            self._commit()

    def get_job_last_seen_event_id(self, job_id: int, user_name: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT last_seen_event_id
            FROM job_audit_user_state
            WHERE job_id = ? AND user_name = ?
            """,
            (job_id, user_name),
        )
        row = cur.fetchone()
        return int(row["last_seen_event_id"] or 0) if row else 0

    # -------------------------------------------------------------------------
    # APP STATE / LOCK GIORNALIERO
    # -------------------------------------------------------------------------

    def get_app_state(self, key: str, default: str = "") -> str:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM app_state WHERE key = ?", (key,))
        row = cur.fetchone()
        if not row:
            return default
        return str(row["value"] or "")

    def set_app_state(self, key: str, value: str, commit: bool = True) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO app_state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        if commit:
            self._commit()

    def get_last_global_scan_date(self) -> str:
        return self.get_app_state("last_global_scan_date", "")

    def set_last_global_scan_date_today(self, commit: bool = True) -> None:
        self.set_app_state("last_global_scan_date", date.today().isoformat(), commit=commit)

    def try_acquire_global_scan_lock(self, owner: str) -> bool:
        """
        Lock logico semplice per evitare doppio scan globale contemporaneo.
        Ritorna True se il lock viene acquisito da questa istanza.
        """
        cur = self.conn.cursor()

        cur.execute("BEGIN IMMEDIATE")

        cur.execute("SELECT value FROM app_state WHERE key = 'global_scan_lock'")
        row = cur.fetchone()
        current_value = str(row["value"] or "") if row else ""

        if current_value:
            self.conn.commit()
            return False

        lock_payload = json.dumps(
            {
                "owner": owner,
                "started_at": datetime.now().isoformat(timespec="seconds"),
            },
            ensure_ascii=False,
        )

        cur.execute(
            """
            INSERT INTO app_state (key, value)
            VALUES ('global_scan_lock', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (lock_payload,),
        )
        self.conn.commit()
        return True

    def release_global_scan_lock(self) -> None:
        self.set_app_state("global_scan_lock", "")

    # -------------------------------------------------------------------------
    # DECODIFICA CAMPI JSON
    # -------------------------------------------------------------------------

    def _decode_json_fields(self, row: Dict[str, Any]) -> None:
        for field in ("permits_checklist_json", "todo_json"):
            value = row.get(field)
            if value in (None, ""):
                row[field] = []
            elif isinstance(value, str):
                try:
                    row[field] = json.loads(value)
                except Exception:
                    logging.exception("JSON non valido in campo %s", field)
                    row[field] = []

        scan_value = row.get("scan_json")
        if scan_value in (None, ""):
            row["scan"] = {}
        elif isinstance(scan_value, str):
            try:
                row["scan"] = json.loads(scan_value)
            except Exception:
                logging.exception("JSON non valido in campo scan_json")
                row["scan"] = {}
        elif isinstance(scan_value, dict):
            row["scan"] = scan_value
        else:
            row["scan"] = {}

        row.pop("scan_json", None)

    def autofill_project_path_if_empty(
        self,
        job_id: int,
        project_base_path: str,
        project_distretto_anno: str,
        project_name: str,
    ) -> bool:
        """
        Autocompila i campi base PRG del job SOLO se project_base_path è ancora vuoto.
        """
        project_base_path = (project_base_path or "").strip()
        project_distretto_anno = (project_distretto_anno or "").strip()
        project_name = (project_name or "").strip()

        if not project_base_path:
            return False

        before_row = self.get_job(job_id)
        if not before_row:
            logging.warning("autofill_project_path_if_empty: job %s non trovato", job_id)
            return False

        existing_value = str(before_row.get("project_base_path") or "").strip()
        if existing_value:
            return False

        if self.exists_project_path(project_base_path, exclude_job_id=job_id):
            logging.warning(
                "Autocompilazione project_base_path ignorata per job %s: path già usato da altro job: %s",
                job_id,
                project_base_path,
            )
            return False

        new_values = {
            "project_base_path": project_base_path,
            "project_distretto_anno": project_distretto_anno,
            "project_name": project_name,
        }
        changes = self._collect_field_changes(
            field_scope="jobs",
            old_data=before_row,
            new_data=new_values,
            field_names=["project_base_path", "project_distretto_anno", "project_name"],
        )
        if not changes:
            return False

        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE jobs
            SET
                project_base_path = ?,
                project_distretto_anno = ?,
                project_name = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                project_base_path,
                project_distretto_anno,
                project_name,
                job_id,
            ),
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="AUTOFILL",
            source_kind="autofill",
            origin_method="autofill_project_path_if_empty",
            summary="Autocompilato percorso progetto da link DL",
            context={"project_base_path": project_base_path},
        )
        self._insert_audit_changes(cur, event_id, changes)

        self._commit()
        return True

    def autofill_psc_path_if_empty(
        self,
        job_id: int,
        psc_path: str,
    ) -> bool:
        """
        Autocompila i campi PSC del job SOLO se psc_path è ancora vuoto.
        """
        psc_path = (psc_path or "").strip()
        if not psc_path:
            return False

        before_row = self.get_job(job_id)
        if not before_row:
            logging.warning("autofill_psc_path_if_empty: job %s non trovato", job_id)
            return False

        existing_value = str(before_row.get("psc_path") or "").strip()
        if existing_value:
            return False

        new_values = {
            "psc_path": psc_path,
            "psc_status": "READY",
        }
        changes = self._collect_field_changes(
            field_scope="job_meta",
            old_data=before_row,
            new_data=new_values,
            field_names=["psc_path", "psc_status"],
        )
        if not changes:
            return False

        cur = self.conn.cursor()

        cur.execute("INSERT OR IGNORE INTO job_meta (job_id) VALUES (?)", (job_id,))
        cur.execute(
            """
            UPDATE job_meta
            SET
                psc_path = ?,
                psc_status = 'READY'
            WHERE job_id = ?
            """,
            (
                psc_path,
                job_id,
            ),
        )
        cur.execute(
            "UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (job_id,),
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="AUTOFILL",
            source_kind="autofill",
            origin_method="autofill_psc_path_if_empty",
            summary="Autocompilato percorso PSC da link DL",
            context={"psc_path": psc_path},
        )
        self._insert_audit_changes(cur, event_id, changes)

        self._commit()
        return True