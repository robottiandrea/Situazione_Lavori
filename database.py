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
    SCHEMA_VERSION = 2
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
                cartesio_delivery_scope TEXT NOT NULL DEFAULT 'NONE',
                permits_checklist_json TEXT,
                permits_notes TEXT,
                cartesio_prg_status TEXT,
                cartesio_prg_notes TEXT,
                rilievi_dl_status TEXT,
                rilievi_dl_notes TEXT,
                cartesio_cos_status TEXT,
                cartesio_cos_notes TEXT,
                project_tracciamento_manual_path TEXT,
                psc_path TEXT,
                psc_status TEXT DEFAULT 'NOT_SET',
                todo_json TEXT,
                exception_mode TEXT NOT NULL DEFAULT 'STANDARD',
                exception_reason TEXT,
                exception_group_code TEXT,
                manual_project_control_path TEXT,
                manual_dl_control_path TEXT,
                manual_cartesio_prg_code TEXT,
                manual_cartesio_prg_path TEXT,
                manual_cartesio_cos_code TEXT,
                manual_cartesio_cos_path TEXT,
                manual_cartesio_acc_code TEXT,
                manual_cartesio_acc_path TEXT,
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_cartesio_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                scope TEXT NOT NULL,
                referente TEXT,
                status TEXT NOT NULL DEFAULT 'NON IMPOSTATO',
                is_active INTEGER NOT NULL DEFAULT 0,
                checklist_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                last_activity_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(job_id, scope),
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_cartesio_threads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'APERTO',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                closed_at TEXT,
                FOREIGN KEY(entry_id) REFERENCES job_cartesio_entries(id) ON DELETE CASCADE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_cartesio_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id INTEGER NOT NULL,
                thread_id INTEGER,
                title TEXT NOT NULL,
                body TEXT,
                checklist_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(entry_id) REFERENCES job_cartesio_entries(id) ON DELETE CASCADE,
                FOREIGN KEY(thread_id) REFERENCES job_cartesio_threads(id) ON DELETE SET NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_cartesio_note_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                note_id INTEGER NOT NULL,
                attachment_kind TEXT NOT NULL,
                stored_rel_path TEXT NOT NULL,
                display_name TEXT,
                subject TEXT,
                sender TEXT,
                received_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                meta_json TEXT,
                FOREIGN KEY(note_id) REFERENCES job_cartesio_notes(id) ON DELETE CASCADE
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
            CREATE INDEX IF NOT EXISTS idx_cartesio_entries_job_scope
            ON job_cartesio_entries(job_id, scope)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cartesio_entries_scope_active
            ON job_cartesio_entries(scope, is_active, last_activity_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cartesio_threads_entry
            ON job_cartesio_threads(entry_id, status, updated_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cartesio_notes_entry
            ON job_cartesio_notes(entry_id, updated_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cartesio_notes_thread
            ON job_cartesio_notes(thread_id, updated_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cartesio_attachments_note
            ON job_cartesio_note_attachments(note_id, created_at DESC)
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
        Migrazioni schema per database già esistenti.

        Strategia:
        - prima applica aggiunte leggere/idempotenti;
        - poi esegue la migrazione versionata v2 se necessaria;
        - infine riesegue il controllo colonne su job_meta, così le nuove colonne
          eccezione/manuali restano presenti anche su DB molto vecchi.
        """
        cur = self.conn.cursor()

        cur.execute("PRAGMA table_info(jobs)")
        jobs_columns = {str(row["name"]) for row in cur.fetchall()}

        if "project_mode" not in jobs_columns:
            logging.info("Aggiunta colonna jobs.project_mode")
            cur.execute("ALTER TABLE jobs ADD COLUMN project_mode TEXT NOT NULL DEFAULT 'GTN'")

        def ensure_job_meta_columns() -> None:
            cur.execute("PRAGMA table_info(job_meta)")
            meta_columns = {str(row["name"]) for row in cur.fetchall()}

            required_meta_columns = {
                "permits_mode": "TEXT NOT NULL DEFAULT 'REQUIRED'",
                "cartesio_delivery_scope": "TEXT NOT NULL DEFAULT 'NONE'",
                "project_tracciamento_manual_path": "TEXT",
                "psc_path": "TEXT",
                "psc_status": "TEXT DEFAULT 'NOT_SET'",
                "exception_mode": "TEXT NOT NULL DEFAULT 'STANDARD'",
                "exception_reason": "TEXT",
                "exception_group_code": "TEXT",
                "manual_project_control_path": "TEXT",
                "manual_dl_control_path": "TEXT",
                "manual_cartesio_prg_code": "TEXT",
                "manual_cartesio_prg_path": "TEXT",
                "manual_cartesio_cos_code": "TEXT",
                "manual_cartesio_cos_path": "TEXT",
                "manual_cartesio_acc_code": "TEXT",
                "manual_cartesio_acc_path": "TEXT",
            }

            for column_name, column_sql in required_meta_columns.items():
                if column_name not in meta_columns:
                    logging.info("Aggiunta colonna job_meta.%s", column_name)
                    cur.execute(f"ALTER TABLE job_meta ADD COLUMN {column_name} {column_sql}")

        ensure_job_meta_columns()

        cur.execute("PRAGMA table_info(job_cartesio_entries)")
        cartesio_entry_columns = {str(row["name"]) for row in cur.fetchall()}

        if "checklist_json" not in cartesio_entry_columns:
            logging.info("Aggiunta colonna job_cartesio_entries.checklist_json")
            cur.execute("ALTER TABLE job_cartesio_entries ADD COLUMN checklist_json TEXT")

        current_version_raw = self.get_app_state("db_schema_version", "1").strip()
        try:
            current_version = int(current_version_raw or "1")
        except ValueError:
            current_version = 1

        if current_version < 2:
            self._migrate_schema_v2()
            self.set_app_state("db_schema_version", str(self.SCHEMA_VERSION), commit=False)

        ensure_job_meta_columns()

        if int(self.get_app_state("db_schema_version", "0") or "0") < self.SCHEMA_VERSION:
            self.set_app_state("db_schema_version", str(self.SCHEMA_VERSION), commit=False)

        self._normalize_legacy_cartesio_status_labels()
        self._backfill_cartesio_from_legacy()


    def _table_columns(self, table_name: str) -> set[str]:
        cur = self.conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name})")
        return {str(row["name"]) for row in cur.fetchall()}


    def _migrate_schema_v2(self) -> None:
        """
        Migrazione strutturale:

        - job_meta: rimuove colonne obsolete cartesio_*_manual_code
        - job_cartesio_entries: rimuove colonna obsolete manual_code
        - job_audit_events: aggiunge FK verso jobs(id) ON DELETE CASCADE
        - job_audit_user_state: aggiunge FK verso jobs(id) ON DELETE CASCADE

        Nota:
        gli audit orfani vengono scartati in copia.
        """
        logging.info("Avvio migrazione schema v2")

        cur = self.conn.cursor()
        cur.execute("PRAGMA foreign_keys = OFF")

        try:
            meta_columns = self._table_columns("job_meta")
            entry_columns = self._table_columns("job_cartesio_entries")
            audit_event_columns = self._table_columns("job_audit_events")
            audit_user_state_columns = self._table_columns("job_audit_user_state")

            # ------------------------------------------------------------------
            # job_meta
            # ------------------------------------------------------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS job_meta_new (
                    job_id INTEGER PRIMARY KEY,
                    permits_mode TEXT NOT NULL DEFAULT 'REQUIRED',
                    cartesio_delivery_scope TEXT NOT NULL DEFAULT 'NONE',
                    permits_checklist_json TEXT,
                    permits_notes TEXT,
                    cartesio_prg_status TEXT,
                    cartesio_prg_notes TEXT,
                    rilievi_dl_status TEXT,
                    rilievi_dl_notes TEXT,
                    cartesio_cos_status TEXT,
                    cartesio_cos_notes TEXT,
                    project_tracciamento_manual_path TEXT,
                    psc_path TEXT,
                    psc_status TEXT DEFAULT 'NOT_SET',
                    todo_json TEXT,
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
                """
            )

            def meta_expr(column_name: str, default_sql: str) -> str:
                return column_name if column_name in meta_columns else default_sql

            cur.execute(
                f"""
                INSERT INTO job_meta_new (
                    job_id,
                    permits_mode,
                    cartesio_delivery_scope,
                    permits_checklist_json,
                    permits_notes,
                    cartesio_prg_status,
                    cartesio_prg_notes,
                    rilievi_dl_status,
                    rilievi_dl_notes,
                    cartesio_cos_status,
                    cartesio_cos_notes,
                    project_tracciamento_manual_path,
                    psc_path,
                    psc_status,
                    todo_json
                )
                SELECT
                    job_id,
                    COALESCE({meta_expr("permits_mode", "'REQUIRED'")}, 'REQUIRED'),
                    COALESCE({meta_expr("cartesio_delivery_scope", "'NONE'")}, 'NONE'),
                    {meta_expr("permits_checklist_json", "NULL")},
                    {meta_expr("permits_notes", "NULL")},
                    {meta_expr("cartesio_prg_status", "NULL")},
                    {meta_expr("cartesio_prg_notes", "NULL")},
                    {meta_expr("rilievi_dl_status", "NULL")},
                    {meta_expr("rilievi_dl_notes", "NULL")},
                    {meta_expr("cartesio_cos_status", "NULL")},
                    {meta_expr("cartesio_cos_notes", "NULL")},
                    {meta_expr("project_tracciamento_manual_path", "NULL")},
                    {meta_expr("psc_path", "NULL")},
                    COALESCE({meta_expr("psc_status", "'NOT_SET'")}, 'NOT_SET'),
                    {meta_expr("todo_json", "NULL")}
                FROM job_meta
                """
            )

            cur.execute("DROP TABLE job_meta")
            cur.execute("ALTER TABLE job_meta_new RENAME TO job_meta")

            # ------------------------------------------------------------------
            # job_cartesio_entries
            # ------------------------------------------------------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS job_cartesio_entries_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    scope TEXT NOT NULL,
                    referente TEXT,
                    status TEXT NOT NULL DEFAULT 'NON IMPOSTATO',
                    is_active INTEGER NOT NULL DEFAULT 0,
                    checklist_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_activity_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(job_id, scope),
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
                """
            )

            def entry_expr(column_name: str, default_sql: str) -> str:
                return column_name if column_name in entry_columns else default_sql

            cur.execute(
                f"""
                INSERT INTO job_cartesio_entries_new (
                    id,
                    job_id,
                    scope,
                    referente,
                    status,
                    is_active,
                    checklist_json,
                    created_at,
                    updated_at,
                    last_activity_at
                )
                SELECT
                    id,
                    job_id,
                    scope,
                    {entry_expr("referente", "NULL")},
                    COALESCE({entry_expr("status", "'NON IMPOSTATO'")}, 'NON IMPOSTATO'),
                    COALESCE({entry_expr("is_active", "0")}, 0),
                    {entry_expr("checklist_json", "NULL")},
                    {entry_expr("created_at", "CURRENT_TIMESTAMP")},
                    {entry_expr("updated_at", "CURRENT_TIMESTAMP")},
                    {entry_expr("last_activity_at", "CURRENT_TIMESTAMP")}
                FROM job_cartesio_entries
                """
            )

            cur.execute("DROP TABLE job_cartesio_entries")
            cur.execute("ALTER TABLE job_cartesio_entries_new RENAME TO job_cartesio_entries")

            # ------------------------------------------------------------------
            # job_audit_events
            # ------------------------------------------------------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS job_audit_events_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    event_ts TEXT NOT NULL,
                    action_kind TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    initiated_by TEXT,
                    machine_name TEXT,
                    origin_method TEXT,
                    summary TEXT,
                    context_json TEXT,
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
                """
            )

            if audit_event_columns:
                cur.execute(
                    """
                    INSERT INTO job_audit_events_new (
                        id,
                        job_id,
                        event_ts,
                        action_kind,
                        source_kind,
                        initiated_by,
                        machine_name,
                        origin_method,
                        summary,
                        context_json
                    )
                    SELECT
                        e.id,
                        e.job_id,
                        e.event_ts,
                        e.action_kind,
                        e.source_kind,
                        e.initiated_by,
                        e.machine_name,
                        e.origin_method,
                        e.summary,
                        e.context_json
                    FROM job_audit_events e
                    WHERE EXISTS (
                        SELECT 1
                        FROM jobs j
                        WHERE j.id = e.job_id
                    )
                    """
                )

            cur.execute("DROP TABLE job_audit_events")
            cur.execute("ALTER TABLE job_audit_events_new RENAME TO job_audit_events")

            # ------------------------------------------------------------------
            # job_audit_user_state
            # ------------------------------------------------------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS job_audit_user_state_new (
                    job_id INTEGER NOT NULL,
                    user_name TEXT NOT NULL,
                    last_seen_event_id INTEGER,
                    checked_at TEXT,
                    PRIMARY KEY (job_id, user_name),
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
                """
            )

            if audit_user_state_columns:
                cur.execute(
                    """
                    INSERT INTO job_audit_user_state_new (
                        job_id,
                        user_name,
                        last_seen_event_id,
                        checked_at
                    )
                    SELECT
                        s.job_id,
                        s.user_name,
                        s.last_seen_event_id,
                        s.checked_at
                    FROM job_audit_user_state s
                    WHERE EXISTS (
                        SELECT 1
                        FROM jobs j
                        WHERE j.id = s.job_id
                    )
                    """
                )

            cur.execute("DROP TABLE job_audit_user_state")
            cur.execute("ALTER TABLE job_audit_user_state_new RENAME TO job_audit_user_state")

            # ------------------------------------------------------------------
            # Cleanup audit_changes orfani
            # ------------------------------------------------------------------
            cur.execute(
                """
                DELETE FROM job_audit_changes
                WHERE event_id NOT IN (
                    SELECT id
                    FROM job_audit_events
                )
                """
            )

            # ------------------------------------------------------------------
            # Ricrea indici persi nel rebuild
            # ------------------------------------------------------------------
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cartesio_entries_job_scope
                ON job_cartesio_entries(job_id, scope)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cartesio_entries_scope_active
                ON job_cartesio_entries(scope, is_active, last_activity_at DESC)
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
                CREATE INDEX IF NOT EXISTS idx_job_audit_user_state_user
                ON job_audit_user_state(user_name, job_id)
                """
            )

        finally:
            cur.execute("PRAGMA foreign_keys = ON")



    def _normalize_cartesio_scope(self, value: Any) -> str:
        scope = str(value or "").strip().upper()
        if scope in {"PRG", "COS", "ACC", "NONE"}:
            return scope
        return "NONE"

    def _normalize_cartesio_thread_status(self, value: Any) -> str:
        status = str(value or "").strip().upper()
        if status in {"APERTO", "CHIUSO"}:
            return status
        return "APERTO"

    def _normalize_cartesio_entry_status(self, value: Any) -> str:
        """
        Normalizza gli stati Cartesio lato entry.

        Note:
        - converte automaticamente il legacy 'IN CORSO' nel nuovo 'IN LAVORAZIONE'
        - usa un set unico volutamente permissivo tra PRG e COS
        """
        status = str(value or "").strip().upper()

        if status == "IN CORSO":
            status = "IN LAVORAZIONE"

        allowed = {
            "NON IMPOSTATO",
            "IN LAVORAZIONE",
            "INCIDENT",
            "VERIFICA M.E.",
            "APPROVATO M.E.",
            "VERIFICA TOTALE",
            "APPROVATO V.T.",
            "RESTITUITO",
        }

        if status in allowed:
            return status

        return "NON IMPOSTATO"

    def _has_open_incident_thread(self, cur: sqlite3.Cursor, entry_id: int) -> bool:
        """
        Verifica se esiste almeno un thread aperto chiamato esattamente INCIDENT
        per la entry Cartesio indicata.
        """
        cur.execute(
            """
            SELECT 1
            FROM job_cartesio_threads
            WHERE entry_id = ?
              AND status = 'APERTO'
              AND UPPER(TRIM(title)) = 'INCIDENT'
            LIMIT 1
            """,
            (entry_id,),
        )
        return cur.fetchone() is not None

    def _resolve_cartesio_entry_status(
        self,
        cur: sqlite3.Cursor,
        entry_id: int,
        fallback_status: Any,
    ) -> str:
        """
        Regola aggiornata:

        - se esiste almeno un thread APERTO chiamato 'INCIDENT',
          lo stato dell'entry deve essere 'INCIDENT'
        - negli altri casi si rispetta sempre lo stato scelto/manuale
          normalizzato dall'utente
        """
        normalized_fallback = self._normalize_cartesio_entry_status(fallback_status)

        if self._has_open_incident_thread(cur, entry_id):
            return "INCIDENT"

        return normalized_fallback

    def _normalize_legacy_cartesio_status_labels(self) -> None:
        """
        Migrazione soft e idempotente:
        rinomina tutti i vecchi valori 'IN CORSO' in 'IN LAVORAZIONE'
        sia nelle entry Cartesio sia nei campi legacy di job_meta.
        """
        cur = self.conn.cursor()

        cur.execute(
            """
            UPDATE job_cartesio_entries
            SET status = 'IN LAVORAZIONE'
            WHERE UPPER(TRIM(COALESCE(status, ''))) = 'IN CORSO'
            """
        )

        cur.execute(
            """
            UPDATE job_meta
            SET cartesio_prg_status = 'IN LAVORAZIONE'
            WHERE UPPER(TRIM(COALESCE(cartesio_prg_status, ''))) = 'IN CORSO'
            """
        )

        cur.execute(
            """
            UPDATE job_meta
            SET cartesio_cos_status = 'IN LAVORAZIONE'
            WHERE UPPER(TRIM(COALESCE(cartesio_cos_status, ''))) = 'IN CORSO'
            """
        )


    def _ensure_cartesio_entry(self, cur: sqlite3.Cursor, job_id: int, scope: str) -> int:
        scope = self._normalize_cartesio_scope(scope)
        if scope not in {"PRG", "COS", "ACC"}:
            raise ValueError(f"Scope Cartesio non valido: {scope}")

        cur.execute(
            """
            INSERT INTO job_cartesio_entries (job_id, scope)
            VALUES (?, ?)
            ON CONFLICT(job_id, scope) DO NOTHING
            """,
            (job_id, scope),
        )
        cur.execute(
            """
            SELECT id
            FROM job_cartesio_entries
            WHERE job_id = ? AND scope = ?
            """,
            (job_id, scope),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(
                f"Impossibile creare/reperire entry Cartesio per job {job_id} scope {scope}"
            )
        return int(row["id"])    

    def _touch_cartesio_entry(self, cur: sqlite3.Cursor, entry_id: int) -> None:
        now_ts = datetime.now().isoformat(timespec="seconds")
        cur.execute(
            """
            UPDATE job_cartesio_entries
            SET updated_at = ?, last_activity_at = ?
            WHERE id = ?
            """,
            (now_ts, now_ts, entry_id),
        )

    def _sync_legacy_cartesio_entry_fields(self, cur: sqlite3.Cursor, job_id: int, scope: str) -> None:
        scope = self._normalize_cartesio_scope(scope)
        if scope not in {"PRG", "COS"}:
            return

        cur.execute(
            """
            SELECT status
            FROM job_cartesio_entries
            WHERE job_id = ? AND scope = ?
            """,
            (job_id, scope),
        )
        row = cur.fetchone()
        if not row:
            return

        normalized_status = self._normalize_cartesio_entry_status(row["status"])

        if scope == "PRG":
            cur.execute(
                """
                INSERT OR IGNORE INTO job_meta (job_id) VALUES (?)
                """,
                (job_id,),
            )
            cur.execute(
                """
                UPDATE job_meta
                SET cartesio_prg_status = ?
                WHERE job_id = ?
                """,
                (normalized_status, job_id),
            )
        else:
            cur.execute(
                """
                INSERT OR IGNORE INTO job_meta (job_id) VALUES (?)
                """,
                (job_id,),
            )
            cur.execute(
                """
                UPDATE job_meta
                SET cartesio_cos_status = ?
                WHERE job_id = ?
                """,
                (normalized_status, job_id),
            )

    def _backfill_cartesio_from_legacy(self) -> None:
        done_key = "cartesio_legacy_migrated_v1"
        if self.get_app_state(done_key, "") == "1":
            return

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                j.id,
                COALESCE(m.cartesio_delivery_scope, 'NONE') AS cartesio_delivery_scope,
                COALESCE(m.cartesio_prg_status, 'NON IMPOSTATO') AS cartesio_prg_status,
                COALESCE(m.cartesio_prg_notes, '') AS cartesio_prg_notes,
                COALESCE(m.cartesio_cos_status, 'NON IMPOSTATO') AS cartesio_cos_status,
                COALESCE(m.cartesio_cos_notes, '') AS cartesio_cos_notes
            FROM jobs j
            LEFT JOIN job_meta m ON m.job_id = j.id
            """
        )
        rows = cur.fetchall()
        now_ts = datetime.now().isoformat(timespec="seconds")

        for row in rows:
            job_id = int(row["id"])
            present_scopes: List[str] = []

            for scope, status_key, notes_key in (
                ("PRG", "cartesio_prg_status", "cartesio_prg_notes"),
                ("COS", "cartesio_cos_status", "cartesio_cos_notes"),
            ):
                status_value = self._normalize_cartesio_entry_status(row[status_key])
                notes_value = str(row[notes_key] or "").strip()

                has_payload = (status_value != "NON IMPOSTATO") or bool(notes_value)
                if not has_payload:
                    continue

                entry_id = self._ensure_cartesio_entry(cur, job_id, scope)
                cur.execute(
                    """
                    UPDATE job_cartesio_entries
                    SET status = ?,
                        updated_at = ?,
                        last_activity_at = ?
                    WHERE id = ?
                    """,
                    (status_value, now_ts, now_ts, entry_id),
                )

                if notes_value:
                    cur.execute(
                        """
                        SELECT 1
                        FROM job_cartesio_notes
                        WHERE entry_id = ? AND title = ?
                        LIMIT 1
                        """,
                        (entry_id, "Nota legacy migrata"),
                    )
                    if cur.fetchone() is None:
                        cur.execute(
                            """
                            INSERT INTO job_cartesio_notes (
                                entry_id,
                                thread_id,
                                title,
                                body,
                                checklist_json,
                                created_at,
                                updated_at
                            ) VALUES (?, NULL, ?, ?, ?, ?, ?)
                            """,
                            (
                                entry_id,
                                "Nota legacy migrata",
                                notes_value,
                                json.dumps([], ensure_ascii=False),
                                now_ts,
                                now_ts,
                            ),
                        )
                present_scopes.append(scope)

            current_scope = self._normalize_cartesio_scope(row["cartesio_delivery_scope"])
            if current_scope == "NONE" and len(present_scopes) == 1:
                current_scope = present_scopes[0]

            cur.execute("INSERT OR IGNORE INTO job_meta (job_id) VALUES (?)", (job_id,))
            cur.execute(
                """
                UPDATE job_meta
                SET cartesio_delivery_scope = ?
                WHERE job_id = ?
                """,
                (current_scope, job_id),
            )
            cur.execute(
                """
                UPDATE job_cartesio_entries
                SET is_active = CASE WHEN scope = ? AND ? <> 'NONE' THEN 1 ELSE 0 END
                WHERE job_id = ?
                """,
                (current_scope, current_scope, job_id),
            )

        self.set_app_state(done_key, "1", commit=False)


    def _decode_cartesio_entry_row(self, row: Optional[sqlite3.Row | Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not row:
            return None

        entry = dict(row)
        checklist_value = entry.get("checklist_json")
        if isinstance(checklist_value, str) and checklist_value:
            try:
                entry["checklist_json"] = json.loads(checklist_value)
            except Exception:
                logging.exception("JSON non valido in job_cartesio_entries.checklist_json")
                entry["checklist_json"] = []
        elif isinstance(checklist_value, list):
            entry["checklist_json"] = checklist_value
        else:
            entry["checklist_json"] = []

        return entry

    def _normalize_cartesio_entry_checklist(self, checklist_json: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        def _as_bool(value: Any) -> bool:
            if isinstance(value, bool):
                return value
            if value is None:
                return False
            if isinstance(value, (int, float)):
                return value != 0
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "y", "on", "si", "sì"}
            return bool(value)

        normalized: List[Dict[str, Any]] = []

        for item in checklist_json or []:
            if not isinstance(item, dict):
                continue

            text_value = str(item.get("text") or "").strip()
            note_value = str(item.get("note") or "").strip()
            if not text_value:
                continue

            normalized.append(
                {
                    "text": text_value,
                    "done": _as_bool(item.get("done")),
                    "note": note_value,
                }
            )

        return normalized

    def get_cartesio_entry(self, job_id: int, scope: str) -> Optional[Dict[str, Any]]:
        scope = self._normalize_cartesio_scope(scope)
        if scope not in {"PRG", "COS", "ACC"}:
            return None

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT e.*, m.cartesio_delivery_scope
            FROM job_cartesio_entries e
            LEFT JOIN job_meta m ON m.job_id = e.job_id
            WHERE e.job_id = ? AND e.scope = ?
            """,
            (job_id, scope),
        )
        row = cur.fetchone()
        return self._decode_cartesio_entry_row(row)

    def get_cartesio_entry_by_id(self, entry_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT e.*, m.cartesio_delivery_scope
            FROM job_cartesio_entries e
            LEFT JOIN job_meta m ON m.job_id = e.job_id
            WHERE e.id = ?
            """,
            (entry_id,),
        )
        row = cur.fetchone()
        return self._decode_cartesio_entry_row(row)

    def get_cartesio_note(self, note_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT n.*, e.job_id, e.scope
            FROM job_cartesio_notes n
            INNER JOIN job_cartesio_entries e ON e.id = n.entry_id
            WHERE n.id = ?
            """,
            (note_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        note = dict(row)
        checklist_value = note.get("checklist_json")
        if isinstance(checklist_value, str) and checklist_value:
            try:
                note["checklist_json"] = json.loads(checklist_value)
            except Exception:
                note["checklist_json"] = []
        elif checklist_value in (None, ""):
            note["checklist_json"] = []
        return note

    def list_cartesio_threads(self, entry_id: int) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                t.*, 
                COALESCE((
                    SELECT COUNT(*)
                    FROM job_cartesio_notes n
                    WHERE n.thread_id = t.id
                ), 0) AS notes_count
            FROM job_cartesio_threads t
            WHERE t.entry_id = ?
            ORDER BY CASE WHEN t.status = 'APERTO' THEN 0 ELSE 1 END, t.updated_at DESC, t.id DESC
            """,
            (entry_id,),
        )
        return [dict(row) for row in cur.fetchall()]

    def list_cartesio_notes(self, entry_id: int) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                n.*,
                t.title AS thread_title,
                t.status AS thread_status
            FROM job_cartesio_notes n
            LEFT JOIN job_cartesio_threads t ON t.id = n.thread_id
            WHERE n.entry_id = ?
            ORDER BY n.updated_at DESC, n.id DESC
            """,
            (entry_id,),
        )
        items: List[Dict[str, Any]] = []
        for row in cur.fetchall():
            item = dict(row)
            checklist_value = item.get("checklist_json")
            if isinstance(checklist_value, str) and checklist_value:
                try:
                    item["checklist_json"] = json.loads(checklist_value)
                except Exception:
                    item["checklist_json"] = []
            elif checklist_value in (None, ""):
                item["checklist_json"] = []
            items.append(item)
        return items

    def list_cartesio_note_attachments(self, note_id: int) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM job_cartesio_note_attachments
            WHERE note_id = ?
            ORDER BY created_at DESC, id DESC
            """,
            (note_id,),
        )
        items: List[Dict[str, Any]] = []
        for row in cur.fetchall():
            item = dict(row)
            meta_value = item.get("meta_json")
            if isinstance(meta_value, str) and meta_value:
                try:
                    item["meta_json"] = json.loads(meta_value)
                except Exception:
                    item["meta_json"] = {}
            elif meta_value in (None, ""):
                item["meta_json"] = {}
            items.append(item)
        return items

    def get_cartesio_bundle(self, job_id: int, scope: str) -> Dict[str, Any]:
        job = self.get_job(job_id) or {}
        entry = self.get_cartesio_entry(job_id, scope)
        if not entry:
            return {
                "job": job,
                "entry": None,
                "threads": [],
                "notes": [],
            }

        threads = self.list_cartesio_threads(int(entry["id"]))
        notes = self.list_cartesio_notes(int(entry["id"]))
        for note in notes:
            note["attachments"] = self.list_cartesio_note_attachments(int(note["id"]))
        return {
            "job": job,
            "entry": entry,
            "threads": threads,
            "notes": notes,
        }


    def save_cartesio_entry_checklist(
        self,
        job_id: int,
        scope: str,
        checklist_json: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        scope = self._normalize_cartesio_scope(scope)
        if scope not in {"PRG", "COS", "ACC"}:
            raise ValueError(f"Scope Cartesio non valido: {scope}")

        normalized_checklist = self._normalize_cartesio_entry_checklist(checklist_json)
        before_entry = self.get_cartesio_entry(job_id, scope) or {}
        now_ts = datetime.now().isoformat(timespec="seconds")

        cur = self.conn.cursor()
        entry_id = self._ensure_cartesio_entry(cur, job_id, scope)

        cur.execute(
            """
            UPDATE job_cartesio_entries
            SET checklist_json = ?,
                updated_at = ?,
                last_activity_at = ?
            WHERE id = ?
            """,
            (
                json.dumps(normalized_checklist, ensure_ascii=False),
                now_ts,
                now_ts,
                entry_id,
            ),
        )

        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (job_id,))

        after_entry = self.get_cartesio_entry_by_id(entry_id) or {}
        changes = self._collect_field_changes(
            "job_cartesio_entries",
            before_entry,
            after_entry,
            ["checklist_json"],
        )

        if changes:
            completed_count = sum(1 for item in normalized_checklist if bool(item.get("done")))
            event_id = self._create_audit_event(
                cur=cur,
                job_id=job_id,
                action_kind="UPDATE_META",
                source_kind="manual",
                origin_method="save_cartesio_entry_checklist",
                summary=(
                    f"Aggiornata checklist Cartesio {scope} "
                    f"({completed_count}/{len(normalized_checklist)})"
                ),
                context={
                    "scope": scope,
                    "entry_id": entry_id,
                    "checklist_total": len(normalized_checklist),
                    "checklist_done": completed_count,
                },
            )
            self._insert_audit_changes(cur, event_id, changes)

        self._commit()
        return self.get_cartesio_bundle(job_id, scope)

    def save_cartesio_entry(
        self,
        job_id: int,
        scope: str,
        referente: str,
        status: str,
        is_active: bool,
    ) -> Dict[str, Any]:
        scope = self._normalize_cartesio_scope(scope)
        if scope not in {"PRG", "COS", "ACC"}:
            raise ValueError(f"Scope Cartesio non valido: {scope}")

        before_bundle = self.get_cartesio_bundle(job_id, scope)
        before_entry = before_bundle.get("entry") or {}
        normalized_scope = scope if is_active else "NONE"
        now_ts = datetime.now().isoformat(timespec="seconds")

        cur = self.conn.cursor()
        entry_id = self._ensure_cartesio_entry(cur, job_id, scope)

        resolved_status = self._resolve_cartesio_entry_status(
            cur=cur,
            entry_id=entry_id,
            fallback_status=status,
        )

        cur.execute(
            """
            UPDATE job_cartesio_entries
            SET referente = ?,
                status = ?,
                is_active = ?,
                updated_at = ?,
                last_activity_at = ?
            WHERE id = ?
            """,
            (
                str(referente or "").strip(),
                resolved_status,
                1 if is_active else 0,
                now_ts,
                now_ts,
                entry_id,
            ),
        )

        cur.execute(
            """
            UPDATE job_cartesio_entries
            SET is_active = CASE WHEN id = ? THEN ? ELSE 0 END
            WHERE job_id = ?
            """,
            (entry_id, 1 if is_active else 0, job_id),
        )

        cur.execute("INSERT OR IGNORE INTO job_meta (job_id) VALUES (?)", (job_id,))
        cur.execute(
            """
            UPDATE job_meta
            SET cartesio_delivery_scope = ?
            WHERE job_id = ?
            """,
            (normalized_scope, job_id),
        )

        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (job_id,))
        self._sync_legacy_cartesio_entry_fields(cur, job_id, scope)

        after_entry = self.get_cartesio_entry(job_id, scope) or {}
        changes = self._collect_field_changes(
            "job_cartesio_entries",
            before_entry,
            after_entry,
            ["referente", "status", "is_active", "cartesio_delivery_scope"],
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="save_cartesio_entry",
            summary=f"Aggiornata entry Cartesio {scope}",
            context={"scope": scope, "entry_id": entry_id},
        )
        self._insert_audit_changes(cur, event_id, changes)
        self._commit()
        return self.get_cartesio_bundle(job_id, scope)
    def add_cartesio_thread(self, job_id: int, scope: str, title: str) -> Dict[str, Any]:
        scope = self._normalize_cartesio_scope(scope)
        if scope not in {"PRG", "COS", "ACC"}:
            raise ValueError(f"Scope Cartesio non valido: {scope}")

        clean_title = str(title or "").strip()
        if not clean_title:
            raise ValueError("Il titolo del thread non può essere vuoto.")

        cur = self.conn.cursor()
        entry_id = self._ensure_cartesio_entry(cur, job_id, scope)
        before_entry = self.get_cartesio_entry_by_id(entry_id) or {}

        cur.execute(
            """
            INSERT INTO job_cartesio_threads (entry_id, title, status)
            VALUES (?, ?, 'APERTO')
            """,
            (entry_id, clean_title),
        )
        thread_id = int(cur.lastrowid)

        self._touch_cartesio_entry(cur, entry_id)

        resolved_status = self._resolve_cartesio_entry_status(
            cur=cur,
            entry_id=entry_id,
            fallback_status=before_entry.get("status"),
        )
        cur.execute(
            """
            UPDATE job_cartesio_entries
            SET status = ?
            WHERE id = ?
            """,
            (resolved_status, entry_id),
        )

        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (job_id,))
        self._sync_legacy_cartesio_entry_fields(cur, job_id, scope)

        after_entry = self.get_cartesio_entry_by_id(entry_id) or {}

        changes: List[Dict[str, str]] = []
        thread_change = self._build_audit_change(
            "job_cartesio_threads",
            "__created__",
            None,
            {"id": thread_id, "title": clean_title},
        )
        if thread_change:
            changes.append(thread_change)

        changes.extend(
            self._collect_field_changes(
                "job_cartesio_entries",
                before_entry,
                after_entry,
                ["status"],
            )
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="add_cartesio_thread",
            summary=f"Creato thread Cartesio {scope}: {clean_title}",
            context={"scope": scope, "thread_id": thread_id},
        )
        self._insert_audit_changes(cur, event_id, changes)
        self._commit()

        cur = self.conn.cursor()
        cur.execute("SELECT * FROM job_cartesio_threads WHERE id = ?", (thread_id,))
        row = cur.fetchone()
        return dict(row) if row else {}

    def set_cartesio_thread_status(self, thread_id: int, status: str) -> None:
        normalized_status = self._normalize_cartesio_thread_status(status)

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT t.*, e.job_id, e.scope
            FROM job_cartesio_threads t
            INNER JOIN job_cartesio_entries e ON e.id = t.entry_id
            WHERE t.id = ?
            """,
            (thread_id,),
        )
        before_row = cur.fetchone()
        if not before_row:
            raise ValueError(f"Thread Cartesio non trovato: {thread_id}")

        before = dict(before_row)
        entry_id = int(before["entry_id"])
        job_id = int(before["job_id"])
        scope = str(before["scope"] or "")
        before_entry = self.get_cartesio_entry_by_id(entry_id) or {}

        closed_at = datetime.now().isoformat(timespec="seconds") if normalized_status == "CHIUSO" else None

        cur.execute(
            """
            UPDATE job_cartesio_threads
            SET status = ?,
                updated_at = CURRENT_TIMESTAMP,
                closed_at = ?
            WHERE id = ?
            """,
            (normalized_status, closed_at, thread_id),
        )

        self._touch_cartesio_entry(cur, entry_id)

        resolved_status = self._resolve_cartesio_entry_status(
            cur=cur,
            entry_id=entry_id,
            fallback_status=before_entry.get("status"),
        )
        cur.execute(
            """
            UPDATE job_cartesio_entries
            SET status = ?
            WHERE id = ?
            """,
            (resolved_status, entry_id),
        )

        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (job_id,))
        self._sync_legacy_cartesio_entry_fields(cur, job_id, scope)

        cur.execute("SELECT * FROM job_cartesio_threads WHERE id = ?", (thread_id,))
        after = dict(cur.fetchone() or {})
        after_entry = self.get_cartesio_entry_by_id(entry_id) or {}

        changes = self._collect_field_changes(
            "job_cartesio_threads",
            before,
            after,
            ["status", "closed_at"],
        )
        changes.extend(
            self._collect_field_changes(
                "job_cartesio_entries",
                before_entry,
                after_entry,
                ["status"],
            )
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="set_cartesio_thread_status",
            summary=f"Thread Cartesio {scope} -> {normalized_status}",
            context={"thread_id": thread_id, "scope": scope},
        )
        self._insert_audit_changes(cur, event_id, changes)
        self._commit()
    def delete_cartesio_thread(self, thread_id: int) -> Dict[str, Any]:
        """
        Elimina un thread Cartesio e, per regola richiesta, elimina anche:
        - tutte le note appartenenti al thread
        - tutti gli allegati delle note appartenenti al thread

        Ritorna i metadati degli allegati eliminati così la GUI può rimuovere
        anche i file fisici dalla cartella cartesio_attachments.
        """
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT t.*, e.job_id, e.scope
            FROM job_cartesio_threads t
            INNER JOIN job_cartesio_entries e ON e.id = t.entry_id
            WHERE t.id = ?
            """,
            (thread_id,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Thread Cartesio non trovato: {thread_id}")

        before_thread = dict(row)
        entry_id = int(before_thread["entry_id"])
        job_id = int(before_thread["job_id"])
        scope = str(before_thread["scope"] or "")
        before_entry = self.get_cartesio_entry_by_id(entry_id) or {}

        cur.execute(
            """
            SELECT
                n.id AS note_id,
                n.title AS note_title,
                a.id AS attachment_id,
                a.stored_rel_path,
                a.display_name
            FROM job_cartesio_notes n
            LEFT JOIN job_cartesio_note_attachments a ON a.note_id = n.id
            WHERE n.thread_id = ?
            ORDER BY n.id DESC, a.id DESC
            """,
            (thread_id,),
        )
        rows = [dict(item) for item in cur.fetchall()]

        note_ids = sorted({int(item["note_id"]) for item in rows if item.get("note_id") is not None})

        attachments: List[Dict[str, Any]] = []
        seen_attachment_ids: set[int] = set()

        for item in rows:
            attachment_id = item.get("attachment_id")
            if attachment_id is None:
                continue

            attachment_id = int(attachment_id)
            if attachment_id in seen_attachment_ids:
                continue

            seen_attachment_ids.add(attachment_id)
            attachments.append(
                {
                    "id": attachment_id,
                    "stored_rel_path": str(item.get("stored_rel_path") or ""),
                    "display_name": str(item.get("display_name") or ""),
                    "note_id": int(item["note_id"]) if item.get("note_id") is not None else None,
                    "note_title": str(item.get("note_title") or ""),
                }
            )

        notes_count = len(note_ids)
        attachments_count = len(attachments)

        cur.execute(
            """
            DELETE FROM job_cartesio_note_attachments
            WHERE note_id IN (
                SELECT id
                FROM job_cartesio_notes
                WHERE thread_id = ?
            )
            """,
            (thread_id,),
        )

        cur.execute(
            """
            DELETE FROM job_cartesio_notes
            WHERE thread_id = ?
            """,
            (thread_id,),
        )

        cur.execute(
            """
            DELETE FROM job_cartesio_threads
            WHERE id = ?
            """,
            (thread_id,),
        )

        self._touch_cartesio_entry(cur, entry_id)

        resolved_status = self._resolve_cartesio_entry_status(
            cur=cur,
            entry_id=entry_id,
            fallback_status=before_entry.get("status"),
        )
        cur.execute(
            """
            UPDATE job_cartesio_entries
            SET status = ?
            WHERE id = ?
            """,
            (resolved_status, entry_id),
        )

        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (job_id,))
        self._sync_legacy_cartesio_entry_fields(cur, job_id, scope)

        after_entry = self.get_cartesio_entry_by_id(entry_id) or {}

        changes: List[Dict[str, str]] = []
        thread_change = self._build_audit_change(
            "job_cartesio_threads",
            "__deleted__",
            before_thread,
            None,
        )
        if thread_change:
            changes.append(thread_change)

        changes.extend(
            self._collect_field_changes(
                "job_cartesio_entries",
                before_entry,
                after_entry,
                ["status"],
            )
        )

        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="delete_cartesio_thread",
            summary=(
                f"Eliminato thread Cartesio {scope}: {before_thread.get('title', '')} "
                f"| note={notes_count} | allegati={attachments_count}"
            ),
            context={
                "thread_id": thread_id,
                "scope": scope,
                "deleted_note_ids": note_ids,
                "deleted_attachment_ids": [item["id"] for item in attachments],
            },
        )
        self._insert_audit_changes(cur, event_id, changes)
        self._commit()

        return {
            "thread": before_thread,
            "notes_count": notes_count,
            "attachments_count": attachments_count,
            "attachments": attachments,
        }

    def add_cartesio_note(
        self,
        job_id: int,
        scope: str,
        title: str,
        body: str,
        checklist_json: Optional[List[Dict[str, Any]]] = None,
        thread_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        scope = self._normalize_cartesio_scope(scope)
        if scope not in {"PRG", "COS", "ACC"}:
            raise ValueError(f"Scope Cartesio non valido: {scope}")

        clean_title = str(title or "").strip()
        if not clean_title:
            raise ValueError("Il titolo della nota non può essere vuoto.")

        cur = self.conn.cursor()
        entry_id = self._ensure_cartesio_entry(cur, job_id, scope)
        cur.execute(
            """
            INSERT INTO job_cartesio_notes (
                entry_id, thread_id, title, body, checklist_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                entry_id,
                thread_id,
                clean_title,
                str(body or "").strip(),
                json.dumps(checklist_json or [], ensure_ascii=False),
            ),
        )
        note_id = int(cur.lastrowid)
        self._touch_cartesio_entry(cur, entry_id)
        if thread_id:
            cur.execute(
                """
                UPDATE job_cartesio_threads
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (thread_id,),
            )
        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (job_id,))
        event_id = self._create_audit_event(
            cur=cur,
            job_id=job_id,
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="add_cartesio_note",
            summary=f"Creata nota Cartesio {scope}: {clean_title}",
            context={"scope": scope, "note_id": note_id, "thread_id": thread_id},
        )
        change = self._build_audit_change("job_cartesio_notes", "__created__", None, {"id": note_id, "title": clean_title})
        self._insert_audit_changes(cur, event_id, [change] if change else [])
        self._commit()
        return self.get_cartesio_note(note_id) or {}

    def update_cartesio_note(
        self,
        note_id: int,
        title: str,
        body: str,
        checklist_json: Optional[List[Dict[str, Any]]] = None,
        thread_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        before = self.get_cartesio_note(note_id)
        if not before:
            raise ValueError(f"Nota Cartesio non trovata: {note_id}")

        clean_title = str(title or "").strip()
        if not clean_title:
            raise ValueError("Il titolo della nota non può essere vuoto.")

        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE job_cartesio_notes
            SET title = ?,
                body = ?,
                checklist_json = ?,
                thread_id = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                clean_title,
                str(body or "").strip(),
                json.dumps(checklist_json or [], ensure_ascii=False),
                thread_id,
                note_id,
            ),
        )
        self._touch_cartesio_entry(cur, int(before["entry_id"]))
        if before.get("thread_id"):
            cur.execute("UPDATE job_cartesio_threads SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (int(before["thread_id"]),))
        if thread_id:
            cur.execute("UPDATE job_cartesio_threads SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (thread_id,))
        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (int(before["job_id"]),))
        after = self.get_cartesio_note(note_id) or {}
        changes = self._collect_field_changes(
            "job_cartesio_notes",
            before,
            after,
            ["title", "body", "checklist_json", "thread_id"],
        )
        event_id = self._create_audit_event(
            cur=cur,
            job_id=int(before["job_id"]),
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="update_cartesio_note",
            summary=f"Aggiornata nota Cartesio {before['scope']}: {clean_title}",
            context={"note_id": note_id, "scope": before["scope"]},
        )
        self._insert_audit_changes(cur, event_id, changes)
        self._commit()
        return self.get_cartesio_note(note_id) or {}

    def delete_cartesio_note(self, note_id: int) -> None:
        before = self.get_cartesio_note(note_id)
        if not before:
            return

        cur = self.conn.cursor()
        cur.execute("DELETE FROM job_cartesio_note_attachments WHERE note_id = ?", (note_id,))
        cur.execute("DELETE FROM job_cartesio_notes WHERE id = ?", (note_id,))
        self._touch_cartesio_entry(cur, int(before["entry_id"]))
        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (int(before["job_id"]),))
        event_id = self._create_audit_event(
            cur=cur,
            job_id=int(before["job_id"]),
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="delete_cartesio_note",
            summary=f"Eliminata nota Cartesio {before['scope']}: {before['title']}",
            context={"note_id": note_id, "scope": before["scope"]},
        )
        change = self._build_audit_change("job_cartesio_notes", "__deleted__", before, None)
        self._insert_audit_changes(cur, event_id, [change] if change else [])
        self._commit()

    def add_cartesio_note_attachment(
        self,
        note_id: int,
        attachment_kind: str,
        stored_rel_path: str,
        display_name: str,
        subject: str = "",
        sender: str = "",
        received_at: str = "",
        meta_json: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        note = self.get_cartesio_note(note_id)
        if not note:
            raise ValueError(f"Nota Cartesio non trovata: {note_id}")

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO job_cartesio_note_attachments (
                note_id,
                attachment_kind,
                stored_rel_path,
                display_name,
                subject,
                sender,
                received_at,
                meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                note_id,
                str(attachment_kind or "file").strip() or "file",
                str(stored_rel_path or "").strip(),
                str(display_name or "").strip(),
                str(subject or "").strip(),
                str(sender or "").strip(),
                str(received_at or "").strip(),
                json.dumps(meta_json or {}, ensure_ascii=False),
            ),
        )
        attachment_id = int(cur.lastrowid)
        self._touch_cartesio_entry(cur, int(note["entry_id"]))
        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (int(note["job_id"]),))
        event_id = self._create_audit_event(
            cur=cur,
            job_id=int(note["job_id"]),
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="add_cartesio_note_attachment",
            summary=f"Aggiunto allegato a nota Cartesio {note['scope']}: {display_name}",
            context={"note_id": note_id, "attachment_id": attachment_id},
        )
        change = self._build_audit_change("job_cartesio_note_attachments", "__created__", None, {"id": attachment_id, "display_name": display_name})
        self._insert_audit_changes(cur, event_id, [change] if change else [])
        self._commit()
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM job_cartesio_note_attachments WHERE id = ?", (attachment_id,))
        row = cur.fetchone()
        return dict(row) if row else {}

    def remove_cartesio_attachment(self, attachment_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT a.*, n.entry_id, e.job_id, e.scope
            FROM job_cartesio_note_attachments a
            INNER JOIN job_cartesio_notes n ON n.id = a.note_id
            INNER JOIN job_cartesio_entries e ON e.id = n.entry_id
            WHERE a.id = ?
            """,
            (attachment_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        before = dict(row)
        cur.execute("DELETE FROM job_cartesio_note_attachments WHERE id = ?", (attachment_id,))
        self._touch_cartesio_entry(cur, int(before["entry_id"]))
        cur.execute("UPDATE jobs SET updated_at = CURRENT_TIMESTAMP WHERE id = ?", (int(before["job_id"]),))
        event_id = self._create_audit_event(
            cur=cur,
            job_id=int(before["job_id"]),
            action_kind="UPDATE_META",
            source_kind="manual",
            origin_method="remove_cartesio_attachment",
            summary=f"Rimosso allegato Cartesio {before['scope']}: {before.get('display_name', '')}",
            context={"attachment_id": attachment_id, "scope": before["scope"]},
        )
        change = self._build_audit_change("job_cartesio_note_attachments", "__deleted__", before, None)
        self._insert_audit_changes(cur, event_id, [change] if change else [])
        self._commit()
        return before

    def fetch_cartesio_dashboard_rows(self, scope: str) -> List[Dict[str, Any]]:
        scope = self._normalize_cartesio_scope(scope)
        if scope not in {"PRG", "COS", "ACC"}:
            return []

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                j.id AS job_id,
                e.id AS entry_id,
                e.scope,
                e.referente,
                e.status AS entry_status,
                e.checklist_json,
                e.last_activity_at,
                j.project_distretto_anno,
                j.project_name,
                j.project_mode,
                j.project_base_path,
                j.dl_distretto_anno,
                j.dl_name,
                j.dl_base_path,
                m.cartesio_delivery_scope,
                COALESCE((
                    SELECT COUNT(*)
                    FROM job_cartesio_threads t
                    WHERE t.entry_id = e.id AND t.status = 'APERTO'
                ), 0) AS open_threads,
                COALESCE((
                    SELECT n.title
                    FROM job_cartesio_notes n
                    WHERE n.entry_id = e.id
                    ORDER BY n.updated_at DESC, n.id DESC
                    LIMIT 1
                ), '') AS latest_note_title,
                COALESCE((
                    SELECT n.updated_at
                    FROM job_cartesio_notes n
                    WHERE n.entry_id = e.id
                    ORDER BY n.updated_at DESC, n.id DESC
                    LIMIT 1
                ), e.last_activity_at) AS latest_note_updated_at
            FROM job_cartesio_entries e
            INNER JOIN jobs j ON j.id = e.job_id
            LEFT JOIN job_meta m ON m.job_id = j.id
            WHERE e.scope = ?
            AND e.is_active = 1
            AND COALESCE(m.cartesio_delivery_scope, 'NONE') = ?
            ORDER BY COALESCE(e.last_activity_at, j.updated_at) DESC, j.updated_at DESC, j.id DESC
            """,
            (scope, scope),
        )

        items: List[Dict[str, Any]] = []

        for row in cur.fetchall():
            item = dict(row)
            checklist_value = item.get("checklist_json")

            if isinstance(checklist_value, str) and checklist_value:
                try:
                    item["checklist_json"] = json.loads(checklist_value)
                except Exception:
                    logging.exception("JSON non valido in dashboard Cartesio: checklist_json")
                    item["checklist_json"] = []
            elif isinstance(checklist_value, list):
                item["checklist_json"] = checklist_value
            else:
                item["checklist_json"] = []

            items.append(item)

        return items

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

    def get_latest_alert_event_map(self, job_ids: List[int], user_name: str) -> Dict[int, Dict[str, Any]]:
        """
        Ritorna, per ogni job, l'ultimo evento che deve accendere il punto esclamativo
        per l'utente corrente.

        Regola:
        - NON genera alert per eventi manual/override fatti dallo stesso utente Windows
        - mantiene visibili scan/autofill/system anche se initiated_by coincide
        - mantiene visibili tutti gli eventi fatti da altri utenti
        """
        if not job_ids:
            return {}

        placeholders = ",".join("?" for _ in job_ids)
        params: List[Any] = [*job_ids, user_name]

        cur = self.conn.cursor()
        cur.execute(
            f"""
            SELECT
                e.job_id,
                e.id AS event_id,
                e.event_ts,
                e.source_kind,
                e.initiated_by,
                e.summary
            FROM job_audit_events e
            INNER JOIN (
                SELECT job_id, MAX(id) AS max_event_id
                FROM job_audit_events
                WHERE job_id IN ({placeholders})
                  AND (
                        LOWER(COALESCE(source_kind, '')) NOT IN ('manual', 'override')
                        OR UPPER(TRIM(COALESCE(initiated_by, ''))) <> UPPER(TRIM(?))
                  )
                GROUP BY job_id
            ) latest
                ON latest.job_id = e.job_id
               AND latest.max_event_id = e.id
            """,
            params,
        )

        result: Dict[int, Dict[str, Any]] = {}
        for row in cur.fetchall():
            result[int(row["job_id"])] = {
                "event_id": int(row["event_id"]),
                "event_ts": str(row["event_ts"] or ""),
                "source_kind": str(row["source_kind"] or ""),
                "initiated_by": str(row["initiated_by"] or ""),
                "summary": str(row["summary"] or ""),
            }
        return result

    def get_job_latest_alert_event(self, job_id: int, user_name: str) -> Dict[str, Any]:
        """
        Helper single-job per dialog storico / refresh puntuali.
        """
        return self.get_latest_alert_event_map([job_id], user_name).get(job_id, {})

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
        exception_mode = str(payload.get("exception_mode", "STANDARD") or "STANDARD").strip().upper()
        if exception_mode == "MANUAL":
            return

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
                job_id,
                permits_mode,
                cartesio_delivery_scope,
                permits_checklist_json,
                permits_notes,
                cartesio_prg_status,
                cartesio_prg_notes,
                rilievi_dl_status,
                rilievi_dl_notes,
                cartesio_cos_status,
                cartesio_cos_notes,
                project_tracciamento_manual_path,
                psc_path,
                psc_status,
                todo_json,
                exception_mode,
                exception_reason,
                exception_group_code,
                manual_project_control_path,
                manual_dl_control_path,
                manual_cartesio_prg_code,
                manual_cartesio_prg_path,
                manual_cartesio_cos_code,
                manual_cartesio_cos_path,
                manual_cartesio_acc_code,
                manual_cartesio_acc_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                payload.get("permits_mode", "REQUIRED"),
                payload.get("cartesio_delivery_scope", "NONE"),
                json.dumps(payload.get("permits_checklist_json") or [], ensure_ascii=False),
                payload.get("permits_notes", ""),
                payload.get("cartesio_prg_status", "NON IMPOSTATO"),
                payload.get("cartesio_prg_notes", ""),
                payload.get("rilievi_dl_status", "NON IMPOSTATO"),
                payload.get("rilievi_dl_notes", ""),
                payload.get("cartesio_cos_status", "NON IMPOSTATO"),
                payload.get("cartesio_cos_notes", ""),
                payload.get("project_tracciamento_manual_path", ""),
                payload.get("psc_path", ""),
                payload.get("psc_status", "NOT_SET"),
                json.dumps(payload.get("todo_json") or [], ensure_ascii=False),
                payload.get("exception_mode", "STANDARD"),
                payload.get("exception_reason", ""),
                payload.get("exception_group_code", ""),
                payload.get("manual_project_control_path", ""),
                payload.get("manual_dl_control_path", ""),
                payload.get("manual_cartesio_prg_code", ""),
                payload.get("manual_cartesio_prg_path", ""),
                payload.get("manual_cartesio_cos_code", ""),
                payload.get("manual_cartesio_cos_path", ""),
                payload.get("manual_cartesio_acc_code", ""),
                payload.get("manual_cartesio_acc_path", ""),
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
            "cartesio_delivery_scope",
            "permits_checklist_json",
            "permits_notes",
            "cartesio_prg_status",
            "cartesio_prg_notes",
            "rilievi_dl_status",
            "rilievi_dl_notes",
            "cartesio_cos_status",
            "cartesio_cos_notes",
            "project_tracciamento_manual_path",
            "psc_path",
            "psc_status",
            "todo_json",
            "exception_mode",
            "exception_reason",
            "exception_group_code",
            "manual_project_control_path",
            "manual_dl_control_path",
            "manual_cartesio_prg_code",
            "manual_cartesio_prg_path",
            "manual_cartesio_cos_code",
            "manual_cartesio_cos_path",
        ]

        new_jobs_values = {
            "project_base_path": payload.get("project_base_path", ""),
            "dl_base_path": payload.get("dl_base_path", ""),
            "project_distretto_anno": payload.get("project_distretto_anno", ""),
            "project_name": payload.get("project_name", ""),
            "project_mode": payload.get("project_mode", "GTN"),
            "dl_distretto_anno": payload.get("dl_distretto_anno", ""),
            "dl_name": payload.get("dl_name", ""),
            "dl_insert_date": payload.get("dl_insert_date", ""),
            "general_notes": payload.get("general_notes", ""),
        }

        new_meta_values = {
            "permits_mode": payload.get("permits_mode", "REQUIRED"),
            "cartesio_delivery_scope": payload.get(
                "cartesio_delivery_scope",
                before_row.get("cartesio_delivery_scope", "NONE"),
            ),
            "permits_checklist_json": payload.get("permits_checklist_json") or [],
            "permits_notes": payload.get("permits_notes", ""),
            "cartesio_prg_status": payload.get("cartesio_prg_status", "NON IMPOSTATO"),
            "cartesio_prg_notes": payload.get("cartesio_prg_notes", ""),
            "rilievi_dl_status": payload.get("rilievi_dl_status", "NON IMPOSTATO"),
            "rilievi_dl_notes": payload.get("rilievi_dl_notes", ""),
            "cartesio_cos_status": payload.get("cartesio_cos_status", "NON IMPOSTATO"),
            "cartesio_cos_notes": payload.get("cartesio_cos_notes", ""),
            "project_tracciamento_manual_path": payload.get("project_tracciamento_manual_path", ""),
            "psc_path": payload.get("psc_path", ""),
            "psc_status": payload.get("psc_status", "NOT_SET"),
            "todo_json": payload.get("todo_json") or [],
            "exception_mode": payload.get("exception_mode", "STANDARD"),
            "exception_reason": payload.get("exception_reason", ""),
            "exception_group_code": payload.get("exception_group_code", ""),
            "manual_project_control_path": payload.get("manual_project_control_path", ""),
            "manual_dl_control_path": payload.get("manual_dl_control_path", ""),
            "manual_cartesio_prg_code": payload.get("manual_cartesio_prg_code", ""),
            "manual_cartesio_prg_path": payload.get("manual_cartesio_prg_path", ""),
            "manual_cartesio_cos_code": payload.get("manual_cartesio_cos_code", ""),
            "manual_cartesio_cos_path": payload.get("manual_cartesio_cos_path", ""),
            "manual_cartesio_acc_code": payload.get("manual_cartesio_acc_code", ""),
            "manual_cartesio_acc_path": payload.get("manual_cartesio_acc_path", ""),
        }

        changes: List[Dict[str, str]] = []
        changes.extend(self._collect_field_changes("jobs", before_row, new_jobs_values, jobs_fields))
        changes.extend(self._collect_field_changes("job_meta", before_row, new_meta_values, meta_fields))

        if not changes:
            logging.info("update_job ignorato: nessuna differenza per job %s", job_id)
            return

        cur = self.conn.cursor()

        cur.execute(
            """
            UPDATE jobs SET
                project_base_path = ?,
                dl_base_path = ?,
                project_distretto_anno = ?,
                project_name = ?,
                project_mode = ?,
                dl_distretto_anno = ?,
                dl_name = ?,
                dl_insert_date = ?,
                general_notes = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                new_jobs_values["project_base_path"],
                new_jobs_values["dl_base_path"],
                new_jobs_values["project_distretto_anno"],
                new_jobs_values["project_name"],
                new_jobs_values["project_mode"],
                new_jobs_values["dl_distretto_anno"],
                new_jobs_values["dl_name"],
                new_jobs_values["dl_insert_date"],
                new_jobs_values["general_notes"],
                job_id,
            ),
        )

        cur.execute(
            """
            INSERT OR REPLACE INTO job_meta (
                job_id,
                permits_mode,
                cartesio_delivery_scope,
                permits_checklist_json,
                permits_notes,
                cartesio_prg_status,
                cartesio_prg_notes,
                rilievi_dl_status,
                rilievi_dl_notes,
                cartesio_cos_status,
                cartesio_cos_notes,
                project_tracciamento_manual_path,
                psc_path,
                psc_status,
                todo_json,
                exception_mode,
                exception_reason,
                exception_group_code,
                manual_project_control_path,
                manual_dl_control_path,
                manual_cartesio_prg_code,
                manual_cartesio_prg_path,
                manual_cartesio_cos_code,
                manual_cartesio_cos_path,
                manual_cartesio_acc_code,
                manual_cartesio_acc_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                new_meta_values["permits_mode"],
                new_meta_values["cartesio_delivery_scope"],
                json.dumps(new_meta_values["permits_checklist_json"], ensure_ascii=False),
                new_meta_values["permits_notes"],
                new_meta_values["cartesio_prg_status"],
                new_meta_values["cartesio_prg_notes"],
                new_meta_values["rilievi_dl_status"],
                new_meta_values["rilievi_dl_notes"],
                new_meta_values["cartesio_cos_status"],
                new_meta_values["cartesio_cos_notes"],
                new_meta_values["project_tracciamento_manual_path"],
                new_meta_values["psc_path"],
                new_meta_values["psc_status"],
                json.dumps(new_meta_values["todo_json"], ensure_ascii=False),
                new_meta_values["exception_mode"],
                new_meta_values["exception_reason"],
                new_meta_values["exception_group_code"],
                new_meta_values["manual_project_control_path"],
                new_meta_values["manual_dl_control_path"],
                new_meta_values["manual_cartesio_prg_code"],
                new_meta_values["manual_cartesio_prg_path"],
                new_meta_values["manual_cartesio_cos_code"],
                new_meta_values["manual_cartesio_cos_path"],
                new_meta_values["manual_cartesio_acc_code"],
                new_meta_values["manual_cartesio_acc_path"],
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

        cur.execute(
            """
            SELECT id
            FROM job_audit_events
            WHERE job_id = ?
            """,
            (job_id,),
        )
        event_ids = [int(row["id"]) for row in cur.fetchall()]

        if event_ids:
            placeholders = ",".join("?" for _ in event_ids)
            cur.execute(
                f"""
                DELETE FROM job_audit_changes
                WHERE event_id IN ({placeholders})
                """,
                event_ids,
            )

        cur.execute("DELETE FROM job_audit_events WHERE job_id = ?", (job_id,))
        cur.execute("DELETE FROM job_audit_user_state WHERE job_id = ?", (job_id,))
        cur.execute("DELETE FROM job_scan_overrides WHERE job_id = ?", (job_id,))
        cur.execute("DELETE FROM job_scan_cache WHERE job_id = ?", (job_id,))
        cur.execute("DELETE FROM job_meta WHERE job_id = ?", (job_id,))
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
            "cartesio_delivery_scope",
            "permits_checklist_json",
            "permits_notes",
            "cartesio_prg_status",
            "cartesio_prg_notes",
            "rilievi_dl_status",
            "rilievi_dl_notes",
            "cartesio_cos_status",
            "cartesio_cos_notes",
            "project_tracciamento_manual_path",
            "psc_path",
            "psc_status",
            "todo_json",
            "exception_mode",
            "exception_reason",
            "exception_group_code",
            "manual_project_control_path",
            "manual_dl_control_path",
            "manual_cartesio_prg_code",
            "manual_cartesio_prg_path",
            "manual_cartesio_cos_code",
            "manual_cartesio_cos_path",
            "manual_cartesio_acc_code",
            "manual_cartesio_acc_path",
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