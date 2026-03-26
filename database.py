# -*- coding: utf-8 -*-
"""Gestione database SQLite per Situazione Lavori."""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils import DB_FILE, path_compare_key


class DatabaseManager:
    """Layer minimale sopra SQLite con row factory dict-like."""

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

        # Abilita integrità referenziale
        cur.execute("PRAGMA foreign_keys = ON;")

        # Aspetta fino a 30 secondi se il DB è occupato
        cur.execute("PRAGMA busy_timeout = 30000;")

        # Su share di rete meglio journal classico
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
                todo_json TEXT,
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            )
            """
        )
        self._commit()

    def close(self) -> None:
        try:
            if getattr(self, "conn", None):
                self.conn.close()
                logging.info("Connessione DB chiusa: %s", self.db_path)
        except Exception:
            logging.exception("Errore durante la chiusura del DB")

    def fetch_jobs(self) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT j.*, m.*
            FROM jobs j
            LEFT JOIN job_meta m ON m.job_id = j.id
            ORDER BY j.id DESC
            """
        )
        rows = [dict(row) for row in cur.fetchall()]
        for row in rows:
            self._decode_json_fields(row)
        return rows

    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT j.*, m.*
            FROM jobs j
            LEFT JOIN job_meta m ON m.job_id = j.id
            WHERE j.id = ?
            """,
            (job_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        data = dict(row)
        self._decode_json_fields(data)
        return data

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
            
    def add_job(self, payload: Dict[str, Any]) -> int:
        self._validate_unique_paths(payload)
        logging.info("Inserimento nuovo job")
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (
                project_base_path, dl_base_path, project_distretto_anno, project_name,
                dl_distretto_anno, dl_name, dl_insert_date, general_notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload.get("project_base_path", ""),
                payload.get("dl_base_path", ""),
                payload.get("project_distretto_anno", ""),
                payload.get("project_name", ""),
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
                job_id, permits_checklist_json, permits_notes,
                cartesio_prg_status, cartesio_prg_notes, cartesio_prg_manual_code,
                rilievi_dl_status, rilievi_dl_notes,
                cartesio_cos_status, cartesio_cos_notes, cartesio_cos_manual_code,
                todo_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
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
                json.dumps(payload.get("todo_json") or [], ensure_ascii=False),
            ),
        )
        self._commit()
        return job_id

    def update_job(self, job_id: int, payload: Dict[str, Any]) -> None:
        self._validate_unique_paths(payload, exclude_job_id=job_id)
        logging.info("Aggiornamento job %s", job_id)
        cur = self.conn.cursor()
        cur.execute(
            """
            UPDATE jobs SET
                project_base_path=?, dl_base_path=?, project_distretto_anno=?, project_name=?,
                dl_distretto_anno=?, dl_name=?, dl_insert_date=?, general_notes=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            (
                payload.get("project_base_path", ""),
                payload.get("dl_base_path", ""),
                payload.get("project_distretto_anno", ""),
                payload.get("project_name", ""),
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
                job_id, permits_checklist_json, permits_notes,
                cartesio_prg_status, cartesio_prg_notes, cartesio_prg_manual_code,
                rilievi_dl_status, rilievi_dl_notes,
                cartesio_cos_status, cartesio_cos_notes, cartesio_cos_manual_code,
                todo_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
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
                json.dumps(payload.get("todo_json") or [], ensure_ascii=False),
            ),
        )
        self._commit()

    def delete_job(self, job_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM job_meta WHERE job_id = ?", (job_id,))
        cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        self._commit()

    def update_meta_fields(self, job_id: int, **fields: Any) -> None:
        """Aggiorna solo alcuni campi di job_meta, senza toccare il resto."""
        current = self.get_job(job_id)
        if not current:
            return
        base = {
            "permits_checklist_json": current.get("permits_checklist_json") or [],
            "permits_notes": current.get("permits_notes", ""),
            "cartesio_prg_status": current.get("cartesio_prg_status", "NON IMPOSTATO"),
            "cartesio_prg_notes": current.get("cartesio_prg_notes", ""),
            "cartesio_prg_manual_code": current.get("cartesio_prg_manual_code", ""),
            "rilievi_dl_status": current.get("rilievi_dl_status", "NON IMPOSTATO"),
            "rilievi_dl_notes": current.get("rilievi_dl_notes", ""),
            "cartesio_cos_status": current.get("cartesio_cos_status", "NON IMPOSTATO"),
            "cartesio_cos_notes": current.get("cartesio_cos_notes", ""),
            "cartesio_cos_manual_code": current.get("cartesio_cos_manual_code", ""),
            "todo_json": current.get("todo_json") or [],
        }
        base.update(fields)
        self.update_job(job_id, {**current, **base})

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