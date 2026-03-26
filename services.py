# -*- coding: utf-8 -*-
"""Servizi applicativi che combinano DB e scansione."""
from __future__ import annotations

import logging
import socket
from datetime import date
from typing import Any, Dict, List, Optional

from database import DatabaseManager
from scanner import FileSystemScanner


class JobService:
    def __init__(self, db: DatabaseManager, scanner: FileSystemScanner) -> None:
        self.db = db
        self.scanner = scanner

    # -------------------------------------------------------------------------
    # LETTURA GUI: SOLO DB
    # -------------------------------------------------------------------------

    def load_jobs_for_ui(self) -> List[Dict[str, Any]]:
        """
        Carica le righe come le deve leggere la GUI:
        solo dati persistiti nel DB, senza scansione runtime.
        """
        jobs = self.db.fetch_jobs()
        return [self.apply_derived_fields_from_db(job) for job in jobs]

    def get_row_for_ui(self, job_id: int) -> Optional[Dict[str, Any]]:
        job = self.db.get_job(job_id)
        if not job:
            return None
        return self.apply_derived_fields_from_db(job)

    def apply_derived_fields_from_db(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Garantisce che una riga letta dal DB abbia tutti i campi di display pronti.
        Non fa scansioni.
        """
        row = dict(job)
        scan_data = row.get("scan") or {}
        row["scan"] = scan_data

        row["permits_display"] = row.get("permits_display") or self._compute_permits_display(
            row.get("permits_checklist_json") or []
        )

        row["cartesio_prg_display"] = row.get("cartesio_prg_display") or self._compute_cartesio_prg_display(
            row,
            scan_data,
        )

        row["cartesio_cos_display"] = row.get("cartesio_cos_display") or self._compute_cartesio_cos_display(
            row,
            scan_data,
        )

        row["rilievi_dl_display"] = row.get("rilievi_dl_display") or scan_data.get("rilievi_dl", {}).get("display", "❌")

        row["revisions_match"] = row.get("revisions_match") or self._revisions_match(
            scan_data.get("project_revision", {}).get("display", ""),
            scan_data.get("permessi_revision", {}).get("display", ""),
        )

        return row

    def refresh_row_without_rescan(self, current_row: Dict[str, Any], **updated_fields: Any) -> Dict[str, Any]:
        """
        Aggiorna in memoria una riga già esistente senza fare scansione.
        Serve per modifiche manuali.
        """
        row = dict(current_row)
        row.update(updated_fields)

        scan_data = row.get("scan") or current_row.get("scan") or {}
        row["scan"] = scan_data

        row["permits_display"] = self._compute_permits_display(
            row.get("permits_checklist_json") or []
        )
        row["cartesio_prg_display"] = self._compute_cartesio_prg_display(row, scan_data)
        row["cartesio_cos_display"] = self._compute_cartesio_cos_display(row, scan_data)
        row["rilievi_dl_display"] = scan_data.get("rilievi_dl", {}).get("display", "❌")
        row["revisions_match"] = self._revisions_match(
            scan_data.get("project_revision", {}).get("display", ""),
            scan_data.get("permessi_revision", {}).get("display", ""),
        )
        return row

    # -------------------------------------------------------------------------
    # SCANSIONE E PERSISTENZA
    # -------------------------------------------------------------------------

    def scan_and_persist_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Esegue la scansione della singola riga, salva il risultato nel DB,
        poi rilegge dal DB la versione finale per la GUI.
        """
        job = self.db.get_job(job_id)
        if not job:
            return None

        scan_data = self.scanner.scan_job(job)

        permits_display = self._compute_permits_display(job.get("permits_checklist_json") or [])
        cartesio_prg_display = self._compute_cartesio_prg_display(job, scan_data)
        cartesio_cos_display = self._compute_cartesio_cos_display(job, scan_data)
        rilievi_dl_display = scan_data.get("rilievi_dl", {}).get("display", "❌")
        revisions_match = self._revisions_match(
            scan_data.get("project_revision", {}).get("display", ""),
            scan_data.get("permessi_revision", {}).get("display", ""),
        )

        self.db.save_scan_cache(
            job_id=job_id,
            scan_data=scan_data,
            permits_display=permits_display,
            cartesio_prg_display=cartesio_prg_display,
            rilievi_dl_display=rilievi_dl_display,
            cartesio_cos_display=cartesio_cos_display,
            revisions_match=revisions_match,
        )

        return self.get_row_for_ui(job_id)

    def scan_and_persist_jobs(self, job_ids: List[int]) -> List[Dict[str, Any]]:
        updated_rows: List[Dict[str, Any]] = []
        for job_id in job_ids:
            updated = self.scan_and_persist_job(job_id)
            if updated:
                updated_rows.append(updated)
        return updated_rows

    def scan_all_and_persist(self) -> List[Dict[str, Any]]:
        """
        Esegue lo scan completo di tutti i job e persiste i risultati nel DB.
        """
        jobs = self.db.fetch_jobs()

        for job in jobs:
            scan_data = self.scanner.scan_job(job)

            permits_display = self._compute_permits_display(job.get("permits_checklist_json") or [])
            cartesio_prg_display = self._compute_cartesio_prg_display(job, scan_data)
            cartesio_cos_display = self._compute_cartesio_cos_display(job, scan_data)
            rilievi_dl_display = scan_data.get("rilievi_dl", {}).get("display", "❌")
            revisions_match = self._revisions_match(
                scan_data.get("project_revision", {}).get("display", ""),
                scan_data.get("permessi_revision", {}).get("display", ""),
            )

            self.db.save_scan_cache(
                job_id=job["id"],
                scan_data=scan_data,
                permits_display=permits_display,
                cartesio_prg_display=cartesio_prg_display,
                rilievi_dl_display=rilievi_dl_display,
                cartesio_cos_display=cartesio_cos_display,
                revisions_match=revisions_match,
                commit=False,
            )

        self.db.set_last_global_scan_date_today(commit=False)
        self.db._commit()

        return self.load_jobs_for_ui()

    def startup_load(self) -> List[Dict[str, Any]]:
        """
        Regola startup:
        - primo avvio del giorno -> scan globale + save DB
        - altrimenti -> solo lettura DB
        """
        today = date.today().isoformat()
        last_scan = self.db.get_last_global_scan_date()

        if last_scan == today:
            logging.info("Startup: dati già aggiornati oggi, lettura solo DB")
            return self.load_jobs_for_ui()

        owner = f"{socket.gethostname()}::{date.today().isoformat()}"
        lock_acquired = False

        try:
            lock_acquired = self.db.try_acquire_global_scan_lock(owner)
            if lock_acquired:
                logging.info("Startup: acquisito lock scan globale, eseguo scan completo")
                rows = self.scan_all_and_persist()
                return rows

            logging.info("Startup: scan globale già in corso da altra istanza, leggo dati DB")
            return self.load_jobs_for_ui()

        finally:
            if lock_acquired:
                try:
                    self.db.release_global_scan_lock()
                except Exception:
                    logging.exception("Errore rilascio lock scan globale")

    # -------------------------------------------------------------------------
    # HELPERS DERIVATI
    # -------------------------------------------------------------------------

    def _compute_permits_display(self, checklist: List[Dict[str, Any]]) -> str:
        if not checklist:
            return "❌"

        required_items = [item for item in checklist if item.get("required")]
        if required_items and all(item.get("obtained") for item in required_items):
            return "✅"

        return "🔄"

    def _compute_cartesio_prg_display(self, row: Dict[str, Any], scan_data: Dict[str, Any]) -> str:
        prg_auto = scan_data.get("cartesio_prg", {}).get("code", "")
        prg_manual = row.get("cartesio_prg_manual_code", "")
        return prg_auto or prg_manual or scan_data.get("cartesio_prg", {}).get("display", "❌")

    def _compute_cartesio_cos_display(self, row: Dict[str, Any], scan_data: Dict[str, Any]) -> str:
        cos_auto = scan_data.get("cartesio_cos", {}).get("code", "")
        cos_manual = row.get("cartesio_cos_manual_code", "")
        return cos_auto or cos_manual or scan_data.get("cartesio_cos", {}).get("display", "❌")

    def _revisions_match(self, rev_project: str, rev_permessi: str) -> str:
        if rev_project.isdigit() and rev_permessi.isdigit():
            return "MATCH" if rev_project == rev_permessi else "MISMATCH"
        return "UNKNOWN"