# -*- coding: utf-8 -*-
"""Servizi applicativi che combinano DB e scansione."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from database import DatabaseManager
from scanner import FileSystemScanner


class JobService:
    def __init__(self, db: DatabaseManager, scanner: FileSystemScanner) -> None:
        self.db = db
        self.scanner = scanner

    def load_jobs_with_scan(self) -> List[Dict[str, Any]]:
        jobs = self.db.fetch_jobs()
        return [self.merge_scanned_data(job) for job in jobs]

    def merge_scanned_data(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fonde i dati DB con una nuova scansione filesystem.
        Usare quando serve ricalcolare i campi automatici della riga.
        """
        scanned = self.scanner.scan_job(job)
        return self.apply_derived_fields(job, scanned=scanned)

    def refresh_row_from_db(self, job_id: int, rescan: bool = True) -> Optional[Dict[str, Any]]:
        """
        Rilegge una singola riga dal DB.
        - rescan=True  -> rifà la scansione filesystem della sola riga
        - rescan=False -> ricalcola solo i campi derivati dai dati già presenti
        """
        job = self.db.get_job(job_id)
        if not job:
            return None

        if rescan:
            return self.merge_scanned_data(job)

        return self.apply_derived_fields(job)

    def refresh_row_without_rescan(self, current_row: Dict[str, Any], **updated_fields: Any) -> Dict[str, Any]:
        """
        Aggiorna una riga già in memoria applicando solo i cambi manuali,
        mantenendo invariata la scansione già disponibile.
        """
        row = dict(current_row)
        row.update(updated_fields)
        return self.apply_derived_fields(row, scanned=row.get("scan") or current_row.get("scan") or {})

    def apply_derived_fields(
        self,
        job: Dict[str, Any],
        scanned: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Calcola i campi di display derivati a partire da:
        - dati DB
        - scansione filesystem già disponibile o appena letta
        """
        row = dict(job)
        scan_data = scanned if scanned is not None else (row.get("scan") or {})
        row["scan"] = scan_data

        # Permessi: derivato dalla checklist manuale
        row["permits_display"] = self._compute_permits_display(
            row.get("permits_checklist_json") or []
        )

        # Cartesio PRG: codice automatico prioritario, altrimenti manuale, poi display scanner
        prg_auto = scan_data.get("cartesio_prg", {}).get("code", "")
        prg_manual = row.get("cartesio_prg_manual_code", "")
        row["cartesio_prg_display"] = (
            prg_auto
            or prg_manual
            or scan_data.get("cartesio_prg", {}).get("display", "❌")
        )

        # Cartesio COS: codice automatico prioritario, altrimenti manuale, poi display scanner
        cos_auto = scan_data.get("cartesio_cos", {}).get("code", "")
        cos_manual = row.get("cartesio_cos_manual_code", "")
        row["cartesio_cos_display"] = (
            cos_auto
            or cos_manual
            or scan_data.get("cartesio_cos", {}).get("display", "❌")
        )

        # Rilievi DL: portato esplicitamente nella riga cache, così il model non dipende dal solo scan raw
        row["rilievi_dl_display"] = scan_data.get("rilievi_dl", {}).get("display", "❌")

        row["revisions_match"] = self._revisions_match(
            scan_data.get("project_revision", {}).get("display", ""),
            scan_data.get("permessi_revision", {}).get("display", ""),
        )

        return row

    def _compute_permits_display(self, checklist: List[Dict[str, Any]]) -> str:
        if not checklist:
            return "❌"

        required_items = [item for item in checklist if item.get("required")]
        if required_items and all(item.get("obtained") for item in required_items):
            return "✅"

        return "🔄"

    def _revisions_match(self, rev_project: str, rev_permessi: str) -> str:
        if rev_project.isdigit() and rev_permessi.isdigit():
            return "MATCH" if rev_project == rev_permessi else "MISMATCH"
        return "UNKNOWN"