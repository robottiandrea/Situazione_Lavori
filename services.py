# -*- coding: utf-8 -*-
"""Servizi applicativi che combinano DB e scansione."""
from __future__ import annotations

import logging
import socket
from datetime import date
from typing import Any, Dict, List, Optional

from database import DatabaseManager
from scanner import FileSystemScanner

from utils import (
    folder_name_from_path,
    infer_project_distretto_anno,
    exists_dir,
)

class JobService:
    def __init__(self, db: DatabaseManager, scanner: FileSystemScanner) -> None:
        self.db = db
        self.scanner = scanner

    # -------------------------------------------------------------------------
    # LETTURA GUI: SOLO DB
    # -------------------------------------------------------------------------
    def _autofill_project_from_dl_link(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Se project_base_path è vuoto, prova a ricavarlo dal .lnk dentro la DL.

        Regole:
        - non sovrascrive mai un project_base_path già presente
        - accetta solo path ricavati come prima sottocartella sotto un base path 'Progetti'
        - non scrive se il path risultante non esiste
        - non scrive se il path è già usato da un altro job
        """
        if not job:
            return job

        if str(job.get("project_base_path", "") or "").strip():
            return job

        dl_base_path = str(job.get("dl_base_path", "") or "").strip()
        if not dl_base_path:
            return job

        link_info = self.scanner.find_project_root_from_dl_link(dl_base_path)
        project_root = str(link_info.get("project_root", "") or "").strip()
        status = str(link_info.get("status", "") or "").strip()

        if not project_root:
            logging.info(
                "Autofill PRG da DL non eseguito per job %s | status=%s | dl=%s | lookup=%s | link=%s | target=%s",
                job.get("id"),
                status,
                dl_base_path,
                link_info.get("lookup_folder", ""),
                link_info.get("link_path", ""),
                link_info.get("target_path", ""),
            )
            return job

        if not exists_dir(project_root):
            logging.warning(
                "Project root da link DL non esistente per job %s: %s",
                job.get("id"),
                project_root,
            )
            return job

        written = self.db.autofill_project_path_if_empty(
            job_id=int(job["id"]),
            project_base_path=project_root,
            project_distretto_anno=infer_project_distretto_anno(project_root),
            project_name=folder_name_from_path(project_root),
        )

        if not written:
            logging.info(
                "Autofill PRG da DL trovato ma non scritto per job %s -> %s",
                job.get("id"),
                project_root,
            )
            return job

        logging.info(
            "Autocompilato project_base_path dal link DL per job %s -> %s",
            job.get("id"),
            project_root,
        )

        refreshed = self.db.get_job(int(job["id"]))
        return refreshed or job

    def _autofill_psc_from_dl_link(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Se psc_path è vuoto, prova a ricavarlo dal .lnk dentro la DL.

        Regole:
        - non sovrascrive mai un psc_path già presente
        - accetta solo path ricavati come prima sottocartella sotto un base path 'PSC'
        - non scrive se il path risultante non esiste
        - se lo trova, salva il percorso e lo marca READY
        """
        if not job:
            return job

        if str(job.get("psc_path", "") or "").strip():
            return job

        dl_base_path = str(job.get("dl_base_path", "") or "").strip()
        if not dl_base_path:
            return job

        link_info = self.scanner.find_psc_root_from_dl_link(dl_base_path)
        psc_root = str(link_info.get("psc_root", "") or "").strip()
        status = str(link_info.get("status", "") or "").strip()

        if not psc_root:
            logging.info(
                "Autofill PSC da DL non eseguito per job %s | status=%s | dl=%s | lookup=%s | link=%s | target=%s",
                job.get("id"),
                status,
                dl_base_path,
                link_info.get("lookup_folder", ""),
                link_info.get("link_path", ""),
                link_info.get("target_path", ""),
            )
            return job

        if not exists_dir(psc_root):
            logging.warning(
                "PSC root da link DL non esistente per job %s: %s",
                job.get("id"),
                psc_root,
            )
            return job

        written = self.db.autofill_psc_path_if_empty(
            job_id=int(job["id"]),
            psc_path=psc_root,
        )

        if not written:
            logging.info(
                "Autofill PSC da DL trovato ma non scritto per job %s -> %s",
                job.get("id"),
                psc_root,
            )
            return job

        logging.info(
            "Autocompilato psc_path dal link DL per job %s -> %s",
            job.get("id"),
            psc_root,
        )

        refreshed = self.db.get_job(int(job["id"]))
        return refreshed or job

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
        overrides = dict(row.get("scan_overrides") or {})

        row["scan"] = scan_data
        row["scan_overrides"] = overrides
        row["scan_override_fields"] = sorted(overrides.keys())

        row["project_rilievo"] = self._effective_scan_value(
            overrides,
            "project_rilievo",
            scan_data.get("project_rilievo", {}).get("status", ""),
        )
        row["project_enti"] = self._effective_scan_value(
            overrides,
            "project_enti",
            scan_data.get("project_enti", {}).get("status", ""),
        )
        row["project_revision"] = self._effective_scan_value(
            overrides,
            "project_revision",
            scan_data.get("project_revision", {}).get("display", ""),
        )
        row["permessi_revision"] = self._effective_scan_value(
            overrides,
            "permessi_revision",
            scan_data.get("permessi_revision", {}).get("display", ""),
        )
        row["project_tracciamento"] = self._effective_scan_value(
            overrides,
            "project_tracciamento",
            scan_data.get("project_tracciamento", {}).get("status", ""),
        )

        row["permits_display"] = self._compute_permits_display(
            row.get("permits_checklist_json") or []
        )
        row["psc_display"] = self._compute_psc_display(row)

        row["cartesio_prg_display"] = self._effective_scan_value(
            overrides,
            "cartesio_prg_display",
            self._compute_cartesio_prg_display(row, scan_data),
        )
        row["rilievi_dl_display"] = self._effective_scan_value(
            overrides,
            "rilievi_dl_display",
            scan_data.get("rilievi_dl", {}).get("display", "❌"),
        )
        row["cartesio_cos_display"] = self._effective_scan_value(
            overrides,
            "cartesio_cos_display",
            self._compute_cartesio_cos_display(row, scan_data),
        )

        row["revisions_match"] = self._revisions_match(
            row.get("project_revision", ""),
            row.get("permessi_revision", ""),
        )

        return row

    def refresh_row_without_rescan(self, current_row: Dict[str, Any], **updated_fields: Any) -> Dict[str, Any]:
        """
        Aggiorna in memoria una riga già esistente senza fare scansione.
        Serve per modifiche manuali.
        """
        row = dict(current_row)
        row.update(updated_fields)
        return self.apply_derived_fields_from_db(row)

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

        # STEP 1: prova ad autocompilare il path progetto partendo dal link DL.
        job = self._autofill_project_from_dl_link(job)

        # STEP 2: prova ad autocompilare il PSC partendo dal link DL.
        job = self._autofill_psc_from_dl_link(job)

        # STEP 3: ora esegui la scansione vera usando il job eventualmente aggiornato.
        scan_data = self.scanner.scan_job(job)

        permits_display = self._compute_permits_display(job.get("permits_checklist_json") or [])
        cartesio_prg_display = self._compute_cartesio_prg_display_auto(scan_data)
        cartesio_cos_display = self._compute_cartesio_cos_display_auto(scan_data)
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
        """
        Esegue scansione e persistenza di più righe.
        Restituisce le righe finali già pronte per la GUI.
        """
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
            job = self._autofill_project_from_dl_link(job)
            job = self._autofill_psc_from_dl_link(job)

            scan_data = self.scanner.scan_job(job)

            permits_display = self._compute_permits_display(job.get("permits_checklist_json") or [])
            cartesio_prg_display = self._compute_cartesio_prg_display_auto(scan_data)
            cartesio_cos_display = self._compute_cartesio_cos_display_auto(scan_data)
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

    def _effective_scan_value(
        self,
        override_map: Dict[str, str],
        field_key: str,
        auto_value: Any,
    ) -> str:
        if field_key in override_map:
            return str(override_map.get(field_key, "") or "")
        return "" if auto_value is None else str(auto_value)

    def _compute_permits_display(self, checklist: List[Dict[str, Any]]) -> str:
        """
        Regole display Permessi:
        - ❌ = nessun permesso richiesto impostato
        - 🔄 = almeno un permesso richiesto impostato, ma non tutti ottenuti
        - ✅ = tutti i permessi richiesti ottenuti

        Nota:
        normalizza i valori booleani per evitare regressioni dovute a payload
        sporchi provenienti dalla GUI o dal DB (es. "true", "false", 0, 1, None).
        """

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

        if not checklist:
            return "❌"

        required_items = [item for item in checklist if _as_bool(item.get("required"))]

        if not required_items:
            return "❌"

        if all(_as_bool(item.get("obtained")) for item in required_items):
            return "✅"

        return "🔄"

    def _compute_psc_display(self, row: Dict[str, Any]) -> str:
        """
        Regole PSC:
        - ❌ = nessun percorso PSC impostato
        - 🔄 = percorso PSC impostato ma non confermato manualmente
        - ✅ = percorso PSC confermato manualmente come pronto
        """
        psc_path = (row.get("psc_path") or "").strip()
        psc_status = (row.get("psc_status") or "").strip().upper()

        if not psc_path:
            return "❌"

        if psc_status == "READY":
            return "✅"

        return "🔄"

    def _compute_cartesio_prg_display(self, row: Dict[str, Any], scan_data: Dict[str, Any]) -> str:
        prg_manual = (row.get("cartesio_prg_manual_code") or "").strip()
        if prg_manual:
            return prg_manual

        acc_auto = scan_data.get("cartesio_acc", {}).get("code", "")
        prg_auto = scan_data.get("cartesio_prg", {}).get("code", "")
        return acc_auto or prg_auto or scan_data.get("cartesio_prg", {}).get("display", "❌")

    def _compute_cartesio_cos_display(self, row: Dict[str, Any], scan_data: Dict[str, Any]) -> str:
        cos_manual = (row.get("cartesio_cos_manual_code") or "").strip()
        if cos_manual:
            return cos_manual

        acc_auto = scan_data.get("cartesio_acc", {}).get("code", "")
        cos_auto = scan_data.get("cartesio_cos", {}).get("code", "")
        return acc_auto or cos_auto or scan_data.get("cartesio_cos", {}).get("display", "❌")

    def _compute_cartesio_prg_display_auto(self, scan_data: Dict[str, Any]) -> str:
        acc_auto = scan_data.get("cartesio_acc", {}).get("code", "")
        prg_auto = scan_data.get("cartesio_prg", {}).get("code", "")
        return acc_auto or prg_auto or scan_data.get("cartesio_prg", {}).get("display", "❌")

    def _compute_cartesio_cos_display_auto(self, scan_data: Dict[str, Any]) -> str:
        acc_auto = scan_data.get("cartesio_acc", {}).get("code", "")
        cos_auto = scan_data.get("cartesio_cos", {}).get("code", "")
        return acc_auto or cos_auto or scan_data.get("cartesio_cos", {}).get("display", "❌")

    def _revisions_match(self, rev_project: str, rev_permessi: str) -> str:
        if rev_project.isdigit() and rev_permessi.isdigit():
            return "MATCH" if rev_project == rev_permessi else "MISMATCH"
        return "UNKNOWN"
