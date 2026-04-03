# -*- coding: utf-8 -*-
"""Servizi applicativi che combinano DB e scansione."""
from __future__ import annotations

import logging
import shutil
import socket
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from database import DatabaseManager
from scanner import FileSystemScanner

from utils import (
    CARTESIO_ATTACHMENTS_DIR,
    exists_dir,
    folder_name_from_path,
    get_current_user_name,
    infer_project_distretto_anno,
)


class JobService:
    def __init__(self, db: DatabaseManager, scanner: FileSystemScanner) -> None:
        self.db = db
        self.scanner = scanner

    # -------------------------------------------------------------------------
    # LETTURA GUI: SOLO DB
    # -------------------------------------------------------------------------
    def _autofill_project_from_dl_link(self, job: Dict[str, Any]) -> Dict[str, Any]:
        if not job:
            return job
        if self._normalize_project_mode(job.get("project_mode")) != "GTN":
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
        jobs = self.db.fetch_jobs()
        rows = [self.apply_derived_fields_from_db(job) for job in jobs]
        return self._decorate_history_fields(rows)

    def get_row_for_ui(self, job_id: int) -> Optional[Dict[str, Any]]:
        job = self.db.get_job(job_id)
        if not job:
            return None
        row = self.apply_derived_fields_from_db(job)
        decorated_rows = self._decorate_history_fields([row])
        return decorated_rows[0] if decorated_rows else row

    def apply_derived_fields_from_db(self, job: Dict[str, Any]) -> Dict[str, Any]:
        row = dict(job)
        scan_data = row.get("scan") or {}
        overrides = dict(row.get("scan_overrides") or {})
        row["project_mode"] = self._normalize_project_mode(row.get("project_mode"))
        row["permits_mode"] = self._normalize_permits_mode(row.get("permits_mode"))
        row["exception_mode"] = self._normalize_exception_mode(row.get("exception_mode"))

        # Normalizzazione esplicita dei campi manuali/extra per evitare None o
        # stringhe sporche nel passaggio DB -> UI e nella dashboard Cartesio.
        for field_name in (
            "exception_reason",
            "exception_group_code",
            "manual_project_control_path",
            "manual_dl_control_path",
            "project_tracciamento_manual_path",
            "psc_path",
            "psc_status",
            "manual_cartesio_prg_code",
            "manual_cartesio_prg_path",
            "manual_cartesio_cos_code",
            "manual_cartesio_cos_path",
            "manual_cartesio_acc_code",
            "manual_cartesio_acc_path",
        ):
            row[field_name] = str(row.get(field_name, "") or "").strip()

        row["project_name_display"] = self._project_name_display(row)

        row["scan"] = scan_data
        row["scan_overrides"] = overrides
        row["scan_override_fields"] = sorted(overrides.keys())

        if self._is_exception_row(row):
            project_control_path = self._exception_project_control_path(row)
            dl_control_path = self._exception_dl_control_path(row)
            tracciamento_manual_path = self._project_tracciamento_manual_path(row)

            # In modalità eccezione i vecchi override cella non devono prevalere
            # sulla logica manuale strutturata della riga.
            row["scan_override_fields"] = []

            row["project_rilievo"] = (
                str(scan_data.get("project_rilievo", {}).get("status", "") or "❌")
                if project_control_path
                else "-"
            )
            row["project_enti"] = (
                str(scan_data.get("project_enti", {}).get("status", "") or "❌")
                if project_control_path
                else "-"
            )
            row["project_revision"] = (
                str(scan_data.get("project_revision", {}).get("display", "") or "❌")
                if project_control_path
                else "-"
            )

            if project_control_path and self._permits_required(row):
                row["permessi_revision"] = str(
                    scan_data.get("permessi_revision", {}).get("display", "") or "❌"
                )
                row["permits_display"] = self._compute_permits_display(
                    row.get("permits_checklist_json") or []
                )
                row["revisions_match"] = self._revisions_match(
                    row.get("project_revision", ""),
                    row.get("permessi_revision", ""),
                )
            else:
                row["permessi_revision"] = "-"
                row["permits_display"] = "-"
                row["revisions_match"] = "NOT_APPLICABLE"

            if tracciamento_manual_path:
                row["project_tracciamento"] = "✅" if exists_dir(tracciamento_manual_path) else "❌"
            elif project_control_path:
                row["project_tracciamento"] = "❌"
            else:
                row["project_tracciamento"] = "-"

            if project_control_path or str(row.get("psc_path", "") or "").strip():
                row["psc_display"] = self._compute_psc_display(row)
            else:
                row["psc_display"] = "-"

            row["cartesio_prg_display"] = self._manual_cartesio_prg_display(row)
            row["cartesio_acc_prg_display"] = self._manual_cartesio_acc_display(row)
            row["rilievi_dl_display"] = (
                str(scan_data.get("rilievi_dl", {}).get("display", "") or "❌")
                if dl_control_path
                else "-"
            )
            row["cartesio_cos_display"] = self._manual_cartesio_cos_display(row)
            row["cartesio_acc_cos_display"] = self._manual_cartesio_acc_display(row)
            row["cartesio_acc_status"] = self._compute_cartesio_acc_status(row, scan_data)
            return row

        row["project_rilievo"] = self._effective_project_scan_value(
            row,
            overrides,
            "project_rilievo",
            scan_data.get("project_rilievo", {}).get("status", ""),
        )
        row["project_enti"] = self._effective_project_scan_value(
            row,
            overrides,
            "project_enti",
            scan_data.get("project_enti", {}).get("status", ""),
        )
        row["project_revision"] = self._effective_project_scan_value(
            row,
            overrides,
            "project_revision",
            scan_data.get("project_revision", {}).get("display", ""),
        )
        if self._project_controls_enabled(row) and self._permits_required(row):
            row["permessi_revision"] = self._effective_project_scan_value(
                row,
                overrides,
                "permessi_revision",
                scan_data.get("permessi_revision", {}).get("display", ""),
            )
        else:
            row["permessi_revision"] = "-"

        row["project_tracciamento"] = self._compute_project_tracciamento_display(
            row=row,
            override_map=overrides,
            scan_data=scan_data,
        )

        if self._project_controls_enabled(row):
            if self._permits_required(row):
                row["permits_display"] = self._compute_permits_display(
                    row.get("permits_checklist_json") or []
                )
            else:
                row["permits_display"] = "-"
            row["psc_display"] = self._compute_psc_display(row)
        else:
            row["permits_display"] = "-"
            row["psc_display"] = "-"

        cartesio_acc_auto = self._compute_cartesio_acc_display(row, scan_data)
        cartesio_prg_auto = self._compute_cartesio_prg_display(row, scan_data)
        cartesio_cos_auto = self._compute_cartesio_cos_display(row, scan_data)

        row["cartesio_prg_display"] = self._effective_project_scan_value(
            row,
            overrides,
            "cartesio_prg_display",
            cartesio_prg_auto,
        )
        row["cartesio_acc_prg_display"] = cartesio_acc_auto
        row["rilievi_dl_display"] = self._effective_scan_value(
            overrides,
            "rilievi_dl_display",
            scan_data.get("rilievi_dl", {}).get("display", "❌"),
        )
        row["cartesio_cos_display"] = self._effective_scan_value(
            overrides,
            "cartesio_cos_display",
            cartesio_cos_auto,
        )
        row["cartesio_acc_cos_display"] = cartesio_acc_auto
        row["cartesio_acc_status"] = self._compute_cartesio_acc_status(row, scan_data)

        if self._project_controls_enabled(row) and self._permits_required(row):
            row["revisions_match"] = self._revisions_match(
                row.get("project_revision", ""),
                row.get("permessi_revision", ""),
            )
        else:
            row["revisions_match"] = "NOT_APPLICABLE"

        return row

    def _decorate_history_fields(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not rows:
            return []

        user_name = get_current_user_name()
        job_ids = [int(row["id"]) for row in rows if row.get("id") is not None]

        latest_map = self.db.get_latest_audit_event_map(job_ids)
        latest_alert_map = self.db.get_latest_alert_event_map(job_ids, user_name)
        seen_map = self.db.get_user_seen_event_map(job_ids, user_name)

        decorated: List[Dict[str, Any]] = []
        for row in rows:
            job_id = int(row["id"])

            latest = latest_map.get(job_id, {})
            latest_alert = latest_alert_map.get(job_id, {})
            seen_event_id = int(seen_map.get(job_id, 0) or 0)

            latest_event_id = int(latest.get("event_id") or 0)
            latest_alert_event_id = int(latest_alert.get("event_id") or 0)

            row = dict(row)
            row["history_user_name"] = user_name
            row["audit_latest_event_id"] = latest_event_id
            row["audit_latest_event_ts"] = str(latest.get("event_ts") or "")
            row["audit_latest_source_kind"] = str(latest.get("source_kind") or "")
            row["audit_latest_summary"] = str(latest.get("summary") or "")

            row["history_alert_event_id"] = latest_alert_event_id
            row["history_alert_event_ts"] = str(latest_alert.get("event_ts") or "")
            row["history_alert_source_kind"] = str(latest_alert.get("source_kind") or "")
            row["history_alert_summary"] = str(latest_alert.get("summary") or "")
            row["history_alert_initiated_by"] = str(latest_alert.get("initiated_by") or "")

            row["audit_last_seen_event_id"] = seen_event_id

            has_pending_alert = latest_alert_event_id > seen_event_id
            if self._is_exception_row(row):
                row["history_alert_display"] = "E!" if has_pending_alert else "E"
            else:
                row["history_alert_display"] = "!" if has_pending_alert else ""

            decorated.append(row)

        return decorated

    def refresh_row_without_rescan(self, current_row: Dict[str, Any], **updated_fields: Any) -> Dict[str, Any]:
        row = dict(current_row)
        row.update(updated_fields)
        row = self.apply_derived_fields_from_db(row)
        decorated_rows = self._decorate_history_fields([row])
        return decorated_rows[0] if decorated_rows else row

    def _normalize_cartesio_dashboard_checklist_items(self, items: Any) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []

        if not isinstance(items, list):
            return normalized

        for item in items:
            if not isinstance(item, dict):
                continue

            text_value = str(item.get("text") or "").strip()
            if not text_value:
                continue

            normalized.append(
                {
                    "text": text_value,
                    "done": bool(item.get("done")),
                    "note": str(item.get("note") or "").strip(),
                }
            )

        return normalized

    def _compute_cartesio_checklist_display(self, items: Any) -> str:
        normalized_items = self._normalize_cartesio_dashboard_checklist_items(items)
        total = len(normalized_items)

        if total <= 0:
            return "-"

        completed = sum(1 for item in normalized_items if bool(item.get("done")))

        if completed >= total:
            return "✅"

        return f"{completed}/{total}"        

    def load_cartesio_rows_for_ui(self, scope: str) -> List[Dict[str, Any]]:
        """
        Carica le righe della dashboard Cartesio riusando gli stessi valori derivati
        della tab Lavori per evitare discrepanze su:
        - cartesio_prg_display / cartesio_cos_display
        - project_name_display
        - stati/override futuri

        Nota architetturale:
        la query dashboard restituisce solo i dati Cartesio-specifici; i campi
        "display" derivati vengono invece recuperati dalla stessa pipeline usata
        dalla tab Lavori (fetch_jobs + apply_derived_fields_from_db).
        """
        normalized_scope = self._normalize_cartesio_scope(scope)
        rows = self.db.fetch_cartesio_dashboard_rows(normalized_scope)

        # Costruisce una mappa job_id -> riga derivata completa, usando la stessa
        # logica della tab Lavori ma senza passare dalla parte audit/storico.
        derived_jobs_by_id: Dict[int, Dict[str, Any]] = {}

        for job in self.db.fetch_jobs():
            try:
                derived = self.apply_derived_fields_from_db(job)
                job_id = int(derived.get("id") or 0)
                if job_id > 0:
                    derived_jobs_by_id[job_id] = derived
            except Exception:
                logging.exception(
                    "Errore derivazione dati per dashboard Cartesio | job_id=%s",
                    job.get("id"),
                )

        decorated: List[Dict[str, Any]] = []

        for row in rows:
            item = dict(row)
            job_id = int(item.get("job_id") or 0)
            derived_job = dict(derived_jobs_by_id.get(job_id) or {})

            normalized_checklist = self._normalize_cartesio_dashboard_checklist_items(
                item.get("checklist_json")
            )

            # Allinea i campi condivisi con la tab Lavori
            item["project_mode"] = str(
                derived_job.get("project_mode") or item.get("project_mode") or ""
            ).strip()

            item["project_base_path"] = str(
                derived_job.get("project_base_path") or item.get("project_base_path") or ""
            ).strip()

            item["dl_base_path"] = str(
                derived_job.get("dl_base_path") or item.get("dl_base_path") or ""
            ).strip()

            # Propaga anche i campi manuali della modalità eccezione, così la
            # dashboard Cartesio e i doppi click possono usare i path corretti.
            for field_name in (
                "exception_mode",
                "exception_reason",
                "exception_group_code",
                "manual_project_control_path",
                "manual_dl_control_path",
                "project_tracciamento_manual_path",
                "psc_path",
                "psc_status",
                "manual_cartesio_prg_code",
                "manual_cartesio_prg_path",
                "manual_cartesio_cos_code",
                "manual_cartesio_cos_path",
                "manual_cartesio_acc_code",
                "manual_cartesio_acc_path",
            ):
                item[field_name] = str(
                    derived_job.get(field_name) or item.get(field_name) or ""
                ).strip()

            item["project_name_display"] = str(
                derived_job.get("project_name_display")
                or self._project_name_display({**item, **derived_job})
                or item.get("project_name")
                or ""
            ).strip() or "-"

            item["cartesio_prg_display"] = str(
                derived_job.get("cartesio_prg_display") or ""
            ).strip() or "-"

            item["cartesio_cos_display"] = str(
                derived_job.get("cartesio_cos_display") or ""
            ).strip() or "-"

            item["cartesio_acc_display"] = str(
                self._compute_cartesio_acc_display(derived_job, derived_job.get("scan") or {})
            ).strip() or "-"

            # Questi servono anche alla colorazione del model Cartesio
            item["cartesio_prg_status"] = str(
                derived_job.get("cartesio_prg_status") or ""
            ).strip()

            item["cartesio_cos_status"] = str(
                derived_job.get("cartesio_cos_status") or ""
            ).strip()

            item["cartesio_acc_status"] = str(
                item.get("entry_status") or ""
            ).strip()

            item["latest_note_title"] = str(item.get("latest_note_title") or "").strip() or "-"
            item["referente"] = str(item.get("referente") or "").strip()

            item["display_last_activity"] = (
                self._max_iso_timestamp_text(
                    item.get("latest_note_updated_at"),
                    item.get("last_activity_at"),
                )
                or "-"
            )

            item["checklist_done"] = sum(
                1 for checklist_item in normalized_checklist if bool(checklist_item.get("done"))
            )
            item["checklist_total"] = len(normalized_checklist)
            item["checklist_display"] = self._compute_cartesio_checklist_display(
                normalized_checklist
            )

            decorated.append(item)

        return decorated

    def get_cartesio_bundle(self, job_id: int, scope: str) -> Dict[str, Any]:
        bundle = self.db.get_cartesio_bundle(job_id, self._normalize_cartesio_scope(scope))
        job = dict(bundle.get("job") or {})
        if job:
            job["project_mode"] = self._normalize_project_mode(job.get("project_mode"))
            job["project_name_display"] = self._project_name_display(job)
        bundle["job"] = job
        return bundle

    def get_cartesio_activation_warning(self, job_id: int, scope: str) -> str:
        normalized_scope = self._normalize_cartesio_scope(scope)
        if normalized_scope != "COS":
            return ""

        prg_entry = self.db.get_cartesio_entry(job_id, "PRG") or {}
        prg_status = str(prg_entry.get("status") or "").strip().upper()
        if prg_entry and prg_status and prg_status != "APPROVATO":
            return (
                "Stai attivando il lato COS ma l'entry PRG non risulta APPROVATO.\n"
                f"Stato PRG attuale: {prg_entry.get('status', '')}"
            )
        return ""

    def save_cartesio_entry(
        self,
        job_id: int,
        scope: str,
        referente: str,
        status: str,
        is_active: bool,
    ) -> Dict[str, Any]:
        normalized_scope = self._normalize_cartesio_scope(scope)
        bundle = self.db.save_cartesio_entry(
            job_id=job_id,
            scope=normalized_scope,
            referente=referente,
            status=status,
            is_active=is_active,
        )
        bundle["activation_warning"] = self.get_cartesio_activation_warning(job_id, normalized_scope) if is_active else ""
        return bundle

    def save_cartesio_checklist(
        self,
        job_id: int,
        scope: str,
        checklist_json: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        return self.db.save_cartesio_entry_checklist(
            job_id=job_id,
            scope=self._normalize_cartesio_scope(scope),
            checklist_json=checklist_json,
        )

    def add_cartesio_thread(self, job_id: int, scope: str, title: str) -> Dict[str, Any]:
        return self.db.add_cartesio_thread(job_id, self._normalize_cartesio_scope(scope), title)

    def set_cartesio_thread_status(self, thread_id: int, status: str) -> None:
        self.db.set_cartesio_thread_status(thread_id, status)

    def delete_cartesio_thread(self, thread_id: int) -> Dict[str, Any]:
        """
        Elimina un thread Cartesio e, per richiesta funzionale, elimina anche:
        - tutte le note collegate al thread
        - tutti gli allegati collegati a quelle note

        Il DB ritorna un payload con i metadati degli allegati eliminati,
        così la GUI può rimuovere anche i file fisici da disco.
        """
        return self.db.delete_cartesio_thread(thread_id)

    def add_cartesio_note(
        self,
        job_id: int,
        scope: str,
        title: str,
        body: str,
        checklist_json: Optional[List[Dict[str, Any]]] = None,
        thread_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        return self.db.add_cartesio_note(
            job_id=job_id,
            scope=self._normalize_cartesio_scope(scope),
            title=title,
            body=body,
            checklist_json=checklist_json,
            thread_id=thread_id,
        )

    def update_cartesio_note(
        self,
        note_id: int,
        title: str,
        body: str,
        checklist_json: Optional[List[Dict[str, Any]]] = None,
        thread_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        return self.db.update_cartesio_note(
            note_id=note_id,
            title=title,
            body=body,
            checklist_json=checklist_json,
            thread_id=thread_id,
        )

    def delete_cartesio_note(self, note_id: int) -> None:
        self.db.delete_cartesio_note(note_id)

    def add_cartesio_attachment(
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
        return self.db.add_cartesio_note_attachment(
            note_id=note_id,
            attachment_kind=attachment_kind,
            stored_rel_path=stored_rel_path,
            display_name=display_name,
            subject=subject,
            sender=sender,
            received_at=received_at,
            meta_json=meta_json,
        )

    def remove_cartesio_attachment(self, attachment_id: int) -> Optional[Dict[str, Any]]:
        return self.db.remove_cartesio_attachment(attachment_id)

    def delete_job(self, job_id: int) -> None:
        attachment_dir = CARTESIO_ATTACHMENTS_DIR / str(int(job_id))

        self.db.delete_job(job_id)

        if attachment_dir.exists():
            try:
                shutil.rmtree(attachment_dir)
            except Exception:
                logging.exception(
                    "Lavoro %s eliminato dal DB, ma non sono riuscito a pulire la cartella allegati: %s",
                    job_id,
                    attachment_dir,
                )

    # -------------------------------------------------------------------------
    # SCANSIONE E PERSISTENZA
    # -------------------------------------------------------------------------

    def scan_and_persist_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        job = self.db.get_job(job_id)
        if not job:
            return None

        if not self._is_exception_row(job):
            job = self._autofill_project_from_dl_link(job)
            job = self._autofill_psc_from_dl_link(job)

        scan_data = self._scan_job_respecting_project_mode(job)
        cached_displays = self._build_cached_scan_displays(job, scan_data)

        self.db.save_scan_cache(
            job_id=job_id,
            scan_data=scan_data,
            permits_display=cached_displays["permits_display"],
            cartesio_prg_display=cached_displays["cartesio_prg_display"],
            rilievi_dl_display=cached_displays["rilievi_dl_display"],
            cartesio_cos_display=cached_displays["cartesio_cos_display"],
            revisions_match=cached_displays["revisions_match"],
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
        jobs = self.db.fetch_jobs()

        for job in jobs:
            if not self._is_exception_row(job):
                job = self._autofill_project_from_dl_link(job)
                job = self._autofill_psc_from_dl_link(job)

            scan_data = self._scan_job_respecting_project_mode(job)
            cached_displays = self._build_cached_scan_displays(job, scan_data)

            self.db.save_scan_cache(
                job_id=job["id"],
                scan_data=scan_data,
                permits_display=cached_displays["permits_display"],
                cartesio_prg_display=cached_displays["cartesio_prg_display"],
                rilievi_dl_display=cached_displays["rilievi_dl_display"],
                cartesio_cos_display=cached_displays["cartesio_cos_display"],
                revisions_match=cached_displays["revisions_match"],
                commit=False,
            )

        self.db.set_last_global_scan_date_today(commit=False)
        self.db._commit()

        return self.load_jobs_for_ui()

    def startup_load(self) -> List[Dict[str, Any]]:
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

    def _max_iso_timestamp_text(self, *values: Any) -> str:
        best_dt: Optional[datetime] = None

        for value in values:
            text = str(value or "").strip()
            if not text:
                continue

            try:
                candidate_dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
                    try:
                        candidate_dt = datetime.strptime(text[:10], "%Y-%m-%d")
                    except ValueError:
                        continue
                else:
                    continue

            if best_dt is None or candidate_dt > best_dt:
                best_dt = candidate_dt

        return best_dt.strftime("%Y-%m-%d") if best_dt else ""

    def _normalize_cartesio_scope(self, value: Any) -> str:
        scope = str(value or "").strip().upper()
        if scope in {"PRG", "COS", "ACC", "NONE"}:
            return scope
        return "NONE"

    def _normalize_project_mode(self, value: Any) -> str:
        mode = str(value or "").strip().upper()
        if mode in {"GTN", "ALTRA_DITTA", "PROGETTO_NON_PREVISTO"}:
            return mode
        return "GTN"

    def _normalize_permits_mode(self, value: Any) -> str:
        mode = str(value or "").strip().upper()
        if mode in {"REQUIRED", "NOT_REQUIRED"}:
            return mode
        return "REQUIRED"


    def _normalize_exception_mode(self, value: Any) -> str:
        mode = str(value or "").strip().upper()
        if mode == "MANUAL":
            return "MANUAL"
        return "STANDARD"

    def _is_exception_row(self, row: Dict[str, Any]) -> bool:
        return self._normalize_exception_mode(row.get("exception_mode")) == "MANUAL"

    def _exception_project_control_path(self, row: Dict[str, Any]) -> str:
        return str(row.get("manual_project_control_path", "") or "").strip()

    def _exception_dl_control_path(self, row: Dict[str, Any]) -> str:
        return str(row.get("manual_dl_control_path", "") or "").strip()

    def _manual_cartesio_prg_display(self, row: Dict[str, Any]) -> str:
        code = str(row.get("manual_cartesio_prg_code", "") or "").strip().upper()
        return code or "❌"

    def _manual_cartesio_cos_display(self, row: Dict[str, Any]) -> str:
        code = str(row.get("manual_cartesio_cos_code", "") or "").strip().upper()
        return code or "❌"

    def _manual_cartesio_acc_display(self, row: Dict[str, Any]) -> str:
        code = str(row.get("manual_cartesio_acc_code", "") or "").strip().upper()
        return code or "❌"

    def _permits_required(self, row: Dict[str, Any]) -> bool:
        return self._normalize_permits_mode(row.get("permits_mode")) == "REQUIRED"

    def _has_project_base_path(self, row: Dict[str, Any]) -> bool:
        return bool(str(row.get("project_base_path", "") or "").strip())

    def _project_controls_enabled(self, row: Dict[str, Any]) -> bool:
        return self._normalize_project_mode(row.get("project_mode")) == "GTN" and self._has_project_base_path(row)

    def _project_name_display(self, row: Dict[str, Any]) -> str:
        mode = self._normalize_project_mode(row.get("project_mode"))
        if mode == "ALTRA_DITTA":
            return "ALTRA DITTA"
        if mode == "PROGETTO_NON_PREVISTO":
            return "PROGETTO NON PREVISTO"
        return str(row.get("project_name", "") or "")

    def _project_tracciamento_manual_path(self, row: Dict[str, Any]) -> str:
        return str(row.get("project_tracciamento_manual_path", "") or "").strip()

    def _compute_project_tracciamento_display(
        self,
        *,
        row: Dict[str, Any],
        override_map: Dict[str, str],
        scan_data: Dict[str, Any],
    ) -> str:
        """
        Regola speciale richiesta:

        - GTN:
            usa la logica automatica classica basata sulla scansione
        - ALTRA_DITTA:
            non esiste scansione standard del tracciamento
            -> se c'è link manuale mostra ✅
            -> se non c'è link manuale mostra ❌
        - PROGETTO_NON_PREVISTO:
            il campo resta non applicabile -> -
        - in ogni caso l'override manuale testuale ha priorità massima
        """
        if "project_tracciamento" in override_map:
            return str(override_map.get("project_tracciamento", "") or "")

        mode = self._normalize_project_mode(row.get("project_mode"))

        if mode == "ALTRA_DITTA":
            return "✅" if self._project_tracciamento_manual_path(row) else "❌"

        if mode != "GTN":
            return "-"

        if not self._has_project_base_path(row):
            return "-"

        return str(scan_data.get("project_tracciamento", {}).get("status", "") or "")      

    def _effective_project_scan_value(
        self,
        row: Dict[str, Any],
        override_map: Dict[str, str],
        field_key: str,
        auto_value: Any,
    ) -> str:
        if self._normalize_project_mode(row.get("project_mode")) != "GTN":
            return "-"

        if field_key in override_map:
            return str(override_map.get(field_key, "") or "")

        if not self._has_project_base_path(row):
            return "-"

        return "" if auto_value is None else str(auto_value)

    def _compute_permits_display(self, checklist: List[Dict[str, Any]]) -> str:
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
        psc_path = (row.get("psc_path") or "").strip()
        psc_status = (row.get("psc_status") or "").strip().upper()

        if not psc_path:
            return "❌"

        if psc_status == "READY":
            return "✅"

        return "🔄"

    def _compute_cartesio_prg_display(self, row: Dict[str, Any], scan_data: Dict[str, Any]) -> str:
        if self._normalize_project_mode(row.get("project_mode")) != "GTN":
            return "-"

        if not self._has_project_base_path(row):
            return "-"

        manual_acc_code = str(row.get("manual_cartesio_acc_code", "") or "").strip().upper()
        if manual_acc_code:
            return "-"

        manual_prg_code = str(row.get("manual_cartesio_prg_code", "") or "").strip().upper()
        if manual_prg_code:
            return manual_prg_code

        acc_data = scan_data.get("cartesio_acc", {}) or {}
        acc_code = str(acc_data.get("code") or "").strip()
        if acc_code:
            return "-"

        prg_data = scan_data.get("cartesio_prg", {}) or {}
        code = str(prg_data.get("code") or "").strip()
        if code:
            return code

        return str(prg_data.get("display") or "❌")

    def _compute_cartesio_cos_display(self, row: Dict[str, Any], scan_data: Dict[str, Any]) -> str:
        manual_acc_code = str(row.get("manual_cartesio_acc_code", "") or "").strip().upper()
        if manual_acc_code:
            return "-"

        manual_cos_code = str(row.get("manual_cartesio_cos_code", "") or "").strip().upper()
        if manual_cos_code:
            return manual_cos_code

        acc_data = scan_data.get("cartesio_acc", {}) or {}
        acc_code = str(acc_data.get("code") or "").strip()
        if acc_code:
            return "-"

        cos_data = scan_data.get("cartesio_cos", {}) or {}
        code = str(cos_data.get("code") or "").strip()
        if code:
            return code

        return str(cos_data.get("display") or "❌")

    def _compute_cartesio_acc_display(self, row: Dict[str, Any], scan_data: Dict[str, Any]) -> str:
        manual_code = str(row.get("manual_cartesio_acc_code", "") or "").strip().upper()
        if manual_code:
            return manual_code

        acc_data = scan_data.get("cartesio_acc", {}) or {}
        code = str(acc_data.get("code") or "").strip()
        if code:
            return code

        return "-"

    def _compute_cartesio_acc_status(self, row: Dict[str, Any], scan_data: Dict[str, Any]) -> str:
        manual_code = str(row.get("manual_cartesio_acc_code", "") or "").strip().upper()
        if manual_code:
            return manual_code

        acc_data = scan_data.get("cartesio_acc", {}) or {}
        code = str(acc_data.get("code") or "").strip()
        if code:
            return code

        return str(acc_data.get("display") or "")

    def _compute_cartesio_prg_display_auto(self, scan_data: Dict[str, Any]) -> str:
        acc_auto = str(scan_data.get("cartesio_acc", {}).get("code", "") or "").strip()
        if acc_auto:
            return "-"

        prg_auto = str(scan_data.get("cartesio_prg", {}).get("code", "") or "").strip()
        if prg_auto:
            return prg_auto

        return str(scan_data.get("cartesio_prg", {}).get("display", "❌") or "❌")

    def _scan_job_respecting_project_mode(self, job: Dict[str, Any]) -> Dict[str, Any]:
        if self._is_exception_row(job):
            project_control_path = self._exception_project_control_path(job)
            dl_control_path = self._exception_dl_control_path(job)
            tracciamento_path = self._project_tracciamento_manual_path(job)

            project_tracciamento = {}
            if tracciamento_path:
                project_tracciamento = {
                    "status": "✅" if exists_dir(tracciamento_path) else "❌",
                    "path": tracciamento_path,
                }

            return {
                "project_rilievo": (
                    self.scanner.scan_project_rilievo(project_control_path)
                    if project_control_path
                    else {}
                ),
                "project_enti": (
                    self.scanner.scan_project_enti(project_control_path)
                    if project_control_path
                    else {}
                ),
                "project_revision": (
                    self.scanner.scan_project_revision(project_control_path)
                    if project_control_path
                    else {}
                ),
                "permessi_revision": (
                    self.scanner.scan_permessi_revision(project_control_path)
                    if project_control_path and self._permits_required(job)
                    else {}
                ),
                "project_tracciamento": project_tracciamento,
                "cartesio_prg": {},
                "rilievi_dl": (
                    self.scanner.scan_rilievi_dl(dl_control_path)
                    if dl_control_path
                    else {}
                ),
                "cartesio_cos": {},
                "cartesio_acc": {},
            }

        mode = self._normalize_project_mode(job.get("project_mode"))
        project_base_path = job.get("project_base_path", "")
        dl_base_path = job.get("dl_base_path", "")

        if mode != "GTN":
            return {
                "project_rilievo": {},
                "project_enti": {},
                "project_revision": {},
                "permessi_revision": {},
                "project_tracciamento": {},
                "cartesio_prg": {},
                "rilievi_dl": self.scanner.scan_rilievi_dl(dl_base_path),
                "cartesio_cos": self.scanner.scan_cartesio_cos(dl_base_path),
                "cartesio_acc": self.scanner.scan_cartesio_acc(
                    project_base_path="",
                    dl_base_path=dl_base_path,
                ),
            }

        scan_data = {
            "project_rilievo": self.scanner.scan_project_rilievo(project_base_path),
            "project_enti": self.scanner.scan_project_enti(project_base_path),
            "project_revision": self.scanner.scan_project_revision(project_base_path),
            "permessi_revision": {},
            "project_tracciamento": self.scanner.scan_project_tracciamento(project_base_path),
            "cartesio_prg": self.scanner.scan_cartesio_prg(project_base_path),
            "rilievi_dl": self.scanner.scan_rilievi_dl(dl_base_path),
            "cartesio_cos": self.scanner.scan_cartesio_cos(dl_base_path),
            "cartesio_acc": self.scanner.scan_cartesio_acc(
                project_base_path=project_base_path,
                dl_base_path=dl_base_path,
            ),
        }

        if self._permits_required(job):
            scan_data["permessi_revision"] = self.scanner.scan_permessi_revision(project_base_path)

        return scan_data

    def _build_cached_scan_displays(self, job: Dict[str, Any], scan_data: Dict[str, Any]) -> Dict[str, str]:
        mode = self._normalize_project_mode(job.get("project_mode"))
        permits_mode = self._normalize_permits_mode(job.get("permits_mode"))
        row = dict(job)
        row["project_mode"] = mode
        row["permits_mode"] = permits_mode
        row["exception_mode"] = self._normalize_exception_mode(job.get("exception_mode"))

        if self._is_exception_row(row):
            project_control_path = self._exception_project_control_path(row)
            dl_control_path = self._exception_dl_control_path(row)

            project_revision = (
                str(scan_data.get("project_revision", {}).get("display", "") or "❌")
                if project_control_path
                else "-"
            )
            permessi_revision = (
                str(scan_data.get("permessi_revision", {}).get("display", "") or "❌")
                if project_control_path and self._permits_required(row)
                else "-"
            )

            revisions_match = (
                self._revisions_match(project_revision, permessi_revision)
                if project_control_path and self._permits_required(row)
                else "NOT_APPLICABLE"
            )

            return {
                "permits_display": (
                    self._compute_permits_display(job.get("permits_checklist_json") or [])
                    if project_control_path and self._permits_required(row)
                    else "-"
                ),
                "cartesio_prg_display": self._manual_cartesio_prg_display(row),
                "rilievi_dl_display": (
                    str(scan_data.get("rilievi_dl", {}).get("display", "") or "❌")
                    if dl_control_path
                    else "-"
                ),
                "cartesio_cos_display": self._manual_cartesio_cos_display(row),
                "revisions_match": revisions_match,
            }

        if self._project_controls_enabled(row):
            p_rev = scan_data.get("project_revision", {}).get("display", "")

            if self._permits_required(row):
                permits_display = self._compute_permits_display(job.get("permits_checklist_json") or [])
                q_rev = scan_data.get("permessi_revision", {}).get("display", "")
                revisions_match = self._revisions_match(p_rev, q_rev)
            else:
                permits_display = "-"
                revisions_match = "NOT_APPLICABLE"

            cartesio_prg_display = self._compute_cartesio_prg_display_auto(scan_data)
        else:
            permits_display = "-"
            revisions_match = "NOT_APPLICABLE"
            cartesio_prg_display = "-"

        return {
            "permits_display": permits_display,
            "cartesio_prg_display": cartesio_prg_display,
            "rilievi_dl_display": scan_data.get("rilievi_dl", {}).get("display", "❌"),
            "cartesio_cos_display": self._compute_cartesio_cos_display_auto(scan_data),
            "revisions_match": revisions_match,
        }

    def _compute_cartesio_cos_display_auto(self, scan_data: Dict[str, Any]) -> str:
        acc_auto = str(scan_data.get("cartesio_acc", {}).get("code", "") or "").strip()
        if acc_auto:
            return "-"

        cos_auto = str(scan_data.get("cartesio_cos", {}).get("code", "") or "").strip()
        if cos_auto:
            return cos_auto

        return str(scan_data.get("cartesio_cos", {}).get("display", "❌") or "❌")

    def _revisions_match(self, rev_project: str, rev_permessi: str) -> str:
        rev_project = str(rev_project or "").strip()
        rev_permessi = str(rev_permessi or "").strip()

        if rev_project == "-" and rev_permessi == "-":
            return "NOT_APPLICABLE"

        if rev_project.isdigit() and rev_permessi.isdigit():
            return "MATCH" if rev_project == rev_permessi else "MISMATCH"

        return "UNKNOWN"