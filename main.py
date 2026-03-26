# -*- coding: utf-8 -*-
"""
Situazione Lavori - V1
----------------------
Base PySide6 + SQLite + scansione filesystem persistita su DB.

Note:
- Una riga rappresenta un lavoro.
- Ogni lavoro può avere sezione Progetto, sezione DL, oppure entrambe.
- La GUI legge solo dati dal DB.
- La scansione filesystem serve solo ad aggiornare il DB/cache condiviso.
"""
from __future__ import annotations

import logging
import sys
import traceback
from pathlib import Path

from PySide6.QtCore import QPoint, Qt
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QFileDialog,
    QHeaderView,
    QLabel,
    QLineEdit,
    QInputDialog,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QStatusBar,
    QStyledItemDelegate,
    QTableView,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from database import DatabaseManager
from dialogs.job_dialog import JobDialog
from dialogs.permits_dialog import PermitsDialog
from dialogs.status_dialog import StatusDialog
from dialogs.todo_dialog import TodoDialog
from models import JobsTableModel
from scanner import FileSystemScanner
from services import JobService
from utils import (
    CARTESIO_COS_STATES,
    CARTESIO_PRG_STATES,
    RILIEVI_DL_STATES,
    folder_name_from_path,
    infer_dl_distretto_anno,
    infer_project_distretto_anno,
    open_in_explorer,
    setup_logging,
)


class PreserveForegroundDelegate(QStyledItemDelegate):
    """Mantiene il colore del testo della cella anche quando la riga è selezionata."""

    def initStyleOption(self, option, index):
        super().initStyleOption(option, index)

        brush = index.data(Qt.ForegroundRole)

        if brush is not None:
            option.palette.setBrush(QPalette.Text, brush)
            option.palette.setBrush(QPalette.WindowText, brush)
            option.palette.setBrush(QPalette.HighlightedText, brush)
        else:
            normal_brush = option.palette.brush(QPalette.Text)
            option.palette.setBrush(QPalette.HighlightedText, normal_brush)


class MainWindow(QMainWindow):
    SCAN_OVERRIDE_COLUMN_MAP = {
        2: "project_rilievo",
        3: "project_enti",
        4: "project_revision",
        5: "permessi_revision",
        7: "project_tracciamento",
        8: "cartesio_prg_display",
        12: "rilievi_dl_display",
        13: "cartesio_cos_display",
    }

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Situazione Lavori - V1")
        self.resize(1700, 900)

        self.db = DatabaseManager()
        self.scanner = FileSystemScanner()
        self.service = JobService(self.db, self.scanner)
        self.model = JobsTableModel()
        self.all_rows = []

        self._build_ui()
        self._startup_load()

    # -------------------------------------------------------------------------
    # HELPERS
    # -------------------------------------------------------------------------    
    def _reapply_current_sort(self):
        header = self.table.horizontalHeader()
        section = header.sortIndicatorSection()
        order = header.sortIndicatorOrder()

        if section >= 0:
            self.model.sort(section, order)
    # -------------------------------------------------------------------------
    # UI
    # -------------------------------------------------------------------------

    def _build_ui(self):
        central = QWidget()
        root = QVBoxLayout(central)
        self.setCentralWidget(central)

        toolbar = QToolBar("Azioni")
        toolbar.setMovable(False)
        self.addToolBar(toolbar)

        btn_new = QPushButton("Nuovo lavoro")
        btn_new.clicked.connect(self.add_job)
        toolbar.addWidget(btn_new)

        btn_import = QPushButton("Importa cartelle")
        btn_import.clicked.connect(self.import_jobs_from_parent)
        toolbar.addWidget(btn_import)

        btn_edit = QPushButton("Modifica")
        btn_edit.clicked.connect(self.edit_selected_job)
        toolbar.addWidget(btn_edit)

        btn_delete = QPushButton("Elimina")
        btn_delete.clicked.connect(self.delete_selected_jobs)
        toolbar.addWidget(btn_delete)

        toolbar.addSeparator()

        btn_refresh = QPushButton("Aggiorna tutto")
        btn_refresh.clicked.connect(self.refresh_data)
        toolbar.addWidget(btn_refresh)

        btn_refresh_selected = QPushButton("Aggiorna selezionati")
        btn_refresh_selected.clicked.connect(self.refresh_selected)
        toolbar.addWidget(btn_refresh_selected)

        toolbar.addSeparator()
        toolbar.addWidget(QLabel("Filtro"))
        self.edt_filter = QLineEdit()
        self.edt_filter.setPlaceholderText("Cerca per nome progetto, nome DL, distretto, path...")
        self.edt_filter.textChanged.connect(self.apply_filter)
        toolbar.addWidget(self.edt_filter)

        self.table = QTableView()

        palette = self.table.palette()
        sel = QColor("#cfe8ff")  # azzurro chiaro
        palette.setColor(QPalette.Active, QPalette.Highlight, sel)
        palette.setColor(QPalette.Inactive, QPalette.Highlight, sel)
        self.table.setPalette(palette)

        self.table.setModel(self.model)
        self.table.setItemDelegate(PreserveForegroundDelegate(self.table))
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.doubleClicked.connect(self.handle_double_click)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.open_context_menu)

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(10, QHeaderView.Stretch)

        root.addWidget(self.table)
        self.setStatusBar(QStatusBar())

    # -------------------------------------------------------------------------
    # STARTUP
    # -------------------------------------------------------------------------

    def _startup_load(self):
        try:
            self.statusBar().showMessage("Caricamento iniziale dati...")
            self.all_rows = self.service.startup_load()
            self.apply_filter()
            self.statusBar().showMessage(f"Lavori caricati: {len(self.all_rows)}", 5000)
        except Exception as exc:
            logging.exception("Errore _startup_load")
            QMessageBox.critical(self, "Errore", f"Errore durante caricamento iniziale:\n{exc}")

    # -------------------------------------------------------------------------
    # SELEZIONE / CACHE
    # -------------------------------------------------------------------------

    def selected_jobs(self):
        """
        Restituisce la lista dei lavori selezionati nella tabella.
        Funziona sulle righe attualmente visibili nel model.
        """
        selection_model = self.table.selectionModel()
        if not selection_model:
            return []

        selected_indexes = selection_model.selectedRows()
        if not selected_indexes:
            return []

        jobs = []
        seen_ids = set()

        for index in selected_indexes:
            if not index.isValid():
                continue

            job = self.model.get_row(index.row())
            if not job:
                continue

            job_id = job.get("id")
            if job_id in seen_ids:
                continue

            seen_ids.add(job_id)
            jobs.append(job)

        return jobs

    def current_job(self):
        index = self.table.currentIndex()
        if not index.isValid():
            return None
        return self.model.get_row(index.row())

    def _replace_row_in_cache(self, updated_row):
        updated_id = updated_row.get("id")
        for i, row in enumerate(self.all_rows):
            if row.get("id") == updated_id:
                self.all_rows[i] = updated_row
                return True
        return False

    def _apply_local_row_update(self, updated_row, force_refilter: bool = False):
        if not updated_row:
            return

        self._replace_row_in_cache(updated_row)

        filter_active = bool(self.edt_filter.text().strip())
        if force_refilter or filter_active:
            self.apply_filter()
            return

        updated = self.model.update_row_by_id(updated_row["id"], updated_row)
        if not updated:
            self.apply_filter()
            return

        self._reapply_current_sort()

    def _scan_override_field_for_column(self, column: int) -> str | None:
        return self.SCAN_OVERRIDE_COLUMN_MAP.get(column)

    def _job_has_scan_override(self, job, field_key: str) -> bool:
        override_fields = set(job.get("scan_override_fields") or [])
        return field_key in override_fields

    # -------------------------------------------------------------------------
    # REFRESH
    # -------------------------------------------------------------------------

    def refresh_data(self):
        """
        Scan totale + salvataggio DB + ricarica GUI dal DB.
        """
        try:
            self.statusBar().showMessage("Aggiornamento dati completo in corso...")
            self.all_rows = self.service.scan_all_and_persist()
            self.apply_filter()
            self.statusBar().showMessage(f"Lavori caricati: {len(self.all_rows)}", 5000)
        except Exception as exc:
            logging.exception("Errore refresh_data")
            QMessageBox.critical(self, "Errore", f"Errore durante refresh dati:\n{exc}")

    def refresh_selected(self):
        """
        Scan dei lavori selezionati + salvataggio DB + update locale GUI.
        Supporta multiselezione.
        """
        jobs = self.selected_jobs()

        if not jobs:
            QMessageBox.information(
                self,
                "Nessuna riga",
                "Seleziona almeno un lavoro da aggiornare.",
            )
            return

        job_ids = [job["id"] for job in jobs]

        try:
            self.statusBar().showMessage("Aggiornamento righe selezionate in corso...")
            updated_rows = self.service.scan_and_persist_jobs(job_ids)

            if not updated_rows:
                raise RuntimeError("Nessuna riga aggiornata.")

            force_refilter = bool(self.edt_filter.text().strip())
            if force_refilter:
                self.all_rows = self.service.load_jobs_for_ui()
                self.apply_filter()
            else:
                for row in updated_rows:
                    self._apply_local_row_update(row)

            self.statusBar().showMessage(
                f"Righe aggiornate: {len(updated_rows)}",
                5000,
            )

        except Exception as exc:
            logging.exception("Errore refresh_selected")
            QMessageBox.critical(
                self,
                "Errore",
                f"Errore durante refresh righe selezionate:\n{exc}",
            )

    # -------------------------------------------------------------------------
    # FILTRO
    # -------------------------------------------------------------------------

    def apply_filter(self):
        text = self.edt_filter.text().strip().lower()
        if not text:
            self.model.set_rows(self.all_rows)
            self._reapply_current_sort()
            return

        filtered = []
        for row in self.all_rows:
            haystack = " | ".join(
                str(row.get(k, ""))
                for k in (
                    "project_distretto_anno",
                    "project_name",
                    "project_base_path",
                    "dl_distretto_anno",
                    "dl_name",
                    "dl_base_path",
                    "general_notes",
                    "cartesio_prg_display",
                    "cartesio_cos_display",
                    "rilievi_dl_display",
                    "permits_display",
                    "project_rilievo",
                    "project_enti",
                    "project_revision",
                    "permessi_revision",
                    "project_tracciamento",
                )
            ).lower()
            if text in haystack:
                filtered.append(row)

        self.model.set_rows(filtered)
        self._reapply_current_sort()

    # -------------------------------------------------------------------------
    # DEFAULT META
    # -------------------------------------------------------------------------

    def _default_meta_fields(self):
        return {
            "permits_checklist_json": [],
            "permits_notes": "",
            "cartesio_prg_status": "NON IMPOSTATO",
            "cartesio_prg_notes": "",
            "cartesio_prg_manual_code": "",
            "rilievi_dl_status": "NON IMPOSTATO",
            "rilievi_dl_notes": "",
            "cartesio_cos_status": "NON IMPOSTATO",
            "cartesio_cos_notes": "",
            "cartesio_cos_manual_code": "",
            "todo_json": [],
        }

    # -------------------------------------------------------------------------
    # CRUD LAVORI
    # -------------------------------------------------------------------------

    def add_job(self):
        dlg = JobDialog(self)
        if dlg.exec():
            payload = dlg.get_payload()
            payload.update(self._default_meta_fields())

            try:
                job_id = self.db.add_job(payload)

                # Nuovo lavoro: scan immediato della singola riga e persist.
                updated = self.service.scan_and_persist_job(job_id)
                if updated:
                    self.all_rows.insert(0, updated)
                    self.apply_filter()

            except ValueError as exc:
                QMessageBox.warning(self, "Duplicato", str(exc))
                return
            except Exception as exc:
                logging.exception("Errore add_job")
                QMessageBox.critical(self, "Errore", f"Errore durante inserimento:\n{exc}")

    def _ask_import_mode(self):
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Question)
        box.setWindowTitle("Import massivo cartelle")
        box.setText("Come vuoi importare le sottocartelle della cartella selezionata?")
        btn_prg = box.addButton("Importa come PRG", QMessageBox.AcceptRole)
        btn_dl = box.addButton("Importa come DL", QMessageBox.AcceptRole)
        box.addButton("Annulla", QMessageBox.RejectRole)
        box.exec()

        clicked = box.clickedButton()
        if clicked == btn_prg:
            return "PRG"
        if clicked == btn_dl:
            return "DL"
        return None

    def _build_import_payload(self, folder_path: Path, import_mode: str):
        path_str = str(folder_path)

        payload = {
            "project_base_path": "",
            "project_distretto_anno": "",
            "project_name": "",
            "dl_base_path": "",
            "dl_distretto_anno": "",
            "dl_name": "",
            "dl_insert_date": "",
            "general_notes": "",
        }

        if import_mode == "PRG":
            payload.update(
                {
                    "project_base_path": path_str,
                    "project_distretto_anno": infer_project_distretto_anno(path_str),
                    "project_name": folder_name_from_path(path_str),
                }
            )
        elif import_mode == "DL":
            payload.update(
                {
                    "dl_base_path": path_str,
                    "dl_distretto_anno": infer_dl_distretto_anno(path_str),
                    "dl_name": folder_name_from_path(path_str),
                }
            )
        else:
            raise ValueError(f"Tipo import non supportato: {import_mode}")

        payload.update(self._default_meta_fields())
        return payload

    def import_jobs_from_parent(self):
        import_mode = self._ask_import_mode()
        if not import_mode:
            return

        parent_path = QFileDialog.getExistingDirectory(
            self,
            "Seleziona la cartella base da cui importare le sottocartelle",
            "",
        )
        if not parent_path:
            return

        parent = Path(parent_path)

        try:
            subfolders = sorted(
                [p for p in parent.iterdir() if p.is_dir()],
                key=lambda p: p.name.lower(),
            )
        except Exception as exc:
            logging.exception("Errore lettura cartella base per import massivo")
            QMessageBox.critical(
                self,
                "Errore",
                f"Impossibile leggere la cartella selezionata:\n{exc}",
            )
            return

        if not subfolders:
            QMessageBox.information(
                self,
                "Nessuna sottocartella",
                "La cartella selezionata non contiene sottocartelle da importare.",
            )
            return

        imported = []
        skipped_duplicates = []
        errors = []
        imported_ids = []

        self.statusBar().showMessage("Import massivo in corso...")

        for subfolder in subfolders:
            try:
                folder_str = str(subfolder)

                if import_mode == "PRG" and self.db.exists_project_path(folder_str):
                    skipped_duplicates.append(subfolder.name)
                    continue

                if import_mode == "DL" and self.db.exists_dl_path(folder_str):
                    skipped_duplicates.append(subfolder.name)
                    continue

                payload = self._build_import_payload(subfolder, import_mode)
                job_id = self.db.add_job(payload)
                imported.append(subfolder.name)
                imported_ids.append(job_id)

            except Exception as exc:
                logging.exception("Errore import sottocartella: %s", subfolder)
                errors.append(f"{subfolder.name}: {exc}")

        if imported_ids:
            try:
                self.service.scan_and_persist_jobs(imported_ids)
            except Exception:
                logging.exception("Errore scansione post-import")
            self.all_rows = self.service.load_jobs_for_ui()
            self.apply_filter()
        else:
            self.statusBar().clearMessage()

        summary_lines = [
            f"Cartella base: {parent}",
            f"Tipo import: {import_mode}",
            f"Sottocartelle trovate: {len(subfolders)}",
            f"Importate: {len(imported)}",
            f"Saltate perché già presenti: {len(skipped_duplicates)}",
            f"Errori: {len(errors)}",
        ]

        if skipped_duplicates:
            preview = "\n".join(f"- {name}" for name in skipped_duplicates[:15])
            summary_lines.append("\nGià presenti:\n" + preview)
            if len(skipped_duplicates) > 15:
                summary_lines.append(f"\n... e altre {len(skipped_duplicates) - 15}")

        if errors:
            preview = "\n".join(f"- {msg}" for msg in errors[:10])
            summary_lines.append("\nErrori:\n" + preview)
            if len(errors) > 10:
                summary_lines.append(f"\n... e altri {len(errors) - 10}")

        QMessageBox.information(self, "Import massivo completato", "\n".join(summary_lines))

    def edit_selected_job(self):
        job = self.current_job()
        if not job:
            QMessageBox.information(self, "Nessuna riga", "Seleziona un lavoro da modificare.")
            return

        dlg = JobDialog(self, job=job)
        if dlg.exec():
            payload = dlg.get_payload()

            for key in (
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
                "todo_json",
            ):
                payload[key] = job.get(key)

            try:
                self.db.update_job(job["id"], payload)

                # Modifica anagrafica/path: scan immediato della singola riga e persist.
                updated = self.service.scan_and_persist_job(job["id"])
                if not updated:
                    raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo l'aggiornamento.")

                self._apply_local_row_update(updated)
                self.statusBar().showMessage(f"Lavoro aggiornato: {job['id']}", 4000)

            except ValueError as exc:
                QMessageBox.warning(self, "Duplicato", str(exc))
                return
            except Exception as exc:
                logging.exception("Errore edit_selected_job")
                QMessageBox.critical(self, "Errore", f"Errore durante modifica lavoro:\n{exc}")

    def delete_selected_jobs(self):
        jobs = self.selected_jobs()

        if not jobs:
            QMessageBox.information(
                self,
                "Nessuna riga",
                "Seleziona almeno un lavoro da eliminare."
            )
            return

        count = len(jobs)

        if count == 1:
            msg = f"Eliminare il lavoro ID {jobs[0]['id']}?"
        else:
            msg = f"Eliminare i {count} lavori selezionati?"

        ans = QMessageBox.question(
            self,
            "Conferma eliminazione",
            msg,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if ans != QMessageBox.Yes:
            return

        deleted = 0
        errors = []

        for job in jobs:
            try:
                self.db.delete_job(job["id"])
                deleted += 1
                logging.info("Lavoro eliminato: id=%s", job["id"])
            except Exception as exc:
                logging.exception("Errore eliminazione lavoro id=%s", job.get("id"))
                errors.append(f"ID {job.get('id')}: {exc}")

        self.all_rows = self.service.load_jobs_for_ui()
        self.apply_filter()

        if errors:
            QMessageBox.warning(
                self,
                "Eliminazione completata con errori",
                f"Eliminati: {deleted}\n"
                f"Errori: {len(errors)}\n\n" +
                "\n".join(errors[:10])
            )
        else:
            self.statusBar().showMessage(f"Lavori eliminati: {deleted}", 5000)

    # -------------------------------------------------------------------------
    # OVERRIDE CAMPI DA SCAN
    # -------------------------------------------------------------------------

    def edit_scan_override(self, job, field_key: str, column_label: str):
        current_value = "" if job.get(field_key) is None else str(job.get(field_key, ""))

        value, ok = QInputDialog.getText(
            self,
            "Modifica valore cella",
            f"{column_label}\n\nInserisci il valore manuale da mostrare in tabella:",
            text=current_value,
        )
        if not ok:
            return

        value = value.strip()
        if not value:
            QMessageBox.warning(
                self,
                "Valore non valido",
                "Il valore manuale non può essere vuoto. Usa 'Ripristina valore automatico'.",
            )
            return

        try:
            self.db.set_scan_override(job["id"], field_key, value)
            updated = self.service.get_row_for_ui(job["id"])
            if not updated:
                raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo il salvataggio override.")

            self._apply_local_row_update(updated)
            self.statusBar().showMessage(f"Override salvato: {column_label}", 4000)

        except Exception as exc:
            logging.exception("Errore edit_scan_override")
            QMessageBox.critical(self, "Errore", f"Errore durante salvataggio override:\n{exc}")

    def clear_scan_override(self, job, field_key: str, column_label: str):
        ans = QMessageBox.question(
            self,
            "Ripristina valore automatico",
            f"Ripristinare il valore automatico per la colonna '{column_label}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if ans != QMessageBox.Yes:
            return

        try:
            self.db.clear_scan_override(job["id"], field_key)
            updated = self.service.get_row_for_ui(job["id"])
            if not updated:
                raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo il ripristino override.")

            self._apply_local_row_update(updated)
            self.statusBar().showMessage(f"Valore automatico ripristinato: {column_label}", 4000)

        except Exception as exc:
            logging.exception("Errore clear_scan_override")
            QMessageBox.critical(self, "Errore", f"Errore durante ripristino valore automatico:\n{exc}")

    # -------------------------------------------------------------------------
    # OPEN PATH
    # -------------------------------------------------------------------------

    def handle_double_click(self, index):
        job = self.model.get_row(index.row())
        if not job:
            return

        col = index.column()
        scan = job.get("scan", {})

        path = ""
        if col == 1:
            path = job.get("project_base_path", "")
        elif col == 2:
            path = scan.get("project_rilievo", {}).get("path", "")
        elif col == 4:
            path = scan.get("project_revision", {}).get("path", "")
        elif col == 5 or col == 6:
            path = scan.get("permessi_revision", {}).get("path", "")
        elif col == 8:
            path = scan.get("cartesio_prg", {}).get("path", "")
        elif col == 10:
            path = job.get("dl_base_path", "")
        elif col == 12:
            path = scan.get("rilievi_dl", {}).get("path", "")
        elif col == 13:
            path = scan.get("cartesio_cos", {}).get("path", "")

        if path:
            ok, msg = open_in_explorer(path)
            if not ok:
                QMessageBox.warning(self, "Apertura percorso", msg)

    def open_context_menu(self, pos: QPoint):
        index = self.table.indexAt(pos)
        if not index.isValid():
            return

        job = self.model.get_row(index.row())
        if not job:
            return

        menu = QMenu(self)
        col = index.column()
        column_label = self.model.headerData(col, Qt.Horizontal)
        field_key = self._scan_override_field_for_column(col)

        act_edit_override = None
        act_reset_override = None
        if field_key:
            act_edit_override = menu.addAction("Modifica valore cella...")
            if self._job_has_scan_override(job, field_key):
                act_reset_override = menu.addAction("Ripristina valore automatico")

        act_permits = act_cart_prg = act_rilievi_dl = act_cart_cos = None

        if col == 6:
            if field_key:
                menu.addSeparator()
            act_permits = menu.addAction("Modifica checklist Permessi...")
        elif col == 8:
            if field_key:
                menu.addSeparator()
            act_cart_prg = menu.addAction("Imposta stato Cartesio PRG...")
        elif col == 12:
            if field_key:
                menu.addSeparator()
            act_rilievi_dl = menu.addAction("Imposta stato Rilievi DL...")
        elif col == 13:
            if field_key:
                menu.addSeparator()
            act_cart_cos = menu.addAction("Imposta stato Cartesio COS...")

        menu.addSeparator()
        act_todo = menu.addAction("ToDo generale lavoro...")

        chosen = menu.exec(self.table.viewport().mapToGlobal(pos))
        if not chosen:
            return

        if chosen == act_edit_override and field_key:
            self.edit_scan_override(job, field_key, column_label)
        elif chosen == act_reset_override and field_key:
            self.clear_scan_override(job, field_key, column_label)
        elif chosen == act_todo:
            self.edit_todo(job)
        elif chosen == act_permits:
            self.edit_permessi(job)
        elif chosen == act_cart_prg:
            self.edit_cartesio_prg(job)
        elif chosen == act_rilievi_dl:
            self.edit_rilievi_dl(job)
        elif chosen == act_cart_cos:
            self.edit_cartesio_cos(job)

    # -------------------------------------------------------------------------
    # EDIT META MANUALI: NO SCAN
    # -------------------------------------------------------------------------

    def edit_permessi(self, job):
        dlg = PermitsDialog(
            self,
            checklist=job.get("permits_checklist_json"),
            notes=job.get("permits_notes", ""),
        )

        if dlg.exec():
            checklist, notes = dlg.get_payload()

            try:
                self.db.update_meta_fields(
                    job["id"],
                    permits_checklist_json=checklist,
                    permits_notes=notes,
                )

                updated = self.service.refresh_row_without_rescan(
                    job,
                    permits_checklist_json=checklist,
                    permits_notes=notes,
                )
                self._apply_local_row_update(updated)
                self.statusBar().showMessage(f"Permessi aggiornati: {job['id']}", 4000)

            except Exception as exc:
                logging.exception("Errore edit_permessi")
                QMessageBox.critical(self, "Errore", f"Errore durante aggiornamento permessi:\n{exc}")

    def edit_cartesio_prg(self, job):
        dlg = StatusDialog(
            "Stato Cartesio Progetto",
            CARTESIO_PRG_STATES,
            current_status=job.get("cartesio_prg_status", "NON IMPOSTATO"),
            notes=job.get("cartesio_prg_notes", ""),
            manual_code=job.get("cartesio_prg_manual_code", ""),
            parent=self,
        )

        if dlg.exec():
            payload = dlg.get_payload()

            try:
                self.db.update_meta_fields(
                    job["id"],
                    cartesio_prg_status=payload["status"],
                    cartesio_prg_notes=payload["notes"],
                    cartesio_prg_manual_code=payload["manual_code"],
                )

                updated = self.service.refresh_row_without_rescan(
                    job,
                    cartesio_prg_status=payload["status"],
                    cartesio_prg_notes=payload["notes"],
                    cartesio_prg_manual_code=payload["manual_code"],
                )
                self._apply_local_row_update(updated)
                self.statusBar().showMessage(f"Cartesio PRG aggiornato: {job['id']}", 4000)

            except Exception as exc:
                logging.exception("Errore edit_cartesio_prg")
                QMessageBox.critical(self, "Errore", f"Errore durante aggiornamento Cartesio PRG:\n{exc}")

    def edit_rilievi_dl(self, job):
        dlg = StatusDialog(
            "Stato Rilievi DL",
            RILIEVI_DL_STATES,
            current_status=job.get("rilievi_dl_status", "NON IMPOSTATO"),
            notes=job.get("rilievi_dl_notes", ""),
            manual_code="",
            parent=self,
        )
        dlg.edt_manual_code.setEnabled(False)

        if dlg.exec():
            payload = dlg.get_payload()

            try:
                self.db.update_meta_fields(
                    job["id"],
                    rilievi_dl_status=payload["status"],
                    rilievi_dl_notes=payload["notes"],
                )

                updated = self.service.refresh_row_without_rescan(
                    job,
                    rilievi_dl_status=payload["status"],
                    rilievi_dl_notes=payload["notes"],
                )
                self._apply_local_row_update(updated)
                self.statusBar().showMessage(f"Rilievi DL aggiornati: {job['id']}", 4000)

            except Exception as exc:
                logging.exception("Errore edit_rilievi_dl")
                QMessageBox.critical(self, "Errore", f"Errore durante aggiornamento Rilievi DL:\n{exc}")

    def edit_cartesio_cos(self, job):
        dlg = StatusDialog(
            "Stato Cartesio COS",
            CARTESIO_COS_STATES,
            current_status=job.get("cartesio_cos_status", "NON IMPOSTATO"),
            notes=job.get("cartesio_cos_notes", ""),
            manual_code=job.get("cartesio_cos_manual_code", ""),
            parent=self,
        )

        if dlg.exec():
            payload = dlg.get_payload()

            try:
                self.db.update_meta_fields(
                    job["id"],
                    cartesio_cos_status=payload["status"],
                    cartesio_cos_notes=payload["notes"],
                    cartesio_cos_manual_code=payload["manual_code"],
                )

                updated = self.service.refresh_row_without_rescan(
                    job,
                    cartesio_cos_status=payload["status"],
                    cartesio_cos_notes=payload["notes"],
                    cartesio_cos_manual_code=payload["manual_code"],
                )
                self._apply_local_row_update(updated)
                self.statusBar().showMessage(f"Cartesio COS aggiornato: {job['id']}", 4000)

            except Exception as exc:
                logging.exception("Errore edit_cartesio_cos")
                QMessageBox.critical(self, "Errore", f"Errore durante aggiornamento Cartesio COS:\n{exc}")

    def edit_todo(self, job):
        dlg = TodoDialog(self, todo_items=job.get("todo_json") or [])

        if dlg.exec():
            todo_items = dlg.get_payload()

            try:
                self.db.update_meta_fields(job["id"], todo_json=todo_items)

                updated = self.service.refresh_row_without_rescan(
                    job,
                    todo_json=todo_items,
                )
                self._apply_local_row_update(updated)
                self.statusBar().showMessage(f"ToDo aggiornato: {job['id']}", 4000)

            except Exception as exc:
                logging.exception("Errore edit_todo")
                QMessageBox.critical(self, "Errore", f"Errore durante aggiornamento ToDo:\n{exc}")

    # -------------------------------------------------------------------------
    # CHIUSURA
    # -------------------------------------------------------------------------

    def closeEvent(self, event):
        try:
            self.db.close()
        except Exception:
            logging.exception("Errore chiusura DB")
        super().closeEvent(event)


if __name__ == "__main__":
    setup_logging()
    try:
        app = QApplication(sys.argv)
        app.setStyle("Fusion")
        window = MainWindow()
        window.show()
        sys.exit(app.exec())
    except Exception:
        logging.exception("Crash applicazione")
        traceback.print_exc()
        raise
