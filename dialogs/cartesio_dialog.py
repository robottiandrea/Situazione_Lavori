# -*- coding: utf-8 -*-
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QInputDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from services import JobService
from utils import (
    CARTESIO_COS_STATES,
    CARTESIO_PRG_STATES,
    build_cartesio_attachment_rel_path,
    ensure_cartesio_attachment_dir,
    open_in_explorer,
    resolve_cartesio_attachment_path,
    safe_filename,
)


class CartesioNoteDialog(QDialog):
    def __init__(
        self,
        parent=None,
        *,
        title: str = "",
        body: str = "",
        checklist_json: Optional[List[Dict[str, Any]]] = None,
        thread_id: Optional[int] = None,
        thread_options: Optional[List[Dict[str, Any]]] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Nota Cartesio")
        self.resize(760, 560)

        self.thread_options = thread_options or []
        self.attachments = [dict(item) for item in (attachments or [])]
        self.pending_attachment_paths: List[str] = []
        self.removed_attachment_ids: List[int] = []

        self._build_ui()
        self.edt_title.setText(title)
        self.txt_body.setPlainText(body)
        checklist_lines = []
        for item in checklist_json or []:
            text_value = str(item.get("text") or item.get("name") or "").strip()
            if text_value:
                checklist_lines.append(text_value)
        self.txt_checklist.setPlainText("\n".join(checklist_lines))

        self.cmb_thread.addItem("Nessun thread", None)
        for item in self.thread_options:
            self.cmb_thread.addItem(str(item.get("title") or ""), int(item.get("id")))
        if thread_id is not None:
            idx = self.cmb_thread.findData(int(thread_id))
            if idx >= 0:
                self.cmb_thread.setCurrentIndex(idx)

        self._reload_attachments_list()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        form = QFormLayout()

        self.edt_title = QLineEdit()
        form.addRow("Titolo", self.edt_title)

        self.cmb_thread = QComboBox()
        form.addRow("Thread", self.cmb_thread)

        self.txt_body = QTextEdit()
        self.txt_body.setPlaceholderText("Testo nota...")
        form.addRow("Testo", self.txt_body)

        self.txt_checklist = QTextEdit()
        self.txt_checklist.setPlaceholderText("Checklist libera, una voce per riga...")
        form.addRow("Checklist", self.txt_checklist)

        root.addLayout(form)

        attachments_box = QGroupBox("Allegati")
        attachments_layout = QVBoxLayout(attachments_box)
        self.lst_attachments = QListWidget()
        attachments_layout.addWidget(self.lst_attachments, 1)

        attachments_btns = QHBoxLayout()
        self.btn_add_attachment = QPushButton("Aggiungi file...")
        self.btn_open_attachment = QPushButton("Apri")
        self.btn_remove_attachment = QPushButton("Rimuovi")
        self.btn_add_attachment.clicked.connect(self._add_attachment_files)
        self.btn_open_attachment.clicked.connect(self._open_selected_attachment)
        self.btn_remove_attachment.clicked.connect(self._remove_selected_attachment)
        attachments_btns.addWidget(self.btn_add_attachment)
        attachments_btns.addWidget(self.btn_open_attachment)
        attachments_btns.addWidget(self.btn_remove_attachment)
        attachments_btns.addStretch(1)
        attachments_layout.addLayout(attachments_btns)
        root.addWidget(attachments_box, 1)

        btns = QHBoxLayout()
        btns.addStretch(1)
        btn_ok = QPushButton("Salva")
        btn_cancel = QPushButton("Annulla")
        btn_ok.clicked.connect(self.accept)
        btn_cancel.clicked.connect(self.reject)
        btns.addWidget(btn_ok)
        btns.addWidget(btn_cancel)
        root.addLayout(btns)

    def _reload_attachments_list(self) -> None:
        self.lst_attachments.clear()
        for item in self.attachments:
            label = str(item.get("display_name") or item.get("source_path") or "").strip()
            prefix = "[nuovo] " if item.get("pending") else ""
            lw_item = QListWidgetItem(prefix + label)
            lw_item.setData(Qt.UserRole, item)
            self.lst_attachments.addItem(lw_item)

    def _add_attachment_files(self) -> None:
        paths, _ = QFileDialog.getOpenFileNames(self, "Seleziona allegati", "")
        if not paths:
            return

        for path in paths:
            clean_path = str(path or "").strip()
            if not clean_path:
                continue
            self.attachments.append(
                {
                    "id": None,
                    "pending": True,
                    "source_path": clean_path,
                    "display_name": Path(clean_path).name,
                    "attachment_kind": "file",
                }
            )
            self.pending_attachment_paths.append(clean_path)
        self._reload_attachments_list()

    def _open_selected_attachment(self) -> None:
        item = self.lst_attachments.currentItem()
        if not item:
            return
        payload = dict(item.data(Qt.UserRole) or {})
        if payload.get("pending"):
            target = str(payload.get("source_path") or "")
        else:
            target = str(resolve_cartesio_attachment_path(payload.get("stored_rel_path", "")))
        if not target:
            return
        ok, msg = open_in_explorer(target)
        if not ok:
            QMessageBox.warning(self, "Allegato", msg)

    def _remove_selected_attachment(self) -> None:
        item = self.lst_attachments.currentItem()
        if not item:
            return
        payload = dict(item.data(Qt.UserRole) or {})
        if payload.get("pending"):
            source_path = str(payload.get("source_path") or "")
            self.pending_attachment_paths = [p for p in self.pending_attachment_paths if p != source_path]
        else:
            attachment_id = payload.get("id")
            if attachment_id is not None:
                self.removed_attachment_ids.append(int(attachment_id))
        attachment_id = payload.get("id")
        source_path = str(payload.get("source_path") or "")
        display_name = str(payload.get("display_name") or "")
        filtered = []
        for attachment in self.attachments:
            if attachment_id is not None and attachment.get("id") == attachment_id:
                continue
            if source_path and str(attachment.get("source_path") or "") == source_path:
                continue
            if attachment_id is None and not source_path and display_name and str(attachment.get("display_name") or "") == display_name and attachment.get("pending") == payload.get("pending"):
                continue
            filtered.append(attachment)
        self.attachments = filtered
        self._reload_attachments_list()

    def accept(self) -> None:
        if not self.edt_title.text().strip():
            QMessageBox.warning(self, "Titolo mancante", "Inserisci il titolo della nota.")
            return
        super().accept()

    def get_payload(self) -> Dict[str, Any]:
        checklist_lines = [line.strip() for line in self.txt_checklist.toPlainText().splitlines() if line.strip()]
        checklist_json = [{"text": line, "done": False} for line in checklist_lines]
        return {
            "title": self.edt_title.text().strip(),
            "body": self.txt_body.toPlainText().strip(),
            "checklist_json": checklist_json,
            "thread_id": self.cmb_thread.currentData(),
            "pending_attachment_paths": list(self.pending_attachment_paths),
            "removed_attachment_ids": list(self.removed_attachment_ids),
        }


class CartesioDialog(QDialog):
    def __init__(self, service: JobService, job_id: int, scope: str, parent=None) -> None:
        super().__init__(parent)
        self.service = service
        self.job_id = int(job_id)
        self.scope = str(scope or "").strip().upper()
        self.setWindowTitle(f"Cartesio {self.scope} - Lavoro #{self.job_id}")
        self.resize(1200, 760)
        self._build_ui()
        self._load_bundle()

    def _states_for_scope(self) -> List[str]:
        return CARTESIO_PRG_STATES if self.scope == "PRG" else CARTESIO_COS_STATES

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)

        self.lbl_title = QLabel("")
        self.lbl_title.setTextInteractionFlags(Qt.TextSelectableByMouse)
        root.addWidget(self.lbl_title)

        header_box = QGroupBox("Entry Cartesio")
        header_layout = QGridLayout(header_box)

        self.chk_active = QCheckBox("Scope attivo")
        self.cmb_status = QComboBox()
        self.cmb_status.addItems(self._states_for_scope())
        self.edt_referente = QLineEdit()
        self.edt_manual_code = QLineEdit()
        self.edt_manual_code.setPlaceholderText("Codice manuale opzionale...")

        header_layout.addWidget(QLabel("Scope"), 0, 0)
        header_layout.addWidget(QLabel(self.scope), 0, 1)
        header_layout.addWidget(self.chk_active, 0, 2, 1, 2)
        header_layout.addWidget(QLabel("Stato"), 1, 0)
        header_layout.addWidget(self.cmb_status, 1, 1)
        header_layout.addWidget(QLabel("Referente"), 1, 2)
        header_layout.addWidget(self.edt_referente, 1, 3)
        header_layout.addWidget(QLabel("Codice manuale"), 2, 0)
        header_layout.addWidget(self.edt_manual_code, 2, 1, 1, 3)

        header_btns = QHBoxLayout()
        self.btn_save_header = QPushButton("Salva entry")
        self.btn_save_header.clicked.connect(self._save_entry_header)
        header_btns.addStretch(1)
        header_btns.addWidget(self.btn_save_header)
        header_layout.addLayout(header_btns, 3, 0, 1, 4)
        root.addWidget(header_box)

        splitter = QSplitter(Qt.Horizontal)
        root.addWidget(splitter, 1)

        threads_panel = QWidget()
        threads_layout = QVBoxLayout(threads_panel)
        threads_layout.addWidget(QLabel("Thread"))
        self.tbl_threads = QTableWidget(0, 3)
        self.tbl_threads.setHorizontalHeaderLabels(["Titolo", "Stato", "Note"])
        self.tbl_threads.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbl_threads.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tbl_threads.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl_threads.verticalHeader().setVisible(False)
        threads_layout.addWidget(self.tbl_threads, 1)
        thread_btns = QHBoxLayout()
        self.btn_new_thread = QPushButton("Nuovo thread")
        self.btn_close_thread = QPushButton("Chiudi thread")
        self.btn_reopen_thread = QPushButton("Riapri thread")
        self.btn_new_thread.clicked.connect(self._create_thread)
        self.btn_close_thread.clicked.connect(lambda: self._set_selected_thread_status("CHIUSO"))
        self.btn_reopen_thread.clicked.connect(lambda: self._set_selected_thread_status("APERTO"))
        thread_btns.addWidget(self.btn_new_thread)
        thread_btns.addWidget(self.btn_close_thread)
        thread_btns.addWidget(self.btn_reopen_thread)
        threads_layout.addLayout(thread_btns)
        splitter.addWidget(threads_panel)

        notes_panel = QWidget()
        notes_layout = QVBoxLayout(notes_panel)
        notes_layout.addWidget(QLabel("Note"))
        self.tbl_notes = QTableWidget(0, 4)
        self.tbl_notes.setHorizontalHeaderLabels(["Titolo", "Thread", "Aggiornata", "Allegati"])
        self.tbl_notes.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbl_notes.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tbl_notes.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl_notes.verticalHeader().setVisible(False)
        self.tbl_notes.itemDoubleClicked.connect(self._edit_selected_note)
        notes_layout.addWidget(self.tbl_notes, 1)
        note_btns = QHBoxLayout()
        self.btn_new_note = QPushButton("Nuova nota")
        self.btn_new_note_in_thread = QPushButton("Nuova nota nel thread")
        self.btn_edit_note = QPushButton("Modifica nota")
        self.btn_delete_note = QPushButton("Elimina nota")
        self.btn_new_note.clicked.connect(self._create_note)
        self.btn_new_note_in_thread.clicked.connect(lambda: self._create_note(force_selected_thread=True))
        self.btn_edit_note.clicked.connect(self._edit_selected_note)
        self.btn_delete_note.clicked.connect(self._delete_selected_note)
        note_btns.addWidget(self.btn_new_note)
        note_btns.addWidget(self.btn_new_note_in_thread)
        note_btns.addWidget(self.btn_edit_note)
        note_btns.addWidget(self.btn_delete_note)
        notes_layout.addLayout(note_btns)
        splitter.addWidget(notes_panel)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 2)

        bottom = QHBoxLayout()
        bottom.addStretch(1)
        btn_close = QPushButton("Chiudi")
        btn_close.clicked.connect(self.accept)
        bottom.addWidget(btn_close)
        root.addLayout(bottom)

    def _load_bundle(self) -> None:
        self.bundle = self.service.get_cartesio_bundle(self.job_id, self.scope)
        job = dict(self.bundle.get("job") or {})
        entry = dict(self.bundle.get("entry") or {})
        self.threads = list(self.bundle.get("threads") or [])
        self.notes = list(self.bundle.get("notes") or [])

        title_parts = [f"Lavoro #{self.job_id}", f"Scope: {self.scope}"]
        project_name = str(job.get("project_name_display") or job.get("project_name") or "").strip()
        dl_name = str(job.get("dl_name") or "").strip()
        if project_name:
            title_parts.append(f"PRG: {project_name}")
        if dl_name:
            title_parts.append(f"DL: {dl_name}")
        self.lbl_title.setText(" | ".join(title_parts))

        if entry:
            self.edt_referente.setText(str(entry.get("referente") or ""))
            self.edt_manual_code.setText(str(entry.get("manual_code") or ""))
            idx = self.cmb_status.findText(str(entry.get("status") or "NON IMPOSTATO"))
            if idx >= 0:
                self.cmb_status.setCurrentIndex(idx)
            self.chk_active.setChecked(str(entry.get("cartesio_delivery_scope") or "NONE").upper() == self.scope)
        else:
            self.edt_referente.clear()
            self.edt_manual_code.clear()
            idx = self.cmb_status.findText("NON IMPOSTATO")
            if idx >= 0:
                self.cmb_status.setCurrentIndex(idx)
            self.chk_active.setChecked(False)

        self._reload_threads_table()
        self._reload_notes_table()

    def _reload_threads_table(self) -> None:
        self.tbl_threads.setRowCount(len(self.threads))
        for row_index, item in enumerate(self.threads):
            title_item = QTableWidgetItem(str(item.get("title") or ""))
            title_item.setData(Qt.UserRole, int(item.get("id") or 0))
            self.tbl_threads.setItem(row_index, 0, title_item)
            self.tbl_threads.setItem(row_index, 1, QTableWidgetItem(str(item.get("status") or "")))
            self.tbl_threads.setItem(row_index, 2, QTableWidgetItem(str(item.get("notes_count") or 0)))
        self.tbl_threads.resizeColumnsToContents()

    def _reload_notes_table(self) -> None:
        self.tbl_notes.setRowCount(len(self.notes))
        for row_index, item in enumerate(self.notes):
            title_item = QTableWidgetItem(str(item.get("title") or ""))
            title_item.setData(Qt.UserRole, int(item.get("id") or 0))
            self.tbl_notes.setItem(row_index, 0, title_item)
            self.tbl_notes.setItem(row_index, 1, QTableWidgetItem(str(item.get("thread_title") or "")))
            self.tbl_notes.setItem(row_index, 2, QTableWidgetItem(str(item.get("updated_at") or "")))
            attachments_count = len(item.get("attachments") or [])
            self.tbl_notes.setItem(row_index, 3, QTableWidgetItem(str(attachments_count)))
        self.tbl_notes.resizeColumnsToContents()

    def _selected_thread(self) -> Optional[Dict[str, Any]]:
        row_index = self.tbl_threads.currentRow()
        if row_index < 0 or row_index >= len(self.threads):
            return None
        return self.threads[row_index]

    def _selected_note(self) -> Optional[Dict[str, Any]]:
        row_index = self.tbl_notes.currentRow()
        if row_index < 0 or row_index >= len(self.notes):
            return None
        return self.notes[row_index]

    def _save_entry_header(self) -> None:
        bundle = self.service.save_cartesio_entry(
            job_id=self.job_id,
            scope=self.scope,
            referente=self.edt_referente.text().strip(),
            status=self.cmb_status.currentText(),
            manual_code=self.edt_manual_code.text().strip(),
            is_active=self.chk_active.isChecked(),
        )
        warning_text = str(bundle.get("activation_warning") or "").strip()
        self._load_bundle()
        if warning_text:
            QMessageBox.warning(self, "Verifica passaggio PRG/COS", warning_text)
        else:
            QMessageBox.information(self, "Cartesio", "Entry Cartesio salvata.")

    def _create_thread(self) -> None:
        thread_title, ok = QInputDialog.getText(self, "Nuovo thread", "Titolo thread:")
        if not ok:
            return
        thread_title = thread_title.strip()
        if not thread_title:
            return
        self._save_entry_header_silent()
        self.service.add_cartesio_thread(self.job_id, self.scope, thread_title)
        self._load_bundle()

    def _save_entry_header_silent(self) -> None:
        self.service.save_cartesio_entry(
            job_id=self.job_id,
            scope=self.scope,
            referente=self.edt_referente.text().strip(),
            status=self.cmb_status.currentText(),
            manual_code=self.edt_manual_code.text().strip(),
            is_active=self.chk_active.isChecked(),
        )

    def _set_selected_thread_status(self, status: str) -> None:
        thread = self._selected_thread()
        if not thread:
            QMessageBox.information(self, "Thread", "Seleziona un thread.")
            return
        self.service.set_cartesio_thread_status(int(thread["id"]), status)
        self._load_bundle()

    def _create_note(self, force_selected_thread: bool = False) -> None:
        selected_thread = self._selected_thread() if force_selected_thread else None
        if force_selected_thread and not selected_thread:
            QMessageBox.information(self, "Thread mancante", "Seleziona prima un thread.")
            return
        self._save_entry_header_silent()
        dlg = CartesioNoteDialog(
            self,
            thread_id=int(selected_thread["id"]) if selected_thread else None,
            thread_options=self.threads,
        )
        if not dlg.exec():
            return
        payload = dlg.get_payload()
        note = self.service.add_cartesio_note(
            job_id=self.job_id,
            scope=self.scope,
            title=payload["title"],
            body=payload["body"],
            checklist_json=payload["checklist_json"],
            thread_id=payload.get("thread_id"),
        )
        self._persist_pending_attachments(note, payload.get("pending_attachment_paths") or [])
        self._load_bundle()

    def _edit_selected_note(self, *_args) -> None:
        note = self._selected_note()
        if not note:
            QMessageBox.information(self, "Nota", "Seleziona una nota.")
            return
        dlg = CartesioNoteDialog(
            self,
            title=str(note.get("title") or ""),
            body=str(note.get("body") or ""),
            checklist_json=note.get("checklist_json") or [],
            thread_id=note.get("thread_id"),
            thread_options=self.threads,
            attachments=note.get("attachments") or [],
        )
        if not dlg.exec():
            return
        payload = dlg.get_payload()
        updated_note = self.service.update_cartesio_note(
            note_id=int(note["id"]),
            title=payload["title"],
            body=payload["body"],
            checklist_json=payload["checklist_json"],
            thread_id=payload.get("thread_id"),
        )
        for attachment_id in payload.get("removed_attachment_ids") or []:
            removed = self.service.remove_cartesio_attachment(int(attachment_id))
            if removed:
                path = resolve_cartesio_attachment_path(str(removed.get("stored_rel_path") or ""))
                try:
                    if path.is_file():
                        path.unlink()
                except Exception:
                    pass
        self._persist_pending_attachments(updated_note, payload.get("pending_attachment_paths") or [])
        self._load_bundle()

    def _delete_selected_note(self) -> None:
        note = self._selected_note()
        if not note:
            QMessageBox.information(self, "Nota", "Seleziona una nota.")
            return
        ans = QMessageBox.question(
            self,
            "Elimina nota",
            f"Eliminare la nota '{note.get('title', '')}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if ans != QMessageBox.Yes:
            return
        for attachment in note.get("attachments") or []:
            path = resolve_cartesio_attachment_path(str(attachment.get("stored_rel_path") or ""))
            try:
                if path.is_file():
                    path.unlink()
            except Exception:
                pass
        self.service.delete_cartesio_note(int(note["id"]))
        self._load_bundle()

    def _persist_pending_attachments(self, note: Dict[str, Any], pending_paths: List[str]) -> None:
        if not note or not pending_paths:
            return
        note_id = int(note["id"])
        bucket_name = f"note_{note_id}"
        target_dir = ensure_cartesio_attachment_dir(self.job_id, self.scope, bucket_name)
        for source_path in pending_paths:
            src = Path(str(source_path or "").strip())
            if not src.is_file():
                continue
            safe_name = safe_filename(src.name, fallback=f"allegato_{note_id}")
            dest = target_dir / safe_name
            counter = 1
            while dest.exists():
                dest = target_dir / f"{dest.stem}_{counter}{dest.suffix}"
                counter += 1
            shutil.copy2(src, dest)
            self.service.add_cartesio_attachment(
                note_id=note_id,
                attachment_kind="file",
                stored_rel_path=build_cartesio_attachment_rel_path(dest),
                display_name=dest.name,
            )
