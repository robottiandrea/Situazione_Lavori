# -*- coding: utf-8 -*-
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from dragdrop import AttachmentDropListWidget, cleanup_temp_drop_file
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

class ExpandableChecklistNoteEdit(QTextEdit):
    """
    Editor nota checklist:
    - compatto da chiuso (1 riga visibile)
    - multilinea reale
    - si espande al click/focus
    - si richiude quando perde il focus
    """

    def __init__(
        self,
        text: str = "",
        *,
        collapsed_lines: int = 1,
        expanded_lines: int = 4,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._collapsed_lines = max(1, int(collapsed_lines))
        self._expanded_lines = max(self._collapsed_lines, int(expanded_lines))
        self._expanded = False

        self.setAcceptRichText(False)
        self.setTabChangesFocus(True)
        self.setPlaceholderText("Nota voce... (clic per espandere)")
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setLineWrapMode(QTextEdit.WidgetWidth)
        self.setPlainText(str(text or ""))

        self._apply_height()

    def _height_for_lines(self, lines: int) -> int:
        line_height = self.fontMetrics().lineSpacing()
        doc_margin = int(self.document().documentMargin() * 2)
        frame = int(self.frameWidth() * 2)
        return (line_height * max(1, lines)) + doc_margin + frame + 6

    def _apply_height(self) -> None:
        visible_lines = self._expanded_lines if self._expanded else self._collapsed_lines
        self.setFixedHeight(self._height_for_lines(visible_lines))

    def set_expanded(self, value: bool) -> None:
        value = bool(value)
        if self._expanded == value:
            return
        self._expanded = value
        self._apply_height()

    def mousePressEvent(self, event) -> None:
        self.set_expanded(True)
        super().mousePressEvent(event)

    def focusInEvent(self, event) -> None:
        self.set_expanded(True)
        super().focusInEvent(event)

    def focusOutEvent(self, event) -> None:
        super().focusOutEvent(event)
        self.set_expanded(False)

class CartesioChecklistRowWidget(QWidget):
    """Riga editabile della checklist entry-level Cartesio."""

    changed = Signal()
    done_toggled = Signal()
    remove_requested = Signal(object)

    def __init__(self, item: Optional[Dict[str, Any]] = None, parent=None) -> None:
        super().__init__(parent)
        payload = dict(item or {})
        self._build_ui(payload)

    def _build_ui(self, payload: Dict[str, Any]) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(4)

        top = QHBoxLayout()
        top.setContentsMargins(0, 0, 0, 0)
        top.setSpacing(6)

        self.chk_done = QCheckBox()
        self.chk_done.setChecked(bool(payload.get("done")))

        self.edt_text = QLineEdit(str(payload.get("text") or ""))
        self.edt_text.setPlaceholderText("Voce checklist...")

        self.btn_remove = QPushButton("X")
        self.btn_remove.setMaximumWidth(32)

        top.addWidget(self.chk_done)
        top.addWidget(self.edt_text, 1)
        top.addWidget(self.btn_remove)

        self.edt_note = ExpandableChecklistNoteEdit(
            str(payload.get("note") or ""),
            collapsed_lines=1,
            expanded_lines=4,
            parent=self,
        )
        self.edt_note.setToolTip(self.edt_note.toPlainText().strip())

        root.addLayout(top)
        root.addWidget(self.edt_note)

        self.chk_done.stateChanged.connect(self._on_done_state_changed)
        self.edt_text.textChanged.connect(lambda _text: self.changed.emit())
        self.edt_note.textChanged.connect(self._on_note_changed)
        self.btn_remove.clicked.connect(lambda: self.remove_requested.emit(self))

    def _on_done_state_changed(self) -> None:
        self.changed.emit()
        self.done_toggled.emit()

    def _on_note_changed(self) -> None:
        self.edt_note.setToolTip(self.edt_note.toPlainText().strip())
        self.changed.emit()

    def get_payload(self) -> Dict[str, Any]:
        return {
            "text": self.edt_text.text().strip(),
            "done": self.chk_done.isChecked(),
            "note": self.edt_note.toPlainText().strip(),
        }

class CartesioNoteDialog(QDialog):
    """Dialog nota Cartesio con allegati da picker o drag&drop esterno."""

    def __init__(
        self,
        parent=None,
        *,
        title: str = "",
        body: str = "",
        thread_id: Optional[int] = None,
        thread_options: Optional[List[Dict[str, Any]]] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Nota Cartesio")
        self.resize(820, 560)

        self.thread_options = thread_options or []
        self.attachments = [dict(item) for item in (attachments or [])]
        self.removed_attachment_ids: List[int] = []

        self._build_ui()
        self.edt_title.setText(title)
        self.txt_body.setPlainText(body)

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

        root.addLayout(form)

        attachments_box = QGroupBox("Allegati")
        attachments_layout = QVBoxLayout(attachments_box)

        lbl_hint = QLabel(
            "Puoi aggiungere file con il pulsante oppure trascinare qui file dal desktop "
            "e mail complete da Outlook (.msg). Il salvataggio fisico in cartesio_attachments "
            "avviene quando confermi la nota."
        )
        lbl_hint.setWordWrap(True)
        attachments_layout.addWidget(lbl_hint)

        self.lst_attachments = AttachmentDropListWidget()
        self.lst_attachments.attachments_dropped.connect(self._on_dropped_attachments)
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

    def _attachment_label(self, item: Dict[str, Any]) -> str:
        kind = str(item.get("attachment_kind") or "file").strip().lower()
        display_name = str(item.get("display_name") or item.get("source_path") or "").strip()

        if item.get("pending"):
            if kind == "outlook_msg":
                return f"[nuova mail] {display_name}"
            return f"[nuovo file] {display_name}"

        if kind == "outlook_msg":
            return f"[mail] {display_name}"

        return display_name

    def _attachment_tooltip(self, item: Dict[str, Any]) -> str:
        lines = [str(item.get("display_name") or "").strip()]

        kind = str(item.get("attachment_kind") or "file").strip()
        if kind:
            lines.append(f"Tipo: {kind}")

        sender = str(item.get("sender") or "").strip()
        if sender:
            lines.append(f"Mittente: {sender}")

        subject = str(item.get("subject") or "").strip()
        if subject:
            lines.append(f"Oggetto: {subject}")

        received_at = str(item.get("received_at") or "").strip()
        if received_at:
            lines.append(f"Ricevuta: {received_at}")

        source_path = str(item.get("source_path") or "").strip()
        if item.get("pending") and source_path:
            lines.append(f"Sorgente temporanea: {source_path}")

        stored_rel_path = str(item.get("stored_rel_path") or "").strip()
        if stored_rel_path and not item.get("pending"):
            lines.append(f"Path salvato: {stored_rel_path}")

        return "\n".join(line for line in lines if line)

    def _reload_attachments_list(self) -> None:
        self.lst_attachments.clear()

        for item in self.attachments:
            lw_item = QListWidgetItem(self._attachment_label(item))
            lw_item.setToolTip(self._attachment_tooltip(item))
            lw_item.setData(Qt.UserRole, item)
            self.lst_attachments.addItem(lw_item)

    def _append_pending_attachments(self, new_items: List[Dict[str, Any]]) -> None:
        appended = False

        for item in new_items:
            payload = dict(item or {})
            payload["id"] = None
            payload["pending"] = True

            source_path = str(payload.get("source_path") or "").strip()
            if not source_path:
                continue

            self.attachments.append(payload)
            appended = True

        if appended:
            self._reload_attachments_list()

    def _on_dropped_attachments(self, items: List[Dict[str, Any]], errors: List[str]) -> None:
        if items:
            self._append_pending_attachments(items)

        if errors:
            QMessageBox.warning(
                self,
                "Drag & Drop allegati",
                "\n".join(str(err) for err in errors if str(err).strip()),
            )

    def _add_attachment_files(self) -> None:
        paths, _ = QFileDialog.getOpenFileNames(self, "Seleziona allegati", "")
        if not paths:
            return

        new_items: List[Dict[str, Any]] = []

        for path in paths:
            clean_path = str(path or "").strip()
            if not clean_path:
                continue

            new_items.append(
                {
                    "id": None,
                    "pending": True,
                    "attachment_kind": "file",
                    "source_path": clean_path,
                    "display_name": Path(clean_path).name,
                    "temp_file": False,
                    "subject": "",
                    "sender": "",
                    "received_at": "",
                    "meta_json": {
                        "source": "dialog_file_picker",
                    },
                }
            )

        self._append_pending_attachments(new_items)

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

    def _cleanup_temp_pending_attachment(self, payload: Dict[str, Any]) -> None:
        if not payload.get("pending"):
            return

        if not bool(payload.get("temp_file")):
            return

        cleanup_temp_drop_file(str(payload.get("source_path") or ""))

    def _cleanup_all_pending_temp_attachments(self) -> None:
        for attachment in self.attachments:
            if attachment.get("pending"):
                self._cleanup_temp_pending_attachment(attachment)

    def _remove_selected_attachment(self) -> None:
        item = self.lst_attachments.currentItem()
        if not item:
            return

        payload = dict(item.data(Qt.UserRole) or {})

        if payload.get("pending"):
            self._cleanup_temp_pending_attachment(payload)
        else:
            attachment_id = payload.get("id")
            if attachment_id is not None:
                self.removed_attachment_ids.append(int(attachment_id))

        attachment_id = payload.get("id")
        source_path = str(payload.get("source_path") or "")
        display_name = str(payload.get("display_name") or "")
        pending_flag = bool(payload.get("pending"))

        filtered: List[Dict[str, Any]] = []

        for attachment in self.attachments:
            if attachment_id is not None and attachment.get("id") == attachment_id:
                continue

            if pending_flag and source_path and str(attachment.get("source_path") or "") == source_path:
                continue

            if (
                pending_flag
                and not source_path
                and display_name
                and str(attachment.get("display_name") or "") == display_name
                and bool(attachment.get("pending")) == pending_flag
            ):
                continue

            filtered.append(attachment)

        self.attachments = filtered
        self._reload_attachments_list()

    def reject(self) -> None:
        self._cleanup_all_pending_temp_attachments()
        super().reject()

    def accept(self) -> None:
        if not self.edt_title.text().strip():
            QMessageBox.warning(self, "Titolo mancante", "Inserisci il titolo della nota.")
            return

        super().accept()

    def get_payload(self) -> Dict[str, Any]:
        pending_attachments = [
            dict(item)
            for item in self.attachments
            if bool(item.get("pending"))
        ]

        return {
            "title": self.edt_title.text().strip(),
            "body": self.txt_body.toPlainText().strip(),
            "thread_id": self.cmb_thread.currentData(),
            "pending_attachments": pending_attachments,
            "removed_attachment_ids": list(self.removed_attachment_ids),
        }

class CartesioDialog(QDialog):
    def __init__(self, service: JobService, job_id: int, scope: str, parent=None) -> None:
        super().__init__(parent)
        self.service = service
        self.job_id = int(job_id)
        self.scope = str(scope or "").strip().upper()
        self.bundle: Dict[str, Any] = {}
        self.threads: List[Dict[str, Any]] = []
        self.notes: List[Dict[str, Any]] = []
        self.checklist_dirty = False
        self.checklist_rows: List[CartesioChecklistRowWidget] = []

        self.setWindowTitle(f"Cartesio {self.scope} - Lavoro #{self.job_id}")
        self.resize(1500, 820)
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

        header_layout.addWidget(QLabel("Scope"), 0, 0)
        header_layout.addWidget(QLabel(self.scope), 0, 1)
        header_layout.addWidget(self.chk_active, 0, 2, 1, 2)
        header_layout.addWidget(QLabel("Stato"), 1, 0)
        header_layout.addWidget(self.cmb_status, 1, 1)
        header_layout.addWidget(QLabel("Referente"), 1, 2)
        header_layout.addWidget(self.edt_referente, 1, 3)

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
        self.btn_delete_thread = QPushButton("Elimina thread")

        self.btn_new_thread.clicked.connect(self._create_thread)
        self.btn_close_thread.clicked.connect(lambda: self._set_selected_thread_status("CHIUSO"))
        self.btn_reopen_thread.clicked.connect(lambda: self._set_selected_thread_status("APERTO"))
        self.btn_delete_thread.clicked.connect(self._delete_selected_thread)

        thread_btns.addWidget(self.btn_new_thread)
        thread_btns.addWidget(self.btn_close_thread)
        thread_btns.addWidget(self.btn_reopen_thread)
        thread_btns.addWidget(self.btn_delete_thread)
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

        checklist_panel = QWidget()
        checklist_layout = QVBoxLayout(checklist_panel)

        checklist_header = QHBoxLayout()
        self.lbl_checklist_title = QLabel("Checklist")
        self.lbl_checklist_summary = QLabel("0/0")
        self.lbl_checklist_summary.setTextInteractionFlags(Qt.TextSelectableByMouse)
        checklist_header.addWidget(self.lbl_checklist_title)
        checklist_header.addStretch(1)
        checklist_header.addWidget(self.lbl_checklist_summary)
        checklist_layout.addLayout(checklist_header)

        add_row = QHBoxLayout()
        self.edt_new_checklist = QLineEdit()
        self.edt_new_checklist.setPlaceholderText("Nuova voce checklist...")
        self.edt_new_checklist.returnPressed.connect(self._add_checklist_item)
        self.btn_add_checklist = QPushButton("Aggiungi voce")
        self.btn_add_checklist.clicked.connect(self._add_checklist_item)
        add_row.addWidget(self.edt_new_checklist, 1)
        add_row.addWidget(self.btn_add_checklist)
        checklist_layout.addLayout(add_row)

        helper_label = QLabel("Ogni voce mostra la nota sotto il testo principale.")
        helper_label.setWordWrap(True)
        checklist_layout.addWidget(helper_label)

        self.checklist_scroll = QScrollArea()
        self.checklist_scroll.setWidgetResizable(True)
        self.checklist_container = QWidget()
        self.checklist_box = QVBoxLayout(self.checklist_container)
        self.checklist_box.setContentsMargins(0, 0, 0, 0)
        self.checklist_box.setSpacing(6)
        self.checklist_box.addStretch(1)
        self.checklist_scroll.setWidget(self.checklist_container)
        checklist_layout.addWidget(self.checklist_scroll, 1)

        checklist_btns = QHBoxLayout()
        self.lbl_checklist_dirty = QLabel("")
        self.btn_save_checklist = QPushButton("Salva checklist")
        self.btn_save_checklist.clicked.connect(self._save_checklist)
        checklist_btns.addWidget(self.lbl_checklist_dirty)
        checklist_btns.addStretch(1)
        checklist_btns.addWidget(self.btn_save_checklist)
        checklist_layout.addLayout(checklist_btns)
        splitter.addWidget(checklist_panel)

        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 2)
        splitter.setStretchFactor(2, 3)

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
            idx = self.cmb_status.findText(str(entry.get("status") or "NON IMPOSTATO"))
            if idx >= 0:
                self.cmb_status.setCurrentIndex(idx)
            self.chk_active.setChecked(str(entry.get("cartesio_delivery_scope") or "NONE").upper() == self.scope)
        else:
            self.edt_referente.clear()
            idx = self.cmb_status.findText("NON IMPOSTATO")
            if idx >= 0:
                self.cmb_status.setCurrentIndex(idx)
            self.chk_active.setChecked(False)

        self._reload_threads_table()
        self._reload_notes_table()
        self._set_checklist_items(entry.get("checklist_json") or [])

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

    def _clear_checklist_rows(self) -> None:
        while self.checklist_box.count() > 1:
            item = self.checklist_box.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self.checklist_rows = []

    def _normalize_checklist_items(self, items: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            text_value = str(item.get("text") or "").strip()
            note_value = str(item.get("note") or "").strip()
            if not text_value:
                continue
            normalized.append(
                {
                    "text": text_value,
                    "done": bool(item.get("done")),
                    "note": note_value,
                }
            )
        return normalized

    def _sort_checklist_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        indexed_items = list(enumerate(items))
        indexed_items.sort(key=lambda pair: (1 if bool(pair[1].get("done")) else 0, pair[0]))
        return [dict(item) for _, item in indexed_items]

    def _set_checklist_items(self, items: Optional[List[Dict[str, Any]]], dirty: bool = False) -> None:
        ordered_items = self._sort_checklist_items(self._normalize_checklist_items(items))
        self._clear_checklist_rows()

        for item in ordered_items:
            row_widget = CartesioChecklistRowWidget(item, self.checklist_container)
            row_widget.changed.connect(self._on_checklist_changed)
            row_widget.done_toggled.connect(self._on_checklist_done_toggled)
            row_widget.remove_requested.connect(self._remove_checklist_row)
            self.checklist_rows.append(row_widget)
            self.checklist_box.insertWidget(self.checklist_box.count() - 1, row_widget)

        self._update_checklist_summary()
        self._set_checklist_dirty(dirty)

    def _collect_checklist_payload(self) -> List[Dict[str, Any]]:
        payload: List[Dict[str, Any]] = []
        for row_widget in self.checklist_rows:
            item = row_widget.get_payload()
            if item["text"]:
                payload.append(item)
        return payload

    def _update_checklist_summary(self) -> None:
        items = self._collect_checklist_payload()
        total = len(items)
        completed = sum(1 for item in items if bool(item.get("done")))
        suffix = " ✅" if total > 0 and completed == total else ""
        self.lbl_checklist_summary.setText(f"{completed}/{total}{suffix}")

    def _set_checklist_dirty(self, value: bool) -> None:
        self.checklist_dirty = bool(value)
        self.lbl_checklist_dirty.setText("Modifiche non salvate" if self.checklist_dirty else "")

    def _on_checklist_changed(self) -> None:
        self._update_checklist_summary()
        self._set_checklist_dirty(True)

    def _on_checklist_done_toggled(self) -> None:
        current_items = self._collect_checklist_payload()
        self._set_checklist_items(current_items, dirty=True)

    def _remove_checklist_row(self, row_widget: CartesioChecklistRowWidget) -> None:
        if row_widget not in self.checklist_rows:
            return

        payload_to_remove = row_widget.get_payload()
        text_value = str(payload_to_remove.get("text") or "").strip() or "questa voce"

        ans = QMessageBox.question(
            self,
            "Elimina voce checklist",
            f"Eliminare la voce checklist '{text_value}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if ans != QMessageBox.Yes:
            return

        remaining_items: List[Dict[str, Any]] = []
        for current_row in self.checklist_rows:
            if current_row is row_widget:
                continue
            payload = current_row.get_payload()
            if payload.get("text"):
                remaining_items.append(payload)

        self._set_checklist_items(remaining_items, dirty=True)

        if not self._save_checklist_to_db():
            self._load_bundle()

    def _add_checklist_item(self) -> None:
        text_value = self.edt_new_checklist.text().strip()
        if not text_value:
            QMessageBox.warning(self, "Checklist", "Scrivi il testo della voce prima di aggiungerla.")
            return

        items = self._collect_checklist_payload()
        items.append({"text": text_value, "done": False, "note": ""})

        self.edt_new_checklist.clear()
        self._set_checklist_items(items, dirty=True)

        if not self._save_checklist_to_db():
            self._load_bundle()

    def _save_checklist_to_db(self) -> bool:
        try:
            bundle = self.service.save_cartesio_checklist(
                job_id=self.job_id,
                scope=self.scope,
                checklist_json=self._collect_checklist_payload(),
            )
            self.bundle = bundle
            self._set_checklist_dirty(False)
            return True
        except Exception as exc:
            QMessageBox.critical(self, "Errore", f"Errore durante il salvataggio checklist:\n{exc}")
            return False

    def _save_checklist(self) -> None:
        self._save_checklist_to_db()

    def _prompt_save_checklist_if_dirty(self) -> bool:
        if not self.checklist_dirty:
            return True

        box = QMessageBox(self)
        box.setIcon(QMessageBox.Warning)
        box.setWindowTitle("Checklist non salvata")
        box.setText("La checklist Cartesio contiene modifiche non salvate.")
        btn_save = box.addButton("Salva", QMessageBox.AcceptRole)
        btn_discard = box.addButton("Scarta", QMessageBox.DestructiveRole)
        btn_cancel = box.addButton("Annulla", QMessageBox.RejectRole)
        box.exec()

        clicked = box.clickedButton()
        if clicked == btn_save:
            return self._save_checklist_to_db()
        if clicked == btn_discard:
            return True
        if clicked == btn_cancel:
            return False
        return False

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
        if not self._prompt_save_checklist_if_dirty():
            return

        bundle = self.service.save_cartesio_entry(
            job_id=self.job_id,
            scope=self.scope,
            referente=self.edt_referente.text().strip(),
            status=self.cmb_status.currentText(),
            is_active=self.chk_active.isChecked(),
        )
        warning_text = str(bundle.get("activation_warning") or "").strip()
        self._load_bundle()
        if warning_text:
            QMessageBox.warning(self, "Verifica passaggio PRG/COS", warning_text)

    def _create_thread(self) -> None:
        if not self._prompt_save_checklist_if_dirty():
            return

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
            is_active=self.chk_active.isChecked(),
        )

    def _set_selected_thread_status(self, status: str) -> None:
        if not self._prompt_save_checklist_if_dirty():
            return

        thread = self._selected_thread()
        if not thread:
            QMessageBox.information(self, "Thread", "Seleziona un thread.")
            return
        self.service.set_cartesio_thread_status(int(thread["id"]), status)
        self._load_bundle()

    def _delete_selected_thread(self) -> None:
        if not self._prompt_save_checklist_if_dirty():
            return

        thread = self._selected_thread()
        if not thread:
            QMessageBox.information(self, "Thread", "Seleziona un thread.")
            return

        thread_id = int(thread["id"])
        thread_title = str(thread.get("title") or "").strip()
        notes_count = int(thread.get("notes_count") or 0)

        ans = QMessageBox.question(
            self,
            "Elimina thread",
            (
                f"Eliminare il thread '{thread_title}'?\n\n"
                "ATTENZIONE:\n"
                "- verranno eliminate tutte le note del thread\n"
                "- verranno eliminati tutti gli allegati collegati\n\n"
                f"Numero note collegate: {notes_count}"
            ),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if ans != QMessageBox.Yes:
            return

        result = self.service.delete_cartesio_thread(thread_id)

        for attachment in result.get("attachments") or []:
            path = resolve_cartesio_attachment_path(str(attachment.get("stored_rel_path") or ""))
            try:
                if path.is_file():
                    path.unlink()
            except Exception:
                # Mantengo lo stesso approccio difensivo già usato nel dialog:
                # l'errore su singolo file fisico non deve impedire il refresh DB/UI.
                pass

        self._load_bundle()

        QMessageBox.information(
            self,
            "Thread eliminato",
            (
                "Thread eliminato correttamente.\n\n"
                f"Note eliminate: {int(result.get('notes_count') or 0)}\n"
                f"Allegati eliminati: {int(result.get('attachments_count') or 0)}"
            ),
        )

    def _create_note(self, force_selected_thread: bool = False) -> None:
        if not self._prompt_save_checklist_if_dirty():
            return

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
            thread_id=payload.get("thread_id"),
        )
        self._persist_pending_attachments(note, payload.get("pending_attachments") or [])
        self._load_bundle()

    def _edit_selected_note(self, *_args) -> None:
        if not self._prompt_save_checklist_if_dirty():
            return

        note = self._selected_note()
        if not note:
            QMessageBox.information(self, "Nota", "Seleziona una nota.")
            return
        dlg = CartesioNoteDialog(
            self,
            title=str(note.get("title") or ""),
            body=str(note.get("body") or ""),
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
            checklist_json=note.get("checklist_json") or [],
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
        self._persist_pending_attachments(updated_note, payload.get("pending_attachments") or [])
        self._load_bundle()

    def _delete_selected_note(self) -> None:
        if not self._prompt_save_checklist_if_dirty():
            return

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

    def _persist_pending_attachments(
        self,
        note: Dict[str, Any],
        pending_attachments: List[Dict[str, Any]],
    ) -> None:
        """Materializza gli allegati pending nella cartella definitiva Cartesio.

        Regole:
        - file desktop: copia normale del file esistente
        - mail Outlook: copia del .msg temporaneo estratto dal drag&drop
        - il DB salva anche i metadati già disponibili
        - i file temporanei vengono puliti in modo difensivo
        """
        if not note or not pending_attachments:
            return

        note_id = int(note["id"])
        bucket_name = f"note_{note_id}"
        target_dir = ensure_cartesio_attachment_dir(self.job_id, self.scope, bucket_name)

        errors: List[str] = []

        for attachment in pending_attachments:
            src = Path(str(attachment.get("source_path") or "").strip())
            if not src.is_file():
                errors.append(f"Sorgente non trovata: {src}")
                continue

            requested_name = str(attachment.get("display_name") or src.name).strip()
            attachment_kind = str(attachment.get("attachment_kind") or "file").strip() or "file"

            safe_name = safe_filename(requested_name, fallback=f"allegato_{note_id}")
            if attachment_kind == "outlook_msg" and not safe_name.lower().endswith(".msg"):
                safe_name += ".msg"

            dest = target_dir / safe_name
            counter = 1

            while dest.exists():
                dest = target_dir / f"{dest.stem}_{counter}{dest.suffix}"
                counter += 1

            try:
                shutil.copy2(src, dest)

                meta_json = dict(attachment.get("meta_json") or {})
                meta_json["persisted_from"] = str(src)
                meta_json["persisted_to"] = str(dest)

                self.service.add_cartesio_attachment(
                    note_id=note_id,
                    attachment_kind=attachment_kind,
                    stored_rel_path=build_cartesio_attachment_rel_path(dest),
                    display_name=dest.name,
                    subject=str(attachment.get("subject") or ""),
                    sender=str(attachment.get("sender") or ""),
                    received_at=str(attachment.get("received_at") or ""),
                    meta_json=meta_json,
                )
            except Exception as exc:
                errors.append(f"{requested_name}: {exc}")
            finally:
                if bool(attachment.get("temp_file")):
                    cleanup_temp_drop_file(str(src))

        if errors:
            QMessageBox.warning(
                self,
                "Allegati Cartesio",
                "Alcuni allegati non sono stati salvati correttamente:\n\n" + "\n".join(errors),
            )

    def accept(self) -> None:
        if not self._prompt_save_checklist_if_dirty():
            return
        super().accept()
