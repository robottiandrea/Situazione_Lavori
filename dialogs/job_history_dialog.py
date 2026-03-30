# -*- coding: utf-8 -*-
"""Dialog storico modifiche di una singola riga."""
from __future__ import annotations

from typing import Any, Dict, List

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from database import DatabaseManager
from utils import get_current_user_name


class JobHistoryDialog(QDialog):
    SOURCE_OPTIONS = [
        ("Tutto", ""),
        ("Manuale", "manual"),
        ("Scan", "scan"),
        ("Override", "override"),
        ("Autofill", "autofill"),
        ("System", "system"),
    ]

    def __init__(self, db: DatabaseManager, job: Dict[str, Any], parent=None) -> None:
        super().__init__(parent)
        self.db = db
        self.job = dict(job or {})
        self.user_name = get_current_user_name()
        self.history_state_changed = False

        self.setWindowTitle(f"Storico lavoro #{self.job.get('id')}")
        self.resize(1200, 700)

        self._build_ui()
        self._load_history()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)

        title_parts = [f"Lavoro #{self.job.get('id')}"]
        project_name = str(self.job.get("project_name_display") or self.job.get("project_name") or "").strip()
        dl_name = str(self.job.get("dl_name") or "").strip()

        if project_name:
            title_parts.append(f"PRG: {project_name}")
        if dl_name:
            title_parts.append(f"DL: {dl_name}")

        self.lbl_title = QLabel(" | ".join(title_parts))
        self.lbl_title.setTextInteractionFlags(Qt.TextSelectableByMouse)
        root.addWidget(self.lbl_title)

        top_bar = QHBoxLayout()
        top_bar.addWidget(QLabel("Filtro origine"))

        self.cmb_source = QComboBox()
        for label, value in self.SOURCE_OPTIONS:
            self.cmb_source.addItem(label, value)
        self.cmb_source.currentIndexChanged.connect(self._load_history)
        top_bar.addWidget(self.cmb_source)

        self.btn_refresh = QPushButton("Aggiorna")
        self.btn_refresh.clicked.connect(self._load_history)
        top_bar.addWidget(self.btn_refresh)

        self.btn_mark_checked = QPushButton("Segna riga come controllata")
        self.btn_mark_checked.clicked.connect(self._mark_row_checked)
        top_bar.addWidget(self.btn_mark_checked)

        top_bar.addStretch(1)

        self.lbl_status = QLabel("")
        self.lbl_status.setTextInteractionFlags(Qt.TextSelectableByMouse)
        top_bar.addWidget(self.lbl_status)

        root.addLayout(top_bar)

        self.tbl_events = QTableWidget(0, 5, self)
        self.tbl_events.setHorizontalHeaderLabels(
            ["Data/Ora", "Tipo", "Origine", "Utente", "Riepilogo"]
        )
        self.tbl_events.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbl_events.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tbl_events.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl_events.verticalHeader().setVisible(False)
        self.tbl_events.horizontalHeader().setStretchLastSection(True)
        self.tbl_events.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.tbl_events.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.tbl_events.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.tbl_events.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.tbl_events.itemSelectionChanged.connect(self._load_selected_event_changes)
        root.addWidget(self.tbl_events, 3)

        self.tbl_changes = QTableWidget(0, 4, self)
        self.tbl_changes.setHorizontalHeaderLabels(
            ["Scope", "Campo", "Valore precedente", "Nuovo valore"]
        )
        self.tbl_changes.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tbl_changes.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tbl_changes.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl_changes.verticalHeader().setVisible(False)
        self.tbl_changes.horizontalHeader().setStretchLastSection(True)
        self.tbl_changes.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.tbl_changes.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        root.addWidget(self.tbl_changes, 2)

        bottom_bar = QHBoxLayout()
        bottom_bar.addStretch(1)

        self.btn_close = QPushButton("Chiudi")
        self.btn_close.clicked.connect(self.accept)
        bottom_bar.addWidget(self.btn_close)

        root.addLayout(bottom_bar)

    def _make_item(self, value: Any) -> QTableWidgetItem:
        item = QTableWidgetItem("" if value is None else str(value))
        item.setFlags(item.flags() & ~Qt.ItemIsEditable)
        return item

    def _load_history(self) -> None:
        source_kind = str(self.cmb_source.currentData() or "")
        events = self.db.fetch_job_history_events(
            job_id=int(self.job["id"]),
            limit=500,
            source_kind=source_kind,
        )

        self.tbl_events.setRowCount(len(events))

        for row_index, event in enumerate(events):
            item_ts = self._make_item(event.get("event_ts", ""))
            item_ts.setData(Qt.UserRole, int(event["id"]))
            self.tbl_events.setItem(row_index, 0, item_ts)
            self.tbl_events.setItem(row_index, 1, self._make_item(event.get("action_kind", "")))
            self.tbl_events.setItem(row_index, 2, self._make_item(event.get("source_kind", "")))
            self.tbl_events.setItem(row_index, 3, self._make_item(event.get("initiated_by", "")))
            self.tbl_events.setItem(row_index, 4, self._make_item(event.get("summary", "")))

        if events:
            self.tbl_events.selectRow(0)
        else:
            self.tbl_changes.setRowCount(0)

        latest_event_id = int(self.job.get("audit_latest_event_id") or 0)
        last_seen_event_id = self.db.get_job_last_seen_event_id(int(self.job["id"]), self.user_name)
        pending = 1 if latest_event_id > last_seen_event_id else 0

        self.lbl_status.setText(
            f"Utente: {self.user_name} | Ultimo evento: {latest_event_id} | "
            f"Ultimo controllato: {last_seen_event_id} | Pendenti: {pending}"
        )

    def _load_selected_event_changes(self) -> None:
        selected = self.tbl_events.selectedItems()
        if not selected:
            self.tbl_changes.setRowCount(0)
            return

        event_id = int(self.tbl_events.item(self.tbl_events.currentRow(), 0).data(Qt.UserRole))
        changes = self.db.fetch_job_history_changes(event_id)

        self.tbl_changes.setRowCount(len(changes))
        for row_index, change in enumerate(changes):
            self.tbl_changes.setItem(row_index, 0, self._make_item(change.get("field_scope", "")))
            self.tbl_changes.setItem(row_index, 1, self._make_item(change.get("field_key", "")))
            self.tbl_changes.setItem(row_index, 2, self._make_item(change.get("old_value_text", "")))
            self.tbl_changes.setItem(row_index, 3, self._make_item(change.get("new_value_text", "")))

    def _mark_row_checked(self) -> None:
        self.db.mark_job_history_checked(int(self.job["id"]), self.user_name)
        self.history_state_changed = True
        self._load_history()