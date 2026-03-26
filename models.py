# -*- coding: utf-8 -*-
"""Model Qt per la tabella principale."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor, QBrush, QFont

from utils import color_for_status


class JobsTableModel(QAbstractTableModel):
    HEADERS = [
        "Distretto/Anno PRG",
        "Nome Progetto",
        "Rilievo PRG",
        "Enti",
        "Rev Progetto",
        "Rev Permessi",
        "Permessi",
        "Tracciamento",
        "Cartesio PRG",
        "Distretto/Anno DL",
        "Nome DL",
        "Inserimento",
        "Rilievi DL",
        "Cartesio COS",
    ]

    KEY_MAP = [
        "project_distretto_anno",
        "project_name",
        "project_rilievo",
        "project_enti",
        "project_revision",
        "permessi_revision",
        "permits_display",
        "project_tracciamento",
        "cartesio_prg_display",
        "dl_distretto_anno",
        "dl_name",
        "dl_insert_date",
        "rilievi_dl_display",
        "cartesio_cos_display",
    ]

    def __init__(self) -> None:
        super().__init__()
        self._rows: List[Dict[str, Any]] = []

    def set_rows(self, rows: List[Dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = rows
        self.endResetModel()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self.HEADERS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return self.HEADERS[section]
        return str(section + 1)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None

        row = self._rows[index.row()]
        col = index.column()
        key = self.KEY_MAP[col]

        if role == Qt.DisplayRole:
            return self._display_value(row, key)

        if role == Qt.ForegroundRole:
            color = self._foreground_color(row, key)
            if color:
                return QBrush(QColor(color))

        if role == Qt.TextAlignmentRole:
            if col in {0, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13}:
                return int(Qt.AlignCenter)
            return int(Qt.AlignVCenter | Qt.AlignLeft)

        if role == Qt.UserRole:
            return row

        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None

        row = self._rows[index.row()]
        col = index.column()
        key = self.KEY_MAP[col]

        if role == Qt.DisplayRole:
            return self._display_value(row, key)

        if role == Qt.ForegroundRole:
            color = self._foreground_color(row, key)
            if color:
                return QBrush(QColor(color))

        if role == Qt.FontRole:
            if key in {"project_revision", "permessi_revision"}:
                text = self._display_value(row, key).strip()
                if text:
                    font = QFont()
                    font.setBold(True)
                    return font

        if role == Qt.TextAlignmentRole:
            if col in {0, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13}:
                return int(Qt.AlignCenter)
            return int(Qt.AlignVCenter | Qt.AlignLeft)

        if role == Qt.UserRole:
            return row

        return None
        
    def _display_value(self, row: Dict[str, Any], key: str) -> str:
        scan = row.get("scan", {})

        if key == "project_rilievo":
            return scan.get("project_rilievo", {}).get("status", "")

        if key == "project_enti":
            return scan.get("project_enti", {}).get("status", "")

        if key == "project_revision":
            return scan.get("project_revision", {}).get("display", "")

        if key == "permessi_revision":
            return scan.get("permessi_revision", {}).get("display", "")

        if key == "project_tracciamento":
            return scan.get("project_tracciamento", {}).get("status", "")

        if key == "rilievi_dl_display":
            value = row.get("rilievi_dl_display", "")
            if value not in (None, ""):
                return str(value)
            return scan.get("rilievi_dl", {}).get("display", "❌")

        value = row.get(key, "")
        return "" if value is None else str(value)

    def _foreground_color(self, row: Dict[str, Any], key: str) -> Optional[str]:
        if key in {"project_revision", "permessi_revision"}:
            match_status = row.get("revisions_match")
            if match_status == "MATCH":
                return "#198754"
            if match_status == "MISMATCH":
                return "#d9534f"
            return "#f0ad4e"

        if key == "cartesio_prg_display":
            return color_for_status(row.get("cartesio_prg_status", ""))

        if key == "rilievi_dl_display":
            return color_for_status(row.get("rilievi_dl_status", ""))

        if key == "cartesio_cos_display":
            return color_for_status(row.get("cartesio_cos_status", ""))

        return None

    def get_row(self, row_index: int) -> Optional[Dict[str, Any]]:
        if 0 <= row_index < len(self._rows):
            return self._rows[row_index]
        return None

    def find_row_index_by_id(self, job_id: int) -> int:
        for row_index, row in enumerate(self._rows):
            if row.get("id") == job_id:
                return row_index
        return -1

    def update_row_by_id(self, job_id: int, updated_row: Dict[str, Any]) -> bool:
        """
        Aggiorna una sola riga del model e notifica la vista senza reset completo.
        """
        row_index = self.find_row_index_by_id(job_id)
        if row_index < 0:
            return False

        self._rows[row_index] = updated_row

        top_left = self.index(row_index, 0)
        bottom_right = self.index(row_index, self.columnCount() - 1)

        self.dataChanged.emit(
            top_left,
            bottom_right,
            [Qt.DisplayRole, Qt.ForegroundRole, Qt.FontRole, Qt.TextAlignmentRole, Qt.UserRole],
        )
        return True