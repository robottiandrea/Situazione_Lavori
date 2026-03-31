# -*- coding: utf-8 -*-
"""Model Qt per la tabella principale."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor, QBrush, QFont

from utils import color_for_status, SCAN_OVERRIDEABLE_FIELDS


class JobsTableModel(QAbstractTableModel):
    COLUMNS = [
        {
            "key": "history_alert_display",
            "header": "",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 30,
        },
        {
            "key": "project_distretto_anno",
            "header": "Distretto\nAnno PRG",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 100,
        },
        {
            "key": "project_name",
            "header": "Cartella\nPRG",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "resize": "content_soft",
            "width": 260,
            "min_width": 180,
            "max_width": 520,
        },
        {
            "key": "project_rilievo",
            "header": "Rilievo",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 70,
        },
        {
            "key": "project_enti",
            "header": "Enti",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 30,
        },
        {
            "key": "project_revision",
            "header": "Disegni",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 70,
        },
        {
            "key": "permessi_revision",
            "header": "Permessi",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 70,
        },
        {
            "key": "permits_display",
            "header": "Permessi\nOttenuti",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 70,
        },
        {
            "key": "psc_display",
            "header": "PSC",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 30,
        },
        {
            "key": "project_tracciamento",
            "header": "File\nTracc.",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 70,
        },
        {
            "key": "cartesio_prg_display",
            "header": "Cartesio\nPRG",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 100,
        },
        {
            "key": "dl_distretto_anno",
            "header": "Distretto\nAnno DL",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 100,
        },
        {
            "key": "dl_name",
            "header": "Cartella\nDL",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "resize": "content_soft",
            "width": 260,
            "min_width": 180,
            "max_width": 520,
        },
        {
            "key": "dl_insert_date",
            "header": "Data\nIns.",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 70,
        },
        {
            "key": "rilievi_dl_display",
            "header": "Rilievo\nDL",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 70,
        },
        {
            "key": "cartesio_cos_display",
            "header": "Cartesio\nCOS",
            "align": Qt.AlignCenter,
            "resize": "fixed",
            "width": 100,
        },
    ]

    HEADERS = [col["header"] for col in COLUMNS]
    KEY_MAP = [col["key"] for col in COLUMNS]
    COLUMN_INDEX = {col["key"]: idx for idx, col in enumerate(COLUMNS)}
    OVERRIDEABLE_SCAN_FIELDS = set(SCAN_OVERRIDEABLE_FIELDS)

    def __init__(self) -> None:
        super().__init__()
        self._rows: List[Dict[str, Any]] = []

    @classmethod
    def column_index(cls, key: str) -> int:
        return cls.COLUMN_INDEX[key]

    @classmethod
    def column_key(cls, column: int) -> str:
        return cls.COLUMNS[column]["key"]

    @classmethod
    def column_config(cls, key_or_column: str | int) -> Dict[str, Any]:
        if isinstance(key_or_column, str):
            return cls.COLUMNS[cls.column_index(key_or_column)]
        return cls.COLUMNS[key_or_column]

    def set_rows(self, rows: List[Dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = rows
        self.endResetModel()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self.COLUMNS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None

        if orientation == Qt.Horizontal and 0 <= section < len(self.HEADERS):
            return self.HEADERS[section]

        if orientation == Qt.Vertical:
            return str(section + 1)

        return None

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder) -> None:
        if not (0 <= column < len(self.KEY_MAP)):
            return

        key = self.KEY_MAP[column]
        reverse = order == Qt.DescendingOrder

        self.layoutAboutToBeChanged.emit()
        self._rows.sort(key=lambda row: self._sort_key(row, key), reverse=reverse)
        self.layoutChanged.emit()

    def _sort_key(self, row: Dict[str, Any], key: str):
        text = self._display_value(row, key)
        return self._natural_key(text)

    @staticmethod
    def _natural_key(value: Any):
        text = "" if value is None else str(value).strip().lower()
        parts = re.split(r"(\d+)", text)
        normalized = []

        for part in parts:
            if part.isdigit():
                normalized.append(int(part))
            else:
                normalized.append(part)

        return tuple(normalized)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None

        row = self._rows[index.row()]
        col = index.column()
        key = self.KEY_MAP[col]
        override_fields = set(row.get("scan_override_fields") or [])

        if role == Qt.DisplayRole:
            return self._display_value(row, key)

        if role == Qt.ForegroundRole:
            color = self._foreground_color(row, key)
            if color:
                return QBrush(QColor(color))

        if role == Qt.ToolTipRole:
            if key == "history_alert_display":
                latest_ts = str(row.get("audit_latest_event_ts", "") or "").strip()
                latest_source = str(row.get("audit_latest_source_kind", "") or "").strip()
                latest_summary = str(row.get("audit_latest_summary", "") or "").strip()

                if row.get("history_alert_display") == "!":
                    parts = ["Modifiche non ancora controllate."]
                    if latest_ts:
                        parts.append(f"Ultimo evento: {latest_ts}")
                    if latest_source:
                        parts.append(f"Origine: {latest_source}")
                    if latest_summary:
                        parts.append(f"Dettaglio: {latest_summary}")
                    return "\n".join(parts)

                return "Nessuna modifica non ancora controllata."

            if key in override_fields:
                return "Valore sovrascritto manualmente. Tasto destro per ripristinare l'automatico."

            if key == "psc_display":
                psc_path = (row.get("psc_path") or "").strip()
                if psc_path:
                    return f"Percorso PSC:\n{psc_path}"
                return "Percorso PSC non impostato."

        if role == Qt.FontRole:
            font = QFont()
            changed = False

            if key == "history_alert_display" and self._display_value(row, key).strip():
                font.setBold(True)
                changed = True

            if key in override_fields:
                font.setUnderline(True)
                changed = True

            if key in {"project_revision", "permessi_revision"}:
                text = self._display_value(row, key).strip()
                if text and text != "-":
                    font.setBold(True)
                    changed = True

            if changed:
                return font

        if role == Qt.TextAlignmentRole:
            return int(self.COLUMNS[col]["align"])

        if role == Qt.UserRole:
            return row

        return None

    def _display_value(self, row: Dict[str, Any], key: str) -> str:
        scan = row.get("scan", {})

        if key == "history_alert_display":
            return str(row.get("history_alert_display", "") or "")

        if key in self.OVERRIDEABLE_SCAN_FIELDS:
            value = row.get(key)
            if value not in (None, ""):
                return str(value)

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
            return scan.get("rilievi_dl", {}).get("display", "❌")

        if key == "project_name":
            display_name = row.get("project_name_display")
            if display_name not in (None, ""):
                return str(display_name)
            return str(row.get("project_name", "") or "")

        value = row.get(key, "")
        return "" if value is None else str(value)

    def _foreground_color(self, row: Dict[str, Any], key: str) -> Optional[str]:
        if key == "history_alert_display":
            if str(row.get("history_alert_display", "") or "").strip():
                return "#d9534f"
            return None

        if key == "project_name":
            mode = str(row.get("project_mode", "") or "").strip().upper()
            if mode in {"ALTRA_DITTA", "PROGETTO_NON_PREVISTO"}:
                return "#0d6efd"

        if key in {"project_revision", "permessi_revision"}:
            permits_mode = str(row.get("permits_mode", "REQUIRED") or "REQUIRED").strip().upper()
            match_status = row.get("revisions_match")

            if permits_mode == "NOT_REQUIRED":
                if key == "project_revision":
                    text = str(row.get("project_revision", "") or "").strip()
                    if text and text != "-":
                        return "#198754"
                return None

            if match_status == "NOT_APPLICABLE":
                return None
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
        row_index = self.find_row_index_by_id(job_id)
        if row_index < 0:
            return False

        self._rows[row_index] = updated_row

        top_left = self.index(row_index, 0)
        bottom_right = self.index(row_index, self.columnCount() - 1)

        self.dataChanged.emit(
            top_left,
            bottom_right,
            [
                Qt.DisplayRole,
                Qt.ForegroundRole,
                Qt.FontRole,
                Qt.ToolTipRole,
                Qt.TextAlignmentRole,
                Qt.UserRole,
            ],
        )
        return True

class CartesioTableModel(QAbstractTableModel):
    COLUMNS = [
        {"key": "project_distretto_anno", "header": "Distretto\nPRG", "align": Qt.AlignCenter, "width": 95},
        {"key": "project_name_display", "header": "Cartella\nPRG", "align": Qt.AlignVCenter | Qt.AlignLeft, "width": 220},
        {"key": "entry_status", "header": "Stato", "align": Qt.AlignCenter, "width": 110},
        {"key": "referente", "header": "Referente", "align": Qt.AlignVCenter | Qt.AlignLeft, "width": 150},
        {"key": "checklist_display", "header": "Checklist", "align": Qt.AlignCenter, "width": 80},
        {"key": "dl_distretto_anno", "header": "Distretto\nDL", "align": Qt.AlignCenter, "width": 95},
        {"key": "dl_name", "header": "Cartella\nDL", "align": Qt.AlignVCenter | Qt.AlignLeft, "width": 220},
        {"key": "display_last_activity", "header": "Ultima\nattività", "align": Qt.AlignCenter, "width": 130},
        {"key": "open_threads", "header": "Thread\naperti", "align": Qt.AlignCenter, "width": 80},
        {"key": "latest_note_title", "header": "Ultima nota", "align": Qt.AlignVCenter | Qt.AlignLeft, "width": 240},
    ]

    def __init__(self) -> None:
        super().__init__()
        self._rows: List[Dict[str, Any]] = []

    def set_rows(self, rows: List[Dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = list(rows or [])
        self.endResetModel()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self.COLUMNS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self.COLUMNS):
            return self.COLUMNS[section]["header"]
        if orientation == Qt.Vertical:
            return str(section + 1)
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None

        row = self._rows[index.row()]
        column = self.COLUMNS[index.column()]
        key = column["key"]
        value = row.get(key, "")

        if role == Qt.DisplayRole:
            return "" if value is None else str(value)

        if role == Qt.TextAlignmentRole:
            return int(column["align"])

        if role == Qt.ForegroundRole:
            if key == "entry_status":
                color = color_for_status(str(value or ""))
                if color:
                    return QBrush(QColor(color))

            if key == "checklist_display" and str(value or "").strip() == "✅":
                return QBrush(QColor("#198754"))

        if role == Qt.FontRole:
            if key == "latest_note_title" and str(value or "").strip():
                font = QFont()
                font.setBold(True)
                return font

            if key == "checklist_display" and str(value or "").strip() == "✅":
                font = QFont()
                font.setBold(True)
                return font

        if role == Qt.ToolTipRole and key == "checklist_display":
            display_value = str(value or "").strip()
            if display_value == "-":
                return "Checklist vuota"
            if display_value == "✅":
                return "Checklist completata"
            return f"Checklist in corso: {display_value}"

        if role == Qt.UserRole:
            return row

        return None

    def get_row(self, row_index: int) -> Optional[Dict[str, Any]]:
        if 0 <= row_index < len(self._rows):
            return self._rows[row_index]
        return None