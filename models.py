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
            "key": "cartesio_acc_prg_display",
            "header": "Cartesio\nACC",
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
        {
            "key": "cartesio_acc_cos_display",
            "header": "Cartesio\nACC",
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
                latest_ts = str(row.get("history_alert_event_ts", "") or "").strip()
                latest_source = str(row.get("history_alert_source_kind", "") or "").strip()
                latest_summary = str(row.get("history_alert_summary", "") or "").strip()
                latest_user = str(row.get("history_alert_initiated_by", "") or "").strip()
                display = str(row.get("history_alert_display", "") or "").strip()
                is_exception = str(row.get("exception_mode", "") or "").strip().upper() == "MANUAL"
                exception_reason = str(row.get("exception_reason", "") or "").strip()
                exception_group = str(row.get("exception_group_code", "") or "").strip()

                parts = []

                if is_exception:
                    parts.append("Riga eccezione manuale.")
                    if exception_group:
                        parts.append(f"Gruppo eccezione: {exception_group}")
                    if exception_reason:
                        parts.append(f"Motivo: {exception_reason}")

                if "!" in display:
                    parts.append("Modifiche non ancora controllate.")
                    if latest_ts:
                        parts.append(f"Ultimo evento rilevante: {latest_ts}")
                    if latest_source:
                        parts.append(f"Origine: {latest_source}")
                    if latest_user:
                        parts.append(f"Utente: {latest_user}")
                    if latest_summary:
                        parts.append(f"Dettaglio: {latest_summary}")

                if parts:
                    return "\n".join(parts)

                return "Nessuna modifica non ancora controllata."

            if key == "project_tracciamento":
                parts = []

                if key in override_fields:
                    parts.append("Valore sovrascritto manualmente. Tasto destro per ripristinare l'automatico.")

                manual_path = (row.get("project_tracciamento_manual_path") or "").strip()
                project_mode = str(row.get("project_mode", "") or "").strip().upper()

                if manual_path:
                    parts.append(f"Percorso manuale tracciamento:\n{manual_path}")
                elif project_mode == "ALTRA_DITTA":
                    parts.append("Nessun link manuale tracciamento impostato.")

                if parts:
                    return "\n\n".join(parts)

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

        row_first_keys = {
            "project_rilievo",
            "project_enti",
            "project_revision",
            "permessi_revision",
            "permits_display",
            "psc_display",
            "project_tracciamento",
            "cartesio_prg_display",
            "cartesio_acc_prg_display",
            "rilievi_dl_display",
            "cartesio_cos_display",
            "cartesio_acc_cos_display",
        }

        if key in row_first_keys:
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
            display = str(row.get("history_alert_display", "") or "").strip()
            if not display:
                return None
            if "!" in display:
                return "#d9534f"
            if display == "E":
                return "#0d6efd"
            return None

        if key == "project_name":
            mode = str(row.get("project_mode", "") or "").strip().upper()
            if mode in {"ALTRA_DITTA", "PROGETTO_NON_PREVISTO"}:
                return "#0d6efd"

        if key in {"project_revision", "permessi_revision"}:
            permits_mode = str(row.get("permits_mode", "REQUIRED") or "REQUIRED").strip().upper()
            match_status = row.get("revisions_match")

            project_revision_text = str(row.get("project_revision", "") or "").strip()
            permessi_revision_text = str(row.get("permessi_revision", "") or "").strip()

            if project_revision_text == "-" and permessi_revision_text == "-":
                return None

            if permits_mode == "NOT_REQUIRED":
                if key == "project_revision":
                    if project_revision_text and project_revision_text != "-":
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

        if key in {"cartesio_acc_prg_display", "cartesio_acc_cos_display"}:
            return color_for_status(row.get("cartesio_acc_status", ""))

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
    PRG_COLUMNS = [
        {
            "key": "cartesio_prg_display",
            "header": "Cartesio\nPRG",
            "align": Qt.AlignCenter,
            "width": 100,
        },
        {
            "key": "project_distretto_anno",
            "header": "Distretto\nPRG",
            "align": Qt.AlignCenter,
            "width": 95,
        },
        {
            "key": "project_name_display",
            "header": "Cartella\nPRG",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "width": 260,
        },
        {
            "key": "entry_status",
            "header": "Stato",
            "align": Qt.AlignCenter,
            "width": 120,
        },
        {
            "key": "checklist_display",
            "header": "Checklist",
            "align": Qt.AlignCenter,
            "width": 80,
        },
        {
            "key": "latest_note_title",
            "header": "Ultima nota",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "width": 280,
        },
        {
            "key": "display_last_activity",
            "header": "Ultima attività",
            "align": Qt.AlignCenter,
            "width": 115,
        },
        {
            "key": "referente",
            "header": "Referente",
            "align": Qt.AlignCenter,
            "width": 170,
        },
    ]

    COS_COLUMNS = [
        {
            "key": "cartesio_cos_display",
            "header": "Cartesio\nCOS",
            "align": Qt.AlignCenter,
            "width": 100,
        },
        {
            "key": "dl_distretto_anno",
            "header": "Distretto\nDL",
            "align": Qt.AlignCenter,
            "width": 95,
        },
        {
            "key": "dl_name",
            "header": "Cartella\nDL",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "width": 260,
        },
        {
            "key": "entry_status",
            "header": "Stato",
            "align": Qt.AlignCenter,
            "width": 120,
        },
        {
            "key": "checklist_display",
            "header": "Checklist",
            "align": Qt.AlignCenter,
            "width": 80,
        },
        {
            "key": "latest_note_title",
            "header": "Ultima nota",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "width": 280,
        },
        {
            "key": "display_last_activity",
            "header": "Ultima attività",
            "align": Qt.AlignCenter,
            "width": 115,
        },
        {
            "key": "referente",
            "header": "Referente",
            "align": Qt.AlignCenter,
            "width": 170,
        },
        {
            "key": "project_distretto_anno",
            "header": "Distretto\nPRG",
            "align": Qt.AlignCenter,
            "width": 95,
        },
        {
            "key": "project_name_display",
            "header": "Cartella\nPRG",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "width": 260,
        },
        {
            "key": "cartesio_prg_display",
            "header": "Cartesio\nPRG",
            "align": Qt.AlignCenter,
            "width": 100,
        },
    ]

    ACC_COLUMNS = [
        {
            "key": "cartesio_acc_display",
            "header": "Cartesio\nACC",
            "align": Qt.AlignCenter,
            "width": 100,
        },
        {
            "key": "dl_distretto_anno",
            "header": "Distretto\nDL",
            "align": Qt.AlignCenter,
            "width": 95,
        },
        {
            "key": "dl_name",
            "header": "Cartella\nDL",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "width": 260,
        },
        {
            "key": "entry_status",
            "header": "Stato",
            "align": Qt.AlignCenter,
            "width": 120,
        },
        {
            "key": "checklist_display",
            "header": "Checklist",
            "align": Qt.AlignCenter,
            "width": 80,
        },
        {
            "key": "latest_note_title",
            "header": "Ultima nota",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "width": 280,
        },
        {
            "key": "display_last_activity",
            "header": "Ultima attività",
            "align": Qt.AlignCenter,
            "width": 115,
        },
        {
            "key": "referente",
            "header": "Referente",
            "align": Qt.AlignCenter,
            "width": 170,
        },
        {
            "key": "project_distretto_anno",
            "header": "Distretto\nPRG",
            "align": Qt.AlignCenter,
            "width": 95,
        },
        {
            "key": "project_name_display",
            "header": "Cartella\nPRG",
            "align": Qt.AlignVCenter | Qt.AlignLeft,
            "width": 260,
        },
        {
            "key": "cartesio_prg_display",
            "header": "Cartesio\nPRG",
            "align": Qt.AlignCenter,
            "width": 100,
        },
    ]

    def __init__(self, scope: str = "PRG") -> None:
        super().__init__()
        normalized_scope = str(scope or "").strip().upper()
        self.scope = normalized_scope if normalized_scope in {"PRG", "COS", "ACC"} else "PRG"

        if self.scope == "COS":
            self.columns = list(self.COS_COLUMNS)
        elif self.scope == "ACC":
            self.columns = list(self.ACC_COLUMNS)
        else:
            self.columns = list(self.PRG_COLUMNS)

        self._rows: List[Dict[str, Any]] = []

    def set_rows(self, rows: List[Dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = list(rows or [])
        self.endResetModel()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self.columns)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None

        if orientation == Qt.Horizontal and 0 <= section < len(self.columns):
            return self.columns[section]["header"]

        if orientation == Qt.Vertical:
            return str(section + 1)

        return None

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder) -> None:
        if not (0 <= column < len(self.columns)):
            return

        key = self.columns[column]["key"]
        reverse = order == Qt.DescendingOrder

        self.layoutAboutToBeChanged.emit()
        self._rows.sort(
            key=lambda row: self._sort_key(row, key),
            reverse=reverse,
        )
        self.layoutChanged.emit()

    def _sort_key(self, row: Dict[str, Any], key: str):
        value = row.get(key, "")

        if key == "display_last_activity":
            return self._date_sort_key(value)

        if key == "checklist_display":
            return self._checklist_sort_key(value)

        return self._natural_key(value)

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

    @staticmethod
    def _date_sort_key(value: Any):
        text = str(value or "").strip()

        if not text or text == "-":
            return (0, 0, 0, 0)

        if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
            try:
                year = int(text[0:4])
                month = int(text[5:7])
                day = int(text[8:10])
                return (1, year, month, day)
            except ValueError:
                pass

        return (0, 0, 0, 0)

    @staticmethod
    def _checklist_sort_key(value: Any):
        text = str(value or "").strip()

        if not text or text == "-":
            return (0, 0, 0)

        if text == "✅":
            return (2, 9999, 9999)

        match = re.match(r"^\s*(\d+)\s*/\s*(\d+)\s*$", text)
        if match:
            done = int(match.group(1))
            total = int(match.group(2))
            return (1, done, total)

        return (0, 0, 0)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None

        row = self._rows[index.row()]
        column = self.columns[index.column()]
        key = column["key"]
        value = row.get(key, "")

        if role == Qt.DisplayRole:
            return "" if value is None else str(value)

        if role == Qt.TextAlignmentRole:
            return int(column["align"])

        if role == Qt.ForegroundRole:
            if key == "project_name_display":
                mode = str(row.get("project_mode", "") or "").strip().upper()
                if mode in {"ALTRA_DITTA", "PROGETTO_NON_PREVISTO"}:
                    return QBrush(QColor("#0d6efd"))

            if key == "entry_status":
                color = color_for_status(str(value or ""))
                if color:
                    return QBrush(QColor(color))

            if key in {"cartesio_prg_display", "cartesio_cos_display", "cartesio_acc_display"}:
                color = color_for_status(str(value or ""))
                if color:
                    return QBrush(QColor(color))

            if key == "checklist_display" and str(value or "").strip() == "✅":
                return QBrush(QColor("#198754"))
            
        if role == Qt.FontRole:
            if key == "latest_note_title" and str(value or "").strip() not in {"", "-"}:
                font = QFont()
                font.setBold(True)
                return font

            if key == "checklist_display" and str(value or "").strip() == "✅":
                font = QFont()
                font.setBold(True)
                return font

        if role == Qt.ToolTipRole:
            if key == "checklist_display":
                display_value = str(value or "").strip()
                if display_value == "-":
                    return "Checklist vuota"
                if display_value == "✅":
                    return "Checklist completata"
                return f"Checklist in corso: {display_value}"

            if key == "latest_note_title":
                return "" if value is None else str(value)

        if role == Qt.UserRole:
            return row

        return None

    def get_row(self, row_index: int) -> Optional[Dict[str, Any]]:
        if 0 <= row_index < len(self._rows):
            return self._rows[row_index]
        return None