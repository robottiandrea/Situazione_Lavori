# -*- coding: utf-8 -*-
from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from utils import PERMIT_DEFAULT_ITEMS


class PermitsDialog(QDialog):
    def __init__(self, parent=None, checklist=None, notes=""):
        super().__init__(parent)
        self.setWindowTitle("Checklist Permessi")
        self.resize(700, 500)
        self._checklist = checklist or []
        self._build_ui()
        self._load_data(notes)

    def _build_ui(self):
        self.rows = []

        root = QVBoxLayout(self)

        # ------------------------------------------------------------------
        # CHECKLIST (scroll)
        # ------------------------------------------------------------------
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_content = QWidget()
        self.grid = QGridLayout(scroll_content)
        self.grid.setContentsMargins(0, 0, 0, 0)
        self.grid.setHorizontalSpacing(12)
        self.grid.setVerticalSpacing(6)

        # Header
        self.grid.addWidget(QLabel("<b>Permesso</b>"), 0, 0)
        self.grid.addWidget(QLabel("<b>Richiesto</b>"), 0, 1)
        self.grid.addWidget(QLabel("<b>Ottenuto</b>"), 0, 2)
        self.grid.addWidget(QLabel("<b>Note</b>"), 0, 3)

        scroll.setWidget(scroll_content)
        root.addWidget(scroll)

        # ------------------------------------------------------------------
        # Aggiunta nuova voce
        # ------------------------------------------------------------------
        add_row = QHBoxLayout()
        self.edt_new_item = QLineEdit()
        self.edt_new_item.setPlaceholderText("Nuovo permesso...")
        btn_add = QPushButton("Aggiungi")
        btn_add.clicked.connect(self.add_item)
        add_row.addWidget(self.edt_new_item, 1)
        add_row.addWidget(btn_add)
        root.addLayout(add_row)

        # ------------------------------------------------------------------
        # Note generali
        # ------------------------------------------------------------------
        root.addWidget(QLabel("Note generali"))
        self.txt_notes = QTextEdit()
        self.txt_notes.setPlaceholderText("Note...")
        root.addWidget(self.txt_notes, 1)

        # ------------------------------------------------------------------
        # Pulsanti OK / Annulla
        # ------------------------------------------------------------------
        btns = QHBoxLayout()
        btns.addStretch(1)
        btn_ok = QPushButton("OK")
        btn_cancel = QPushButton("Annulla")
        btn_ok.clicked.connect(self.accept)
        btn_cancel.clicked.connect(self.reject)
        btns.addWidget(btn_ok)
        btns.addWidget(btn_cancel)
        root.addLayout(btns)

    def _load_data(self, notes):
        items = self._checklist[:] if self._checklist else [
            {"name": name, "required": False, "obtained": False, "notes": ""}
            for name in PERMIT_DEFAULT_ITEMS
        ]
        for item in items:
            self._append_row(item)
        self.txt_notes.setPlainText(notes)

    def _append_row(self, item):
        # +1 perché la riga 0 è l'header
        row = len(self.rows) + 1
        lbl = QLabel(item.get("name", ""))
        chk_required = QCheckBox("Richiesto")
        chk_required.setChecked(bool(item.get("required")))
        chk_obtained = QCheckBox("Ottenuto")
        chk_obtained.setChecked(bool(item.get("obtained")))
        edt_notes = QLineEdit(item.get("notes", ""))
        edt_notes.setPlaceholderText("Note...")

        self.grid.addWidget(lbl, row, 0)
        self.grid.addWidget(chk_required, row, 1)
        self.grid.addWidget(chk_obtained, row, 2)
        self.grid.addWidget(edt_notes, row, 3)
        self.rows.append((lbl, chk_required, chk_obtained, edt_notes))

    def add_item(self):
        name = self.edt_new_item.text().strip()
        if not name:
            QMessageBox.warning(self, "Voce vuota", "Scrivi un nome prima di aggiungere il permesso.")
            return
        self._append_row({"name": name, "required": False, "obtained": False, "notes": ""})
        self.edt_new_item.clear()

    def get_payload(self):
        checklist = []
        for lbl, chk_required, chk_obtained, edt_notes in self.rows:
            checklist.append(
                {
                    "name": lbl.text(),
                    "required": chk_required.isChecked(),
                    "obtained": chk_obtained.isChecked(),
                    "notes": edt_notes.text().strip(),
                }
            )
        return checklist, self.txt_notes.toPlainText().strip()
