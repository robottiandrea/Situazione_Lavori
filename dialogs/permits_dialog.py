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
        layout = QVBoxLayout(self)

        top = QHBoxLayout()
        self.edt_new_item = QLineEdit()
        self.edt_new_item.setPlaceholderText("Nuovo permesso personalizzato...")
        self.btn_add = QPushButton("Aggiungi")
        self.btn_add.clicked.connect(self.add_item)
        top.addWidget(self.edt_new_item)
        top.addWidget(self.btn_add)
        layout.addLayout(top)

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.container = QWidget()
        self.grid = QGridLayout(self.container)
        self.grid.setColumnStretch(3, 1)
        self.scroll.setWidget(self.container)
        layout.addWidget(self.scroll)

        self.txt_notes = QTextEdit()
        self.txt_notes.setPlaceholderText("Note generali permessi...")
        layout.addWidget(QLabel("Note"))
        layout.addWidget(self.txt_notes)

        btns = QHBoxLayout()
        self.btn_ok = QPushButton("Salva")
        self.btn_cancel = QPushButton("Annulla")
        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)
        btns.addStretch(1)
        btns.addWidget(self.btn_ok)
        btns.addWidget(self.btn_cancel)
        layout.addLayout(btns)

        self.rows = []

    def _load_data(self, notes):
        items = self._checklist[:] if self._checklist else [
            {"name": name, "required": False, "obtained": False, "notes": ""}
            for name in PERMIT_DEFAULT_ITEMS
        ]
        for item in items:
            self._append_row(item)
        self.txt_notes.setPlainText(notes)

    def _append_row(self, item):
        row = len(self.rows)
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
