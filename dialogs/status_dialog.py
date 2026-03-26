# -*- coding: utf-8 -*-
from __future__ import annotations

from PySide6.QtWidgets import QComboBox, QDialog, QFormLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QTextEdit, QVBoxLayout


class StatusDialog(QDialog):
    def __init__(self, title: str, states: list[str], current_status: str = "NON IMPOSTATO", notes: str = "", manual_code: str = "", parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(420, 260)
        self._build_ui(states, current_status, notes, manual_code)

    def _build_ui(self, states, current_status, notes, manual_code):
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.cmb_status = QComboBox()
        self.cmb_status.addItems(states)
        idx = self.cmb_status.findText(current_status)
        if idx >= 0:
            self.cmb_status.setCurrentIndex(idx)
        form.addRow("Stato", self.cmb_status)

        self.edt_manual_code = QLineEdit(manual_code)
        self.edt_manual_code.setPlaceholderText("Codice manuale opzionale...")
        form.addRow("Codice manuale", self.edt_manual_code)

        self.txt_notes = QTextEdit(notes)
        form.addRow("Note", self.txt_notes)
        layout.addLayout(form)

        btns = QHBoxLayout()
        ok = QPushButton("Salva")
        cancel = QPushButton("Annulla")
        ok.clicked.connect(self.accept)
        cancel.clicked.connect(self.reject)
        btns.addStretch(1)
        btns.addWidget(ok)
        btns.addWidget(cancel)
        layout.addLayout(btns)

    def get_payload(self):
        return {
            "status": self.cmb_status.currentText(),
            "notes": self.txt_notes.toPlainText().strip(),
            "manual_code": self.edt_manual_code.text().strip(),
        }
