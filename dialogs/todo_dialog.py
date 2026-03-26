# -*- coding: utf-8 -*-
from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)


class TodoDialog(QDialog):
    def __init__(self, parent=None, todo_items=None):
        super().__init__(parent)
        self.setWindowTitle("ToDo generale lavoro")
        self.resize(600, 420)
        self.items = todo_items or []
        self.rows = []
        self._build_ui()
        self._load_data()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        top = QHBoxLayout()
        self.edt_new = QLineEdit()
        self.edt_new.setPlaceholderText("Nuovo task...")
        self.btn_add = QPushButton("Aggiungi")
        self.btn_add.clicked.connect(self.add_item)
        top.addWidget(self.edt_new)
        top.addWidget(self.btn_add)
        layout.addLayout(top)

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.container = QWidget()
        self.box = QVBoxLayout(self.container)
        self.box.addStretch(1)
        self.scroll.setWidget(self.container)
        layout.addWidget(self.scroll)

        btns = QHBoxLayout()
        ok = QPushButton("Salva")
        cancel = QPushButton("Annulla")
        ok.clicked.connect(self.accept)
        cancel.clicked.connect(self.reject)
        btns.addStretch(1)
        btns.addWidget(ok)
        btns.addWidget(cancel)
        layout.addLayout(btns)

    def _load_data(self):
        for item in self.items:
            self._append_row(item.get("text", ""), bool(item.get("done")))

    def _append_row(self, text: str, done: bool):
        row_widget = QWidget()
        row_layout = QHBoxLayout(row_widget)
        chk = QCheckBox()
        chk.setChecked(done)
        edt = QLineEdit(text)
        btn_del = QPushButton("X")
        btn_del.setMaximumWidth(32)
        btn_del.clicked.connect(lambda: self.remove_row(row_widget))
        row_layout.addWidget(chk)
        row_layout.addWidget(edt)
        row_layout.addWidget(btn_del)
        self.box.insertWidget(max(0, self.box.count() - 1), row_widget)
        self.rows.append((row_widget, chk, edt))

    def add_item(self):
        text = self.edt_new.text().strip()
        if not text:
            QMessageBox.warning(self, "Task vuoto", "Scrivi il testo del task prima di aggiungerlo.")
            return
        self._append_row(text, False)
        self.edt_new.clear()

    def remove_row(self, widget):
        for i, (row_widget, _, _) in enumerate(self.rows):
            if row_widget is widget:
                self.rows.pop(i)
                widget.setParent(None)
                widget.deleteLater()
                break

    def get_payload(self):
        data = []
        for _, chk, edt in self.rows:
            text = edt.text().strip()
            if text:
                data.append({"text": text, "done": chk.isChecked()})
        return data
