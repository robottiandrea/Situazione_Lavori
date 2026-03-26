# -*- coding: utf-8 -*-
from __future__ import annotations

from PySide6.QtWidgets import (
    QDialog,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
)

from utils import (
    exists_dir,
    folder_name_from_path,
    infer_dl_distretto_anno,
    infer_project_distretto_anno,
    norm_path,
    parse_date_text,
)


class JobDialog(QDialog):
    def __init__(self, parent=None, job=None):
        super().__init__(parent)
        self.setWindowTitle("Nuovo Lavoro" if not job else "Modifica Lavoro")
        self.resize(720, 420)
        self.job = job or {}
        self._loading = False
        self._build_ui()
        self._load_data()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.edt_project_path = QLineEdit()
        self.edt_project_path.setPlaceholderText(r"Es. S:\Disegni\Snam\Progettazioni_Milano_2025\023_NOME LAVORO")
        self.edt_project_path.textChanged.connect(self.on_project_path_changed)
        self.btn_project_path = QPushButton("Sfoglia...")
        self.btn_project_path.clicked.connect(lambda: self.pick_dir(self.edt_project_path))
        project_path_row = QHBoxLayout()
        project_path_row.addWidget(self.edt_project_path)
        project_path_row.addWidget(self.btn_project_path)
        form.addRow("Path Base Progetto", project_path_row)

        self.edt_project_distretto = QLineEdit()
        form.addRow("Distretto/Anno PRG", self.edt_project_distretto)

        self.edt_project_name = QLineEdit()
        form.addRow("Cartella PRG", self.edt_project_name)

        self.edt_dl_path = QLineEdit()
        self.edt_dl_path.setPlaceholderText(r"Es. S:\Lavori\Snam\81-08 MILANO\2025\DIREZIONE LAVORI\16_NOME LAVORO")
        self.edt_dl_path.textChanged.connect(self.on_dl_path_changed)
        self.btn_dl_path = QPushButton("Sfoglia...")
        self.btn_dl_path.clicked.connect(lambda: self.pick_dir(self.edt_dl_path))
        dl_path_row = QHBoxLayout()
        dl_path_row.addWidget(self.edt_dl_path)
        dl_path_row.addWidget(self.btn_dl_path)
        form.addRow("Path Base DL", dl_path_row)

        self.edt_dl_distretto = QLineEdit()
        form.addRow("Distretto/Anno DL", self.edt_dl_distretto)

        self.edt_dl_name = QLineEdit()
        form.addRow("Cartella DL", self.edt_dl_name)

        self.edt_dl_insert_date = QLineEdit()
        self.edt_dl_insert_date.setPlaceholderText("yyyy-MM-dd (opzionale)")
        form.addRow("Data Inserimento DL", self.edt_dl_insert_date)

        self.txt_notes = QTextEdit()
        self.txt_notes.setPlaceholderText("Note generali lavoro...")
        form.addRow("Note generali", self.txt_notes)

        info = QLabel(
            "I campi Distretto/Anno e Nome vengono proposti automaticamente dal path selezionato, ma puoi correggerli manualmente."
        )
        info.setWordWrap(True)
        layout.addWidget(info)
        layout.addLayout(form)

        btns = QHBoxLayout()
        self.btn_ok = QPushButton("Salva")
        self.btn_cancel = QPushButton("Annulla")
        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)
        btns.addStretch(1)
        btns.addWidget(self.btn_ok)
        btns.addWidget(self.btn_cancel)
        layout.addLayout(btns)

    def _load_data(self):
        if not self.job:
            return

        self._loading = True
        try:
            self.edt_project_path.setText(self.job.get("project_base_path", ""))
            self.edt_project_distretto.setText(self.job.get("project_distretto_anno", ""))
            self.edt_project_name.setText(self.job.get("project_name", ""))
            self.edt_dl_path.setText(self.job.get("dl_base_path", ""))
            self.edt_dl_distretto.setText(self.job.get("dl_distretto_anno", ""))
            self.edt_dl_name.setText(self.job.get("dl_name", ""))
            self.edt_dl_insert_date.setText(self.job.get("dl_insert_date", ""))
            self.txt_notes.setPlainText(self.job.get("general_notes", ""))
        finally:
            self._loading = False

    def pick_dir(self, line_edit: QLineEdit):
        start_dir = line_edit.text().strip() or ""
        path = QFileDialog.getExistingDirectory(self, "Seleziona cartella", start_dir)
        if path:
            line_edit.setText(path)

    def on_project_path_changed(self, text: str):
        if self._loading:
            return
        path = norm_path(text)
        self.edt_project_distretto.setText(infer_project_distretto_anno(path))
        self.edt_project_name.setText(folder_name_from_path(path))

    def on_dl_path_changed(self, text: str):
        if self._loading:
            return
        path = norm_path(text)
        self.edt_dl_distretto.setText(infer_dl_distretto_anno(path))
        self.edt_dl_name.setText(folder_name_from_path(path))

    def accept(self):
        project_path = norm_path(self.edt_project_path.text())
        dl_path = norm_path(self.edt_dl_path.text())
        dl_insert_date = self.edt_dl_insert_date.text().strip()

        if not project_path and not dl_path:
            QMessageBox.warning(self, "Dati mancanti", "Devi valorizzare almeno Path Base Progetto oppure Path Base DL.")
            return

        if project_path and not exists_dir(project_path):
            QMessageBox.warning(self, "Path non valido", f"Il path progetto non esiste:\n{project_path}")
            return

        if dl_path and not exists_dir(dl_path):
            QMessageBox.warning(self, "Path non valido", f"Il path DL non esiste:\n{dl_path}")
            return

        if dl_insert_date and not parse_date_text(dl_insert_date):
            QMessageBox.warning(self, "Data non valida", "La Data Inserimento DL, se compilata, deve essere nel formato yyyy-MM-dd.")
            return

        super().accept()

    def get_payload(self):
        return {
            "project_base_path": norm_path(self.edt_project_path.text()),
            "project_distretto_anno": self.edt_project_distretto.text().strip(),
            "project_name": self.edt_project_name.text().strip(),
            "dl_base_path": norm_path(self.edt_dl_path.text()),
            "dl_distretto_anno": self.edt_dl_distretto.text().strip(),
            "dl_name": self.edt_dl_name.text().strip(),
            "dl_insert_date": self.edt_dl_insert_date.text().strip(),
            "general_notes": self.txt_notes.toPlainText().strip(),
        }
