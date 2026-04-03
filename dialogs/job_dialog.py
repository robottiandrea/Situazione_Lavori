# -*- coding: utf-8 -*-
from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
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
        self.resize(860, 760)
        self.job = job or {}
        self._loading = False
        self._last_exception_toggle_state = False
        self._build_ui()
        self._load_data()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        info = QLabel(
            "I campi Distretto/Anno e Nome vengono proposti automaticamente dal path selezionato, "
            "ma puoi correggerli manualmente.\n"
            "La modalità eccezione manuale serve per righe fuori schema: consente duplicati PRG/DL "
            "e usa i percorsi di controllo manuali invece delle regole standard."
        )
        info.setWordWrap(True)
        layout.addWidget(info)

        base_box = QGroupBox("Dati base riga")
        base_form = QFormLayout(base_box)

        self.edt_project_path = QLineEdit()
        self.edt_project_path.setPlaceholderText(
            r"Es. S:\Disegni\Snam\Progettazioni_Milano_2025\023_NOME LAVORO"
        )
        self.edt_project_path.textChanged.connect(self.on_project_path_changed)
        self.btn_project_path = QPushButton("Sfoglia...")
        self.btn_project_path.clicked.connect(lambda: self.pick_dir(self.edt_project_path))
        project_path_row = QHBoxLayout()
        project_path_row.addWidget(self.edt_project_path)
        project_path_row.addWidget(self.btn_project_path)
        base_form.addRow("Path Base Progetto", project_path_row)

        self.edt_project_distretto = QLineEdit()
        base_form.addRow("Distretto/Anno PRG", self.edt_project_distretto)

        self.edt_project_name = QLineEdit()
        base_form.addRow("Cartella PRG", self.edt_project_name)

        self.cmb_project_mode = QComboBox()
        self.cmb_project_mode.addItem("GTN", "GTN")
        self.cmb_project_mode.addItem("ALTRA DITTA", "ALTRA_DITTA")
        self.cmb_project_mode.addItem("PROGETTO NON PREVISTO", "PROGETTO_NON_PREVISTO")
        base_form.addRow("Stato PRG", self.cmb_project_mode)

        self.cmb_permits_mode = QComboBox()
        self.cmb_permits_mode.addItem("SÌ", "REQUIRED")
        self.cmb_permits_mode.addItem("NO", "NOT_REQUIRED")
        base_form.addRow("Permessi previsti", self.cmb_permits_mode)

        self.edt_dl_path = QLineEdit()
        self.edt_dl_path.setPlaceholderText(
            r"Es. S:\Lavori\Snam\81-08 MILANO\2025\DIREZIONE LAVORI\16_NOME LAVORO"
        )
        self.edt_dl_path.textChanged.connect(self.on_dl_path_changed)
        self.btn_dl_path = QPushButton("Sfoglia...")
        self.btn_dl_path.clicked.connect(lambda: self.pick_dir(self.edt_dl_path))
        dl_path_row = QHBoxLayout()
        dl_path_row.addWidget(self.edt_dl_path)
        dl_path_row.addWidget(self.btn_dl_path)
        base_form.addRow("Path Base DL", dl_path_row)

        self.edt_dl_distretto = QLineEdit()
        base_form.addRow("Distretto/Anno DL", self.edt_dl_distretto)

        self.edt_dl_name = QLineEdit()
        base_form.addRow("Cartella DL", self.edt_dl_name)

        self.edt_dl_insert_date = QLineEdit()
        self.edt_dl_insert_date.setPlaceholderText("yyyy-MM-dd (opzionale)")
        base_form.addRow("Data Inserimento DL", self.edt_dl_insert_date)

        self.txt_notes = QTextEdit()
        self.txt_notes.setPlaceholderText("Note generali lavoro...")
        base_form.addRow("Note generali", self.txt_notes)

        layout.addWidget(base_box)

        self.chk_exception = QCheckBox("Riga eccezione manuale (E)")
        self.chk_exception.toggled.connect(self._on_exception_toggled)
        layout.addWidget(self.chk_exception)

        self.exception_box = QGroupBox("Configurazione eccezione manuale")
        exception_layout = QVBoxLayout(self.exception_box)

        warning = QLabel(
            "Le righe eccezione non usano autofill da DL e non deducono automaticamente i codici "
            "Cartesio PRG/COS. I codici Cartesio sono manuali."
        )
        warning.setWordWrap(True)
        exception_layout.addWidget(warning)

        exception_form = QFormLayout()

        self.edt_exception_reason = QLineEdit()
        self.edt_exception_reason.setPlaceholderText("Motivo eccezione...")
        exception_form.addRow("Motivo eccezione", self.edt_exception_reason)

        self.edt_exception_group_code = QLineEdit()
        self.edt_exception_group_code.setPlaceholderText("Opzionale, es. E-2026-001")
        exception_form.addRow("Gruppo eccezione", self.edt_exception_group_code)

        self.edt_manual_project_control_path = QLineEdit()
        self.edt_manual_project_control_path.setPlaceholderText(
            r"Percorso manuale da usare per i controlli lato PRG"
        )
        self.btn_manual_project_control_path = QPushButton("Sfoglia...")
        self.btn_manual_project_control_path.clicked.connect(
            lambda: self.pick_dir(self.edt_manual_project_control_path)
        )
        manual_project_row = QHBoxLayout()
        manual_project_row.addWidget(self.edt_manual_project_control_path)
        manual_project_row.addWidget(self.btn_manual_project_control_path)
        exception_form.addRow("Path PRG di controllo", manual_project_row)

        self.edt_manual_dl_control_path = QLineEdit()
        self.edt_manual_dl_control_path.setPlaceholderText(
            r"Percorso manuale da usare per i controlli lato DL"
        )
        self.btn_manual_dl_control_path = QPushButton("Sfoglia...")
        self.btn_manual_dl_control_path.clicked.connect(
            lambda: self.pick_dir(self.edt_manual_dl_control_path)
        )
        manual_dl_row = QHBoxLayout()
        manual_dl_row.addWidget(self.edt_manual_dl_control_path)
        manual_dl_row.addWidget(self.btn_manual_dl_control_path)
        exception_form.addRow("Path DL di controllo", manual_dl_row)

        self.edt_manual_psc_path = QLineEdit()
        self.edt_manual_psc_path.setPlaceholderText("Percorso manuale PSC (opzionale)")
        self.btn_manual_psc_path = QPushButton("Sfoglia...")
        self.btn_manual_psc_path.clicked.connect(lambda: self.pick_dir(self.edt_manual_psc_path))
        manual_psc_row = QHBoxLayout()
        manual_psc_row.addWidget(self.edt_manual_psc_path)
        manual_psc_row.addWidget(self.btn_manual_psc_path)
        exception_form.addRow("Path PSC manuale", manual_psc_row)

        self.edt_manual_tracciamento_path = QLineEdit()
        self.edt_manual_tracciamento_path.setPlaceholderText(
            "Percorso manuale File Tracciamento (opzionale)"
        )
        self.btn_manual_tracciamento_path = QPushButton("Sfoglia...")
        self.btn_manual_tracciamento_path.clicked.connect(
            lambda: self.pick_dir(self.edt_manual_tracciamento_path)
        )
        manual_tracciamento_row = QHBoxLayout()
        manual_tracciamento_row.addWidget(self.edt_manual_tracciamento_path)
        manual_tracciamento_row.addWidget(self.btn_manual_tracciamento_path)
        exception_form.addRow("Path Tracciamento manuale", manual_tracciamento_row)

        self.edt_manual_cartesio_prg_code = QLineEdit()
        self.edt_manual_cartesio_prg_code.setPlaceholderText("Es. PRG001234")
        exception_form.addRow("Codice Cartesio PRG", self.edt_manual_cartesio_prg_code)

        self.edt_manual_cartesio_prg_path = QLineEdit()
        self.edt_manual_cartesio_prg_path.setPlaceholderText("Percorso manuale Cartesio PRG")
        self.btn_manual_cartesio_prg_path = QPushButton("Sfoglia...")
        self.btn_manual_cartesio_prg_path.clicked.connect(
            lambda: self.pick_dir(self.edt_manual_cartesio_prg_path)
        )
        manual_cart_prg_row = QHBoxLayout()
        manual_cart_prg_row.addWidget(self.edt_manual_cartesio_prg_path)
        manual_cart_prg_row.addWidget(self.btn_manual_cartesio_prg_path)
        exception_form.addRow("Path Cartesio PRG", manual_cart_prg_row)

        self.edt_manual_cartesio_cos_code = QLineEdit()
        self.edt_manual_cartesio_cos_code.setPlaceholderText("Es. COS001234")
        exception_form.addRow("Codice Cartesio COS", self.edt_manual_cartesio_cos_code)

        self.edt_manual_cartesio_cos_path = QLineEdit()
        self.edt_manual_cartesio_cos_path.setPlaceholderText("Percorso manuale Cartesio COS")
        self.btn_manual_cartesio_cos_path = QPushButton("Sfoglia...")
        self.btn_manual_cartesio_cos_path.clicked.connect(
            lambda: self.pick_dir(self.edt_manual_cartesio_cos_path)
        )
        manual_cart_cos_row = QHBoxLayout()
        manual_cart_cos_row.addWidget(self.edt_manual_cartesio_cos_path)
        manual_cart_cos_row.addWidget(self.btn_manual_cartesio_cos_path)
        exception_form.addRow("Path Cartesio COS", manual_cart_cos_row)

        exception_layout.addLayout(exception_form)
        layout.addWidget(self.exception_box)

        btns = QHBoxLayout()
        self.btn_ok = QPushButton("Salva")
        self.btn_cancel = QPushButton("Annulla")
        self.btn_ok.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)
        btns.addStretch(1)
        btns.addWidget(self.btn_ok)
        btns.addWidget(self.btn_cancel)
        layout.addLayout(btns)

        self.exception_box.setVisible(False)

    def _load_data(self):
        if not self.job:
            return

        self._loading = True
        try:
            self.edt_project_path.setText(self.job.get("project_base_path", ""))
            self.edt_project_distretto.setText(self.job.get("project_distretto_anno", ""))
            self.edt_project_name.setText(self.job.get("project_name", ""))

            project_mode = str(self.job.get("project_mode", "GTN") or "GTN").strip().upper()
            if project_mode not in {"GTN", "ALTRA_DITTA", "PROGETTO_NON_PREVISTO"}:
                project_mode = "GTN"
            mode_index = self.cmb_project_mode.findData(project_mode)
            self.cmb_project_mode.setCurrentIndex(mode_index if mode_index >= 0 else 0)

            permits_mode = str(self.job.get("permits_mode", "REQUIRED") or "REQUIRED").strip().upper()
            if permits_mode not in {"REQUIRED", "NOT_REQUIRED"}:
                permits_mode = "REQUIRED"
            permits_index = self.cmb_permits_mode.findData(permits_mode)
            self.cmb_permits_mode.setCurrentIndex(permits_index if permits_index >= 0 else 0)

            self.edt_dl_path.setText(self.job.get("dl_base_path", ""))
            self.edt_dl_distretto.setText(self.job.get("dl_distretto_anno", ""))
            self.edt_dl_name.setText(self.job.get("dl_name", ""))
            self.edt_dl_insert_date.setText(self.job.get("dl_insert_date", ""))
            self.txt_notes.setPlainText(self.job.get("general_notes", ""))

            exception_mode = str(self.job.get("exception_mode", "STANDARD") or "STANDARD").strip().upper()
            is_exception = exception_mode == "MANUAL"
            self.chk_exception.setChecked(is_exception)

            self.edt_exception_reason.setText(self.job.get("exception_reason", ""))
            self.edt_exception_group_code.setText(self.job.get("exception_group_code", ""))
            self.edt_manual_project_control_path.setText(self.job.get("manual_project_control_path", ""))
            self.edt_manual_dl_control_path.setText(self.job.get("manual_dl_control_path", ""))
            self.edt_manual_psc_path.setText(self.job.get("psc_path", ""))
            self.edt_manual_tracciamento_path.setText(
                self.job.get("project_tracciamento_manual_path", "")
            )
            self.edt_manual_cartesio_prg_code.setText(
                self.job.get("manual_cartesio_prg_code", "")
            )
            self.edt_manual_cartesio_prg_path.setText(
                self.job.get("manual_cartesio_prg_path", "")
            )
            self.edt_manual_cartesio_cos_code.setText(
                self.job.get("manual_cartesio_cos_code", "")
            )
            self.edt_manual_cartesio_cos_path.setText(
                self.job.get("manual_cartesio_cos_path", "")
            )

            self.exception_box.setVisible(is_exception)
            self._last_exception_toggle_state = is_exception

        finally:
            self._loading = False

    def _on_exception_toggled(self, checked: bool) -> None:
        self.exception_box.setVisible(bool(checked))

        if self._loading:
            self._last_exception_toggle_state = bool(checked)
            return

        if checked and not self._last_exception_toggle_state:
            self._prefill_exception_fields_from_standard()

        self._last_exception_toggle_state = bool(checked)

    def _prefill_exception_fields_from_standard(self) -> None:
        project_path = norm_path(self.edt_project_path.text())
        dl_path = norm_path(self.edt_dl_path.text())

        if project_path and not self.edt_manual_project_control_path.text().strip():
            self.edt_manual_project_control_path.setText(project_path)

        if dl_path and not self.edt_manual_dl_control_path.text().strip():
            self.edt_manual_dl_control_path.setText(dl_path)

        if not self.edt_manual_psc_path.text().strip():
            existing_psc = norm_path(self.job.get("psc_path", ""))
            if existing_psc:
                self.edt_manual_psc_path.setText(existing_psc)

        if not self.edt_manual_tracciamento_path.text().strip():
            existing_tracciamento = norm_path(
                self.job.get("project_tracciamento_manual_path", "")
            )
            if existing_tracciamento:
                self.edt_manual_tracciamento_path.setText(existing_tracciamento)

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

        is_exception = self.chk_exception.isChecked()
        exception_reason = self.edt_exception_reason.text().strip()
        manual_project_control_path = norm_path(self.edt_manual_project_control_path.text())
        manual_dl_control_path = norm_path(self.edt_manual_dl_control_path.text())
        manual_psc_path = norm_path(self.edt_manual_psc_path.text())
        manual_tracciamento_path = norm_path(self.edt_manual_tracciamento_path.text())
        manual_cartesio_prg_path = norm_path(self.edt_manual_cartesio_prg_path.text())
        manual_cartesio_cos_path = norm_path(self.edt_manual_cartesio_cos_path.text())

        if is_exception:
            if not manual_project_control_path and not manual_dl_control_path:
                QMessageBox.warning(
                    self,
                    "Dati mancanti",
                    "Per una riga eccezione devi valorizzare almeno Path PRG di controllo oppure Path DL di controllo.",
                )
                return

            if not exception_reason:
                QMessageBox.warning(
                    self,
                    "Motivo mancante",
                    "Compila il motivo eccezione prima di salvare.",
                )
                return
        else:
            if not project_path and not dl_path:
                QMessageBox.warning(
                    self,
                    "Dati mancanti",
                    "Devi valorizzare almeno Path Base Progetto oppure Path Base DL.",
                )
                return

        for label, value in (
            ("Path Base Progetto", project_path),
            ("Path Base DL", dl_path),
            ("Path PRG di controllo", manual_project_control_path),
            ("Path DL di controllo", manual_dl_control_path),
            ("Path PSC manuale", manual_psc_path),
            ("Path Tracciamento manuale", manual_tracciamento_path),
            ("Path Cartesio PRG", manual_cartesio_prg_path),
            ("Path Cartesio COS", manual_cartesio_cos_path),
        ):
            if value and not exists_dir(value):
                QMessageBox.warning(self, "Path non valido", f"{label} non esiste:\n{value}")
                return

        if dl_insert_date and not parse_date_text(dl_insert_date):
            QMessageBox.warning(
                self,
                "Data non valida",
                "La Data Inserimento DL, se compilata, deve essere nel formato yyyy-MM-dd.",
            )
            return

        super().accept()

    def get_payload(self):
        psc_path = norm_path(self.edt_manual_psc_path.text())

        return {
            "project_base_path": norm_path(self.edt_project_path.text()),
            "project_distretto_anno": self.edt_project_distretto.text().strip(),
            "project_name": self.edt_project_name.text().strip(),
            "project_mode": self.cmb_project_mode.currentData() or "GTN",
            "permits_mode": self.cmb_permits_mode.currentData() or "REQUIRED",
            "dl_base_path": norm_path(self.edt_dl_path.text()),
            "dl_distretto_anno": self.edt_dl_distretto.text().strip(),
            "dl_name": self.edt_dl_name.text().strip(),
            "dl_insert_date": self.edt_dl_insert_date.text().strip(),
            "general_notes": self.txt_notes.toPlainText().strip(),
            "exception_mode": "MANUAL" if self.chk_exception.isChecked() else "STANDARD",
            "exception_reason": self.edt_exception_reason.text().strip(),
            "exception_group_code": self.edt_exception_group_code.text().strip(),
            "manual_project_control_path": norm_path(self.edt_manual_project_control_path.text()),
            "manual_dl_control_path": norm_path(self.edt_manual_dl_control_path.text()),
            "psc_path": psc_path,
            "project_tracciamento_manual_path": norm_path(
                self.edt_manual_tracciamento_path.text()
            ),
            "manual_cartesio_prg_code": self.edt_manual_cartesio_prg_code.text().strip().upper(),
            "manual_cartesio_prg_path": norm_path(self.edt_manual_cartesio_prg_path.text()),
            "manual_cartesio_cos_code": self.edt_manual_cartesio_cos_code.text().strip().upper(),
            "manual_cartesio_cos_path": norm_path(self.edt_manual_cartesio_cos_path.text()),
            "psc_status": "PENDING" if psc_path else "NOT_SET",
        }
