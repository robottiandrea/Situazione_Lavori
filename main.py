# -*- coding: utf-8 -*-
"""
Situazione Lavori - V1
----------------------
Base PySide6 + SQLite + scansione filesystem persistita su DB.

Note:
- Una riga rappresenta un lavoro.
- Ogni lavoro può avere sezione Progetto, sezione DL, oppure entrambe.
- La GUI legge solo dati dal DB.
- La scansione filesystem serve solo ad aggiornare il DB/cache condiviso.
"""
from __future__ import annotations

import logging
import sys
import traceback
from pathlib import Path

from PySide6.QtCore import QPoint, Qt
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QInputDialog,
    QMainWindow,
    QTabWidget,
    QMenu,
    QMessageBox,
    QPushButton,
    QStatusBar,
    QStyledItemDelegate,
    QTableView,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from database import DatabaseManager
from dialogs.job_dialog import JobDialog
from dialogs.job_history_dialog import JobHistoryDialog
from dialogs.permits_dialog import PermitsDialog
from dialogs.status_dialog import StatusDialog
from dialogs.todo_dialog import TodoDialog
from dialogs.cartesio_dialog import CartesioDialog
from models import CartesioTableModel, JobsTableModel
from scanner import FileSystemScanner
from services import JobService
from utils import (
    CARTESIO_ACC_STATES,
    CARTESIO_COS_STATES,
    CARTESIO_PRG_STATES,
    RILIEVI_DL_STATES,
    folder_name_from_path,
    infer_dl_distretto_anno,
    infer_project_distretto_anno,
    open_in_explorer,
    setup_logging,
)


class PreserveForegroundDelegate(QStyledItemDelegate):
    """Mantiene il colore del testo della cella anche quando la riga è selezionata."""

    def initStyleOption(self, option, index):
        super().initStyleOption(option, index)

        brush = index.data(Qt.ForegroundRole)

        if brush is not None:
            option.palette.setBrush(QPalette.Text, brush)
            option.palette.setBrush(QPalette.WindowText, brush)
            option.palette.setBrush(QPalette.HighlightedText, brush)
        else:
            normal_brush = option.palette.brush(QPalette.Text)
            option.palette.setBrush(QPalette.HighlightedText, normal_brush)


class MainWindow(QMainWindow):
    """
    Finestra principale.

    Obiettivi di questa versione:
    - eliminare la dipendenza fragile dagli indici colonna hardcoded;
    - configurare larghezze e comportamenti usando le chiavi logiche del model;
    - tenere le colonne nome compatte in base al contenuto reale delle celle.
    """


    def __init__(self):
        super().__init__()
        self.setWindowTitle("Situazione Lavori - V1")
        self.resize(1900, 1000)

        self.db = DatabaseManager()
        self.scanner = FileSystemScanner()
        self.service = JobService(self.db, self.scanner)
        self.model = JobsTableModel()

        self.cartesio_prg_model = CartesioTableModel("PRG")
        self.cartesio_cos_model = CartesioTableModel("COS")
        self.cartesio_acc_model = CartesioTableModel("ACC")

        # Cache complete per ogni vista/tab.
        # La barra filtro deve lavorare su queste cache e NON ricaricare dal DB
        # ad ogni battitura.
        self.all_rows = []
        self.cartesio_prg_all_rows = []
        self.cartesio_cos_all_rows = []
        self.cartesio_acc_all_rows = []

        # Stato sort manuale.
        self.user_sort_active = False
        self.cartesio_prg_user_sort_active = False
        self.cartesio_cos_user_sort_active = False
        self.cartesio_acc_user_sort_active = False

        # Subset temporaneo della tab Lavori costruito a partire dalle righe
        # attualmente visibili nella tab Cartesio.
        self.jobs_subset_from_cartesio_ids: set[int] = set()

        # Filtro manuale locale della tab Cartesio costruito dalle righe
        # selezionate manualmente nelle tre tabelle PRG / COS / ACC.
        self.cartesio_manual_subset_ids: set[int] = set()

        # Filtri indipendenti per macro-tab.
        # L'editor grafico è unico, ma il testo persistito dipende dalla tab attiva.
        self.jobs_filter_text = ""
        self.cartesio_filter_text = ""

        self._build_ui()
        self.showMaximized()
        self._startup_load()

    # -------------------------------------------------------------------------
    # HELPERS COLONNE
    # -------------------------------------------------------------------------

    def _column_key(self, column: int) -> str | None:
        try:
            return self.model.column_key(column)
        except Exception:
            return None

    def _on_user_sort_clicked(self, section: int):
        """
        Segna che da questo momento l'utente ha richiesto un ordinamento manuale
        nella tab Lavori.
        """
        self.user_sort_active = True

    def _on_cartesio_user_sort_clicked(self, scope: str) -> None:
        """
        Segna che l'utente ha richiesto un ordinamento manuale in una dashboard Cartesio.
        """
        normalized_scope = str(scope or "").strip().upper()
        if normalized_scope == "COS":
            self.cartesio_cos_user_sort_active = True
        elif normalized_scope == "ACC":
            self.cartesio_acc_user_sort_active = True
        else:
            self.cartesio_prg_user_sort_active = True

    def _reapply_table_sort(self, table: QTableView, model) -> None:
        """
        Riapplica il sort corrente dell'header al model passato.
        """
        header = table.horizontalHeader()
        section = header.sortIndicatorSection()
        order = header.sortIndicatorOrder()

        if section >= 0:
            model.sort(section, order)

    def _reapply_current_sort(self):
        """
        Riapplica l'ordinamento corrente della tab Lavori.
        """
        self._reapply_table_sort(self.table, self.model)

    def _normalized_jobs_filter_text(self) -> str:
        """
        Restituisce il filtro normalizzato della tab Lavori.
        """
        return str(self.jobs_filter_text or "").strip().lower()

    def _normalized_cartesio_filter_text(self) -> str:
        """
        Restituisce il filtro normalizzato della tab Cartesio.
        """
        return str(self.cartesio_filter_text or "").strip().lower()

    def _set_filter_editor_text(self, text: str) -> None:
        """
        Aggiorna il QLineEdit del filtro senza generare il segnale textChanged.
        """
        self.edt_filter.blockSignals(True)
        try:
            self.edt_filter.setText(str(text or ""))
        finally:
            self.edt_filter.blockSignals(False)

    def _sync_filter_editor_from_current_tab(self) -> None:
        """
        Allinea il contenuto del box filtro alla tab attiva.
        """
        if not hasattr(self, "tabs") or not hasattr(self, "edt_filter"):
            return

        current_index = self.tabs.currentIndex()

        if current_index == 0:
            self.edt_filter.setPlaceholderText(
                "Cerca nella tab Lavori..."
            )
            self._set_filter_editor_text(self.jobs_filter_text)
        else:
            self.edt_filter.setPlaceholderText(
                "Cerca nella tab Cartesio..."
            )
            self._set_filter_editor_text(self.cartesio_filter_text)

    def _on_filter_text_changed(self, text: str) -> None:
        """
        Salva il testo filtro nello stato della tab attiva e riapplica le viste.
        """
        clean_text = str(text or "")

        if hasattr(self, "tabs") and self.tabs.currentIndex() == 0:
            self.jobs_filter_text = clean_text
        else:
            self.cartesio_filter_text = clean_text

        self.apply_filter()

    def _on_tab_changed(self, index: int) -> None:
        """
        Quando cambia tab, il box filtro mostra il filtro della tab corrente.
        """
        _ = index
        self._sync_filter_editor_from_current_tab()

    def _row_matches_filter_text(self, row, field_names: tuple[str, ...], text: str) -> bool:
        """
        Verifica se una riga matcha il filtro testuale cercando dentro una lista
        di campi specifici.
        """
        if not text:
            return True

        parts = []

        for field_name in field_names:
            value = row.get(field_name, "")

            if isinstance(value, dict):
                parts.extend("" if item is None else str(item) for item in value.values())
                continue

            if isinstance(value, (list, tuple, set)):
                parts.extend("" if item is None else str(item) for item in value)
                continue

            parts.append("" if value is None else str(value))

        haystack = " | ".join(parts).lower()
        return text in haystack

    def _filtered_rows_from_fields(self, source_rows, field_names: tuple[str, ...], text: str):
        """
        Restituisce le righe filtrate in base ai campi dichiarati.
        """
        base_rows = list(source_rows or [])

        if not text:
            return base_rows

        return [
            row
            for row in base_rows
            if self._row_matches_filter_text(row, field_names, text)
        ]

    def _jobs_filter_fields(self) -> tuple[str, ...]:
        """
        Campi su cui il filtro globale deve cercare nella tab Lavori.
        """
        return (
            "history_alert_display",
            "project_distretto_anno",
            "project_name",
            "project_name_display",
            "project_mode",
            "project_base_path",
            "dl_distretto_anno",
            "dl_name",
            "dl_base_path",
            "general_notes",
            "audit_latest_source_kind",
            "audit_latest_summary",
            "cartesio_prg_display",
            "cartesio_acc_prg_display",
            "cartesio_cos_display",
            "cartesio_acc_cos_display",
            "rilievi_dl_display",
            "permits_display",
            "psc_display",
            "psc_path",
            "project_rilievo",
            "project_enti",
            "project_revision",
            "permessi_revision",
            "project_tracciamento",
            "project_tracciamento_manual_path",
            "exception_mode",
            "exception_reason",
            "exception_group_code",
            "manual_project_control_path",
            "manual_dl_control_path",
            "manual_cartesio_prg_code",
            "manual_cartesio_prg_path",
            "manual_cartesio_cos_code",
            "manual_cartesio_cos_path",
            "manual_cartesio_acc_code",
            "manual_cartesio_acc_path",
        )


    def _cartesio_filter_fields(self, scope: str) -> tuple[str, ...]:
        """
        Campi su cui il filtro globale deve cercare nella dashboard Cartesio.
        """
        normalized_scope = str(scope or "").strip().upper()

        common_fields = (
            "job_id",
            "entry_id",
            "scope",
            "referente",
            "entry_status",
            "checklist_display",
            "latest_note_title",
            "display_last_activity",
            "last_activity_at",
            "open_threads",
            "project_distretto_anno",
            "project_name",
            "project_name_display",
            "project_mode",
            "project_base_path",
            "dl_distretto_anno",
            "dl_name",
            "dl_base_path",
            "cartesio_delivery_scope",
            "exception_mode",
            "exception_reason",
            "exception_group_code",
            "manual_project_control_path",
            "manual_dl_control_path",
        )

        if normalized_scope == "COS":
            return common_fields + ("cartesio_cos_display",)

        if normalized_scope == "ACC":
            return common_fields + ("cartesio_acc_display",)

        return common_fields + ("cartesio_prg_display",)

    def _jobs_view_has_active_subset(self) -> bool:
        """
        True se la tab Lavori è limitata a un subset proveniente da Cartesio.
        """
        return bool(self.jobs_subset_from_cartesio_ids)

    def _jobs_view_has_active_filter(self) -> bool:
        """
        Considera "vista filtrata" sia il filtro testuale della tab Lavori
        sia il subset temporaneo derivato dalla tab Cartesio.
        """
        return bool(str(self.jobs_filter_text or "").strip()) or self._jobs_view_has_active_subset()

    def _visible_cartesio_job_ids(self) -> set[int]:
        """
        Restituisce l'unione dei job_id attualmente visibili nelle tre tabelle
        Cartesio: PRG, COS e ACC.
        """
        visible_ids: set[int] = set()

        for model in (
            self.cartesio_prg_model,
            self.cartesio_cos_model,
            self.cartesio_acc_model,
        ):
            for row_index in range(model.rowCount()):
                row = model.get_row(row_index)
                if not row:
                    continue

                try:
                    job_id = int(row.get("job_id") or 0)
                except Exception:
                    continue

                if job_id > 0:
                    visible_ids.add(job_id)

        return visible_ids

    def _selected_cartesio_job_ids(self) -> set[int]:
        """
        Restituisce l'unione dei job_id selezionati manualmente nelle tre tabelle
        Cartesio: PRG, COS e ACC.
        """
        selected_ids: set[int] = set()

        table_model_pairs = (
            (self.tbl_cartesio_prg, self.cartesio_prg_model),
            (self.tbl_cartesio_cos, self.cartesio_cos_model),
            (self.tbl_cartesio_acc, self.cartesio_acc_model),
        )

        for table, model in table_model_pairs:
            selection_model = table.selectionModel()
            if not selection_model:
                continue

            for index in selection_model.selectedRows():
                if not index.isValid():
                    continue

                row = model.get_row(index.row())
                if not row:
                    continue

                try:
                    job_id = int(row.get("job_id") or 0)
                except Exception:
                    continue

                if job_id > 0:
                    selected_ids.add(job_id)

        return selected_ids

    def _cartesio_view_has_manual_subset(self) -> bool:
        """
        True se la tab Cartesio è limitata alle righe selezionate manualmente.
        """
        return bool(self.cartesio_manual_subset_ids)

    def _normalize_cartesio_manual_subset_ids(self) -> None:
        """
        Mantiene nel subset manuale Cartesio solo i job_id ancora presenti
        nelle cache complete PRG / COS / ACC.
        """
        if not self.cartesio_manual_subset_ids:
            return

        existing_ids: set[int] = set()

        for source_rows in (
            self.cartesio_prg_all_rows,
            self.cartesio_cos_all_rows,
            self.cartesio_acc_all_rows,
        ):
            for row in source_rows or []:
                try:
                    job_id = int(row.get("job_id") or 0)
                except Exception:
                    continue

                if job_id > 0:
                    existing_ids.add(job_id)

        self.cartesio_manual_subset_ids &= existing_ids

    def _update_cartesio_jobs_button_text(self) -> None:
        """
        Aggiorna il testo del pulsante toggle principale in basso nella tab Cartesio.
        """
        if not hasattr(self, "btn_cartesio_show_in_jobs"):
            return

        if self._jobs_view_has_active_subset():
            self.btn_cartesio_show_in_jobs.setText("Ripristina tab Lavori")
        else:
            self.btn_cartesio_show_in_jobs.setText("Mostra visibili in tab Lavori")

    def _update_cartesio_selected_filter_button_text(self) -> None:
        """
        Aggiorna il testo del pulsante filtro selezionati della tab Cartesio.
        """
        if not hasattr(self, "btn_cartesio_filter_selected"):
            return

        if self._cartesio_view_has_manual_subset():
            self.btn_cartesio_filter_selected.setText("Ripristina filtro selezionati")
        else:
            self.btn_cartesio_filter_selected.setText("Filtra selezionati")

    def _toggle_jobs_subset_from_cartesio(self) -> None:
        """
        Toggle one-shot:
        - se il subset NON è attivo: prende tutte le righe attualmente visibili
          nelle tre tabelle Cartesio e le mostra in tab Lavori;
        - se il subset è già attivo: ripristina la vista completa della tab Lavori.
        """
        if self._jobs_view_has_active_subset():
            self.jobs_subset_from_cartesio_ids = set()
            self._update_cartesio_jobs_button_text()
            self.tabs.setCurrentIndex(0)
            self.apply_filter()
            self.statusBar().showMessage(
                "Tab Lavori ripristinata alla vista completa",
                5000,
            )
            return

        visible_job_ids = self._visible_cartesio_job_ids()
        if not visible_job_ids:
            QMessageBox.information(
                self,
                "Tab Cartesio",
                "Non ci sono righe visibili in Cartesio da mostrare in tab Lavori.",
            )
            return

        self.jobs_subset_from_cartesio_ids = visible_job_ids
        self.jobs_filter_text = ""

        self._update_cartesio_jobs_button_text()
        self.tabs.setCurrentIndex(0)
        self.apply_filter()
        self.statusBar().showMessage(
            f"Tab Lavori limitata a {len(visible_job_ids)} righe visibili in Cartesio",
            5000,
        )

    def _toggle_cartesio_selected_filter(self) -> None:
        """
        Toggle del filtro manuale locale Cartesio:
        - se non attivo: limita Cartesio ai job selezionati manualmente;
        - se già attivo: ripristina la vista Cartesio completa.
        """
        if self._cartesio_view_has_manual_subset():
            self.cartesio_manual_subset_ids = set()
            self.apply_filter()
            self._update_cartesio_selected_filter_button_text()
            self.statusBar().showMessage(
                "Filtro selezionati Cartesio ripristinato",
                5000,
            )
            return

        selected_job_ids = self._selected_cartesio_job_ids()
        if not selected_job_ids:
            QMessageBox.information(
                self,
                "Tab Cartesio",
                "Seleziona almeno una riga in Cartesio prima di filtrare i selezionati.",
            )
            return

        self.cartesio_manual_subset_ids = selected_job_ids
        self.apply_filter()
        self._update_cartesio_selected_filter_button_text()
        self.statusBar().showMessage(
            f"Tab Cartesio limitata a {len(selected_job_ids)} righe selezionate",
            5000,
        )


    def _apply_jobs_filter_view(self, text: str) -> None:
        """
        Applica il filtro della tab Lavori.
        """
        rows = self._filtered_rows_from_fields(
            self.all_rows,
            self._jobs_filter_fields(),
            text,
        )

        if self.jobs_subset_from_cartesio_ids:
            rows = [
                row
                for row in rows
                if int(row.get("id") or 0) in self.jobs_subset_from_cartesio_ids
            ]

        if not self.user_sort_active:
            self._apply_default_order(rows)

        self.model.set_rows(rows)

        if self.user_sort_active:
            self._reapply_current_sort()

        self._resize_name_columns_to_contents()

    def _apply_cartesio_filter_view(self, scope: str, text: str) -> None:
        """
        Applica il filtro a una dashboard Cartesio (PRG o COS),
        preservando un eventuale sort manuale locale.
        """
        normalized_scope = str(scope or "").strip().upper()

        if normalized_scope == "COS":
            source_rows = self.cartesio_cos_all_rows
            model = self.cartesio_cos_model
            table = self.tbl_cartesio_cos
            user_sort_active = self.cartesio_cos_user_sort_active
        elif normalized_scope == "ACC":
            source_rows = self.cartesio_acc_all_rows
            model = self.cartesio_acc_model
            table = self.tbl_cartesio_acc
            user_sort_active = self.cartesio_acc_user_sort_active
        else:
            source_rows = self.cartesio_prg_all_rows
            model = self.cartesio_prg_model
            table = self.tbl_cartesio_prg
            user_sort_active = self.cartesio_prg_user_sort_active

        rows = self._filtered_rows_from_fields(
            source_rows,
            self._cartesio_filter_fields(normalized_scope),
            text,
        )

        if self.cartesio_manual_subset_ids:
            rows = [
                row
                for row in rows
                if int(row.get("job_id") or 0) in self.cartesio_manual_subset_ids
            ]

        model.set_rows(rows)

        if user_sort_active:
            self._reapply_table_sort(table, model)

    def _apply_filter_to_all_views(self) -> None:
        """
        Punto unico di applicazione dei filtri salvati per Lavori e Cartesio.
        """
        jobs_text = self._normalized_jobs_filter_text()
        cartesio_text = self._normalized_cartesio_filter_text()

        self._apply_jobs_filter_view(jobs_text)
        self._apply_cartesio_filter_view("PRG", cartesio_text)
        self._apply_cartesio_filter_view("COS", cartesio_text)
        self._apply_cartesio_filter_view("ACC", cartesio_text)

    def _reset_to_default_order(self):
        """
        Ripristina l'ordinamento base del programma:
        in alto i lavori con l'ultima modifica effettiva più recente.
        """
        self.user_sort_active = False

        header = self.table.horizontalHeader()

        # Prova ad azzerare anche l'indicatore grafico del sort sull'header.
        try:
            header.setSortIndicator(-1, Qt.AscendingOrder)
        except Exception:
            logging.debug("Impossibile azzerare il sort indicator dell'header", exc_info=True)

        self.apply_filter()
        self.statusBar().showMessage("Ordinamento default ripristinato", 4000)

    def open_header_context_menu(self, pos: QPoint):
        """
        Menu contestuale dell'intestazione colonne.
        """
        header = self.table.horizontalHeader()
        menu = QMenu(self)

        act_reset_default = menu.addAction("Ripristina ordinamento default")

        chosen = menu.exec(header.viewport().mapToGlobal(pos))
        if not chosen:
            return

        if chosen == act_reset_default:
            self._reset_to_default_order()

    def _apply_default_order(self, rows=None):
        """
        Ordine base del programma:
        in alto i lavori con l'ultima modifica effettiva più recente.

        Regola aggiuntiva per i gruppi eccezione:
        - le righe MANUAL con lo stesso exception_group_code devono restare
          consecutive nella vista;
        - se una riga del gruppo riceve un evento più recente, tutto il gruppo
          eredita quel rank e sale insieme.
        """
        target = self.all_rows if rows is None else rows

        group_rank_map = {}

        for row in target:
            is_manual_exception = (
                str(row.get("exception_mode", "") or "").strip().upper() == "MANUAL"
            )
            group_code = str(row.get("exception_group_code", "") or "").strip().upper()

            if not is_manual_exception or not group_code:
                continue

            latest_event_id = int(row.get("audit_latest_event_id") or 0)
            latest_event_ts = str(row.get("audit_latest_event_ts") or "")
            created_at = str(row.get("created_at") or "")
            row_id = int(row.get("id") or 0)

            info = group_rank_map.setdefault(
                group_code,
                {
                    "latest_event_id": 0,
                    "latest_event_ts": "",
                    "created_at": "",
                    "anchor_row_id": 0,
                },
            )

            if latest_event_id > int(info.get("latest_event_id") or 0):
                info["latest_event_id"] = latest_event_id

            if latest_event_ts > str(info.get("latest_event_ts") or ""):
                info["latest_event_ts"] = latest_event_ts

            if created_at > str(info.get("created_at") or ""):
                info["created_at"] = created_at

            if row_id > int(info.get("anchor_row_id") or 0):
                info["anchor_row_id"] = row_id

        def sort_key(row):
            row_id = int(row.get("id") or 0)
            latest_event_id = int(row.get("audit_latest_event_id") or 0)
            latest_event_ts = str(row.get("audit_latest_event_ts") or "")
            created_at = str(row.get("created_at") or "")

            is_manual_exception = (
                str(row.get("exception_mode", "") or "").strip().upper() == "MANUAL"
            )
            group_code = str(row.get("exception_group_code", "") or "").strip().upper()

            if is_manual_exception and group_code:
                info = group_rank_map.get(group_code) or {}

                return (
                    int(info.get("latest_event_id") or latest_event_id),
                    str(info.get("latest_event_ts") or latest_event_ts),
                    str(info.get("created_at") or created_at),
                    int(info.get("anchor_row_id") or row_id),
                    group_code,
                    row_id,
                )

            return (
                latest_event_id,
                latest_event_ts,
                created_at,
                row_id,
                f"ROW::{row_id}",
                row_id,
            )

        target.sort(key=sort_key, reverse=True)
        return target
        
    def _configure_table_columns(self):
        """
        Configura la tabella usando direttamente la definizione centralizzata del model.
        """
        header = self.table.horizontalHeader()
        header.setFixedHeight(42)
        header.setDefaultAlignment(Qt.AlignCenter)
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(False)
        header.setCascadingSectionResizes(False)
        header.setMinimumSectionSize(30)

        for column_cfg in self.model.COLUMNS:
            field_key = column_cfg["key"]
            col = self.model.column_index(field_key)

            resize_mode = column_cfg.get("resize", "interactive")
            width = int(column_cfg.get("width", 80))

            header.setSectionResizeMode(col, QHeaderView.Interactive)
            self.table.setColumnWidth(col, width)

    def _resize_name_columns_to_contents(self):
        """
        Adatta solo le colonne che nel model sono marcate come 'content_soft'.
        """
        for column_cfg in self.model.COLUMNS:
            if column_cfg.get("resize") == "content_soft":
                self._resize_column_from_cells(column_cfg["key"])

    def _resize_column_from_cells(self, field_key: str):
        """
        Ridimensiona una colonna testuale in base al contenuto reale delle celle,
        usando i vincoli definiti nel model.

        - field_key: chiave logica colonna, es. "project_name" oppure "dl_name"
        - usa min_width / max_width / width dichiarati in models.py
        - ignora la larghezza del testo dell'header
        """
        try:
            # Recupera indice colonna e configurazione dal model
            col = self.model.column_index(field_key)
            cfg = self.model.column_config(field_key)
        except Exception:
            logging.warning("Impossibile ridimensionare colonna inesistente: %s", field_key)
            return

        # Legge i vincoli dal model
        min_width = int(cfg.get("min_width", cfg.get("width", 80)))
        max_width = int(cfg.get("max_width", min_width))
        padding = 28

        # Misura la larghezza del testo con il font attuale della tabella
        fm = self.table.fontMetrics()
        width = min_width

        # Limita il numero di righe da controllare per non rallentare troppo la GUI
        rows_to_check = min(self.model.rowCount(), 300)

        for row in range(rows_to_check):
            idx = self.model.index(row, col)

            # Testo realmente mostrato in cella
            text = str(idx.data(Qt.DisplayRole) or "").replace("\n", " ").strip()

            if not text:
                continue

            # Calcola la larghezza necessaria per contenere il testo
            text_width = fm.horizontalAdvance(text) + padding
            width = max(width, text_width)

            # Se superi il massimo, ti fermi subito
            if width >= max_width:
                width = max_width
                break

        # Applica la larghezza finale alla colonna
        self.table.setColumnWidth(col, width)

    def _scan_override_field_for_column(self, column: int) -> str | None:
        """
        Ricava il campo overrideabile dalla chiave logica della colonna,
        delegando la lista dei campi ammessi al model.
        """
        field_key = self._column_key(column)
        if not field_key:
            return None

        if field_key in self.model.OVERRIDEABLE_SCAN_FIELDS:
            return field_key

        return None

    def _job_has_scan_override(self, job, field_key: str) -> bool:
        """
        Verifica se il lavoro ha già un override manuale attivo per quel campo.
        """
        override_fields = set(job.get("scan_override_fields") or [])
        return field_key in override_fields

    def _path_for_column_key(self, job, column_key: str) -> str:
        """
        Mappa la colonna logica al path da aprire con doppio click.
        """
        scan = job.get("scan", {})
        project_base_path = str(job.get("project_base_path", "") or "").strip()
        project_mode = str(job.get("project_mode", "GTN") or "GTN").strip().upper()
        manual_tracciamento_path = str(job.get("project_tracciamento_manual_path", "") or "").strip()
        is_exception = str(job.get("exception_mode", "") or "").strip().upper() == "MANUAL"

        if is_exception:
            manual_project_control_path = str(
                job.get("manual_project_control_path", "") or ""
            ).strip()
            manual_dl_control_path = str(
                job.get("manual_dl_control_path", "") or ""
            ).strip()
            manual_cartesio_prg_path = str(
                job.get("manual_cartesio_prg_path", "") or ""
            ).strip()
            manual_cartesio_cos_path = str(
                job.get("manual_cartesio_cos_path", "") or ""
            ).strip()
            manual_cartesio_acc_path = str(
                job.get("manual_cartesio_acc_path", "") or ""
            ).strip()

            if column_key == "project_name":
                return manual_project_control_path or project_base_path

            if column_key in {
                "project_rilievo",
                "project_enti",
                "project_revision",
                "permessi_revision",
                "permits_display",
            }:
                return manual_project_control_path

            if column_key == "psc_display":
                return str(job.get("psc_path", "") or "").strip()

            if column_key == "project_tracciamento":
                return manual_tracciamento_path

            if column_key == "cartesio_prg_display":
                return manual_cartesio_prg_path

            if column_key in {"cartesio_acc_prg_display", "cartesio_acc_cos_display"}:
                return manual_cartesio_acc_path

            if column_key == "dl_name":
                return manual_dl_control_path or str(job.get("dl_base_path", "") or "").strip()

            if column_key == "rilievi_dl_display":
                return manual_dl_control_path

            if column_key == "cartesio_cos_display":
                return manual_cartesio_cos_path

            return ""

        project_controls_columns = {
            "project_rilievo",
            "project_enti",
            "project_revision",
            "permessi_revision",
            "permits_display",
            "psc_display",
            "project_tracciamento",
            "cartesio_prg_display",
            "cartesio_acc_prg_display",
        }
        permits_mode = str(job.get("permits_mode", "REQUIRED") or "REQUIRED").strip().upper()
        permits_controls_columns = {"permessi_revision", "permits_display"}

        if column_key == "project_tracciamento":
            if project_mode == "ALTRA_DITTA":
                return manual_tracciamento_path
            if project_mode != "GTN":
                return ""
            if not project_base_path:
                return ""
            return scan.get("project_tracciamento", {}).get("path", "")

        if column_key in project_controls_columns and project_mode != "GTN":
            return ""
        if column_key in permits_controls_columns and permits_mode != "REQUIRED":
            return ""
        if not project_base_path and column_key in project_controls_columns:
            return ""

        if column_key == "project_name":
            if project_mode == "PROGETTO_NON_PREVISTO":
                return ""
            return job.get("project_base_path", "")

        if column_key == "project_rilievo":
            return scan.get("project_rilievo", {}).get("path", "")

        if column_key == "project_enti":
            return scan.get("project_enti", {}).get("path", "")

        if column_key == "project_revision":
            return scan.get("project_revision", {}).get("path", "")

        if column_key in {"permessi_revision", "permits_display"}:
            return scan.get("permessi_revision", {}).get("path", "")

        if column_key == "psc_display":
            return job.get("psc_path", "")

        if column_key == "cartesio_prg_display":
            return scan.get("cartesio_prg", {}).get("path", "")

        if column_key in {"cartesio_acc_prg_display", "cartesio_acc_cos_display"}:
            return scan.get("cartesio_acc", {}).get("path", "")

        if column_key == "dl_name":
            return job.get("dl_base_path", "")

        if column_key == "rilievi_dl_display":
            return scan.get("rilievi_dl", {}).get("path", "")

        if column_key == "cartesio_cos_display":
            return scan.get("cartesio_cos", {}).get("path", "")

        return ""

    def _configure_cartesio_table(self, table: QTableView, model: CartesioTableModel) -> None:
        table.setModel(model)
        table.setItemDelegate(PreserveForegroundDelegate(table))
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.setAlternatingRowColors(True)
        table.setWordWrap(False)
        table.setTextElideMode(Qt.ElideRight)
        table.setSortingEnabled(True)

        palette = table.palette()
        sel = QColor("#cfe8ff")
        palette.setColor(QPalette.Active, QPalette.Highlight, sel)
        palette.setColor(QPalette.Inactive, QPalette.Highlight, sel)
        table.setPalette(palette)

        table.verticalHeader().setVisible(False)
        table.verticalHeader().setSectionResizeMode(QHeaderView.Fixed)
        table.verticalHeader().setDefaultSectionSize(26)

        header = table.horizontalHeader()
        header.setFixedHeight(40)
        header.setDefaultAlignment(Qt.AlignCenter)
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(False)

        header.sectionClicked.connect(
            lambda _section, scope=model.scope: self._on_cartesio_user_sort_clicked(scope)
        )

        for col_index, column_cfg in enumerate(model.columns):
            table.setColumnWidth(col_index, int(column_cfg.get("width", 120)))

    def _build_cartesio_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)

        layout.addWidget(QLabel("Cartesio PRG"))
        self.tbl_cartesio_prg = QTableView()
        self._configure_cartesio_table(self.tbl_cartesio_prg, self.cartesio_prg_model)
        self.tbl_cartesio_prg.doubleClicked.connect(
            lambda index: self._handle_cartesio_dashboard_double_click("PRG", index)
        )
        layout.addWidget(self.tbl_cartesio_prg, 1)

        layout.addWidget(QLabel("Cartesio COS"))
        self.tbl_cartesio_cos = QTableView()
        self._configure_cartesio_table(self.tbl_cartesio_cos, self.cartesio_cos_model)
        self.tbl_cartesio_cos.doubleClicked.connect(
            lambda index: self._handle_cartesio_dashboard_double_click("COS", index)
        )
        layout.addWidget(self.tbl_cartesio_cos, 1)

        layout.addWidget(QLabel("Cartesio ACC"))
        self.tbl_cartesio_acc = QTableView()
        self._configure_cartesio_table(self.tbl_cartesio_acc, self.cartesio_acc_model)
        self.tbl_cartesio_acc.doubleClicked.connect(
            lambda index: self._handle_cartesio_dashboard_double_click("ACC", index)
        )
        layout.addWidget(self.tbl_cartesio_acc, 1)

        btns = QHBoxLayout()

        self.btn_cartesio_filter_selected = QPushButton("Filtra selezionati")
        self.btn_cartesio_filter_selected.clicked.connect(self._toggle_cartesio_selected_filter)

        self.btn_cartesio_show_in_jobs = QPushButton("Mostra visibili in tab Lavori")
        self.btn_cartesio_show_in_jobs.clicked.connect(self._toggle_jobs_subset_from_cartesio)

        btns.addWidget(self.btn_cartesio_filter_selected)
        btns.addWidget(self.btn_cartesio_show_in_jobs)
        btns.addStretch(1)

        layout.addLayout(btns)

        self._update_cartesio_selected_filter_button_text()
        self._update_cartesio_jobs_button_text()

        return widget

    def _current_cartesio_row(self, scope: str):
        normalized_scope = str(scope or "").strip().upper()

        if normalized_scope == "COS":
            table = self.tbl_cartesio_cos
            model = self.cartesio_cos_model
        elif normalized_scope == "ACC":
            table = self.tbl_cartesio_acc
            model = self.cartesio_acc_model
        else:
            table = self.tbl_cartesio_prg
            model = self.cartesio_prg_model

        index = table.currentIndex()
        if not index.isValid():
            return None

        return model.get_row(index.row())

    def _cartesio_dashboard_column_key(self, model, column: int) -> str | None:
        """
        Ricava la chiave logica della colonna nella dashboard Cartesio.

        Supporta sia il model vecchio (model.COLUMNS) sia quello nuovo
        eventualmente reso scope-aware (model.columns).
        """
        try:
            if hasattr(model, "column_key"):
                return model.column_key(column)
        except Exception:
            logging.debug("column_key non disponibile sul model Cartesio", exc_info=True)

        columns = getattr(model, "columns", None)
        if columns is None:
            columns = getattr(model, "COLUMNS", None)

        if not isinstance(columns, list):
            return None

        if not (0 <= column < len(columns)):
            return None

        try:
            return str(columns[column].get("key") or "").strip() or None
        except Exception:
            logging.debug("Impossibile leggere la chiave colonna Cartesio", exc_info=True)
            return None


    def _cartesio_dashboard_path_for_column(self, row, column_key: str) -> str:
        """
        Path da aprire con doppio click nella dashboard Cartesio.

        Regola aggiornata:
        - colonne cartella PRG / DL -> aprono la rispettiva cartella
        - colonne Cartesio PRG / COS -> per righe eccezione aprono i path
          manuali associati al codice Cartesio
        - tutte le altre colonne -> NON aprono path qui, quindi il caller
          continuerà con l'apertura del dialog Cartesio
        """
        if not row or not column_key:
            return ""

        is_exception = str(row.get("exception_mode", "") or "").strip().upper() == "MANUAL"

        if column_key == "dl_name":
            if is_exception:
                return (
                    str(row.get("manual_dl_control_path", "") or "").strip()
                    or str(row.get("dl_base_path", "") or "").strip()
                )
            return str(row.get("dl_base_path", "") or "").strip()

        if column_key == "project_name_display":
            project_mode = str(row.get("project_mode", "") or "").strip().upper()

            if project_mode == "PROGETTO_NON_PREVISTO":
                return ""

            if is_exception:
                return (
                    str(row.get("manual_project_control_path", "") or "").strip()
                    or str(row.get("project_base_path", "") or "").strip()
                )

            return str(row.get("project_base_path", "") or "").strip()

        if is_exception and column_key == "cartesio_prg_display":
            return str(row.get("manual_cartesio_prg_path", "") or "").strip()

        if is_exception and column_key == "cartesio_cos_display":
            return str(row.get("manual_cartesio_cos_path", "") or "").strip()

        if is_exception and column_key in {"cartesio_acc_display", "cartesio_acc_prg_display", "cartesio_acc_cos_display"}:
            return str(row.get("manual_cartesio_acc_path", "") or "").strip()

        return ""



    def _handle_cartesio_dashboard_double_click(self, scope: str, index) -> None:
        """
        Doppio click dashboard Cartesio:
        - su Cartella PRG / Cartella DL apre la cartella relativa
        - sulle altre colonne mantiene il comportamento attuale
        aprendo il dialog Cartesio
        """
        normalized_scope = str(scope or "").strip().upper()

        if normalized_scope == "COS":
            model = self.cartesio_cos_model
        elif normalized_scope == "ACC":
            model = self.cartesio_acc_model
        else:
            model = self.cartesio_prg_model

        if index is None or not index.isValid():
            return

        row = model.get_row(index.row())
        if not row:
            return

        column_key = self._cartesio_dashboard_column_key(model, index.column())
        path = self._cartesio_dashboard_path_for_column(row, column_key or "")

        if path:
            ok, msg = open_in_explorer(path)
            if not ok:
                QMessageBox.warning(self, "Apertura percorso", msg)
            return

        self._open_cartesio_dialog(int(row["job_id"]), normalized_scope)

    def _open_cartesio_dialog(self, job_id: int, scope: str) -> None:
        dlg = CartesioDialog(self.service, job_id=int(job_id), scope=scope, parent=self)
        dlg.exec()
        updated = self.service.get_row_for_ui(int(job_id))
        if updated:
            self._after_job_updated(updated)
        self._reload_cartesio_tab()

    def _reload_cartesio_tab(self) -> None:
        """
        Ricarica le cache complete della dashboard Cartesio e poi riapplica
        i filtri alle viste.
        """
        try:
            self.cartesio_prg_all_rows = self.service.load_cartesio_rows_for_ui("PRG")
            self.cartesio_cos_all_rows = self.service.load_cartesio_rows_for_ui("COS")
            self.cartesio_acc_all_rows = self.service.load_cartesio_rows_for_ui("ACC")

            self._normalize_cartesio_manual_subset_ids()
            self._apply_filter_to_all_views()
            self._update_cartesio_selected_filter_button_text()
            self._update_cartesio_jobs_button_text()
        except Exception:
            logging.exception("Errore reload tab Cartesio")

    # -------------------------------------------------------------------------
    # UI
    # -------------------------------------------------------------------------

    def _build_ui(self):
        central = QWidget()
        root = QVBoxLayout(central)
        self.setCentralWidget(central)

        toolbar = QToolBar("Azioni")
        toolbar.setMovable(False)
        self.addToolBar(toolbar)

        btn_new = QPushButton("Nuovo lavoro")
        btn_new.clicked.connect(self.add_job)
        toolbar.addWidget(btn_new)

        btn_import = QPushButton("Importa cartelle")
        btn_import.clicked.connect(self.import_jobs_from_parent)
        toolbar.addWidget(btn_import)

        btn_edit = QPushButton("Modifica")
        btn_edit.clicked.connect(self.edit_selected_job)
        toolbar.addWidget(btn_edit)

        btn_delete = QPushButton("Elimina")
        btn_delete.clicked.connect(self.delete_selected_jobs)
        toolbar.addWidget(btn_delete)

        toolbar.addSeparator()

        btn_refresh = QPushButton("Aggiorna tutto")
        btn_refresh.clicked.connect(self.refresh_data)
        toolbar.addWidget(btn_refresh)

        btn_refresh_selected = QPushButton("Aggiorna selezionati")
        btn_refresh_selected.clicked.connect(self.refresh_selected)
        toolbar.addWidget(btn_refresh_selected)

        btn_history = QPushButton("Storico")
        btn_history.clicked.connect(self.open_current_job_history)
        toolbar.addWidget(btn_history)

        toolbar.addSeparator()
        toolbar.addWidget(QLabel("Filtro"))

        self.edt_filter = QLineEdit()
        self.edt_filter.setPlaceholderText("Cerca nella tab Lavori...")
        self.edt_filter.textChanged.connect(self._on_filter_text_changed)
        toolbar.addWidget(self.edt_filter)

        # ------------------------------------------------------------------
        # TABELLA
        # ------------------------------------------------------------------
        self.table = QTableView()

        palette = self.table.palette()
        sel = QColor("#cfe8ff")
        palette.setColor(QPalette.Active, QPalette.Highlight, sel)
        palette.setColor(QPalette.Inactive, QPalette.Highlight, sel)
        self.table.setPalette(palette)

        self.table.setModel(self.model)
        self.table.setItemDelegate(PreserveForegroundDelegate(self.table))
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.doubleClicked.connect(self.handle_double_click)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.open_context_menu)

        header = self.table.horizontalHeader()
        header.sectionClicked.connect(self._on_user_sort_clicked)
        header.setContextMenuPolicy(Qt.CustomContextMenu)
        header.customContextMenuRequested.connect(self.open_header_context_menu)

        # Ottimizzazioni resize/repaint
        self.table.setWordWrap(False)
        self.table.setTextElideMode(Qt.ElideRight)

        vheader = self.table.verticalHeader()
        vheader.setVisible(False)
        vheader.setSectionResizeMode(QHeaderView.Fixed)
        vheader.setDefaultSectionSize(26)

        self._configure_table_columns()
        self.tabs = QTabWidget()

        jobs_tab = QWidget()
        jobs_layout = QVBoxLayout(jobs_tab)
        jobs_layout.setContentsMargins(0, 0, 0, 0)
        jobs_layout.addWidget(self.table)
        self.tabs.addTab(jobs_tab, "Lavori")

        self.cartesio_tab = self._build_cartesio_tab()
        self.tabs.addTab(self.cartesio_tab, "Cartesio")
        self.tabs.currentChanged.connect(self._on_tab_changed)

        root.addWidget(self.tabs)
        self.setStatusBar(QStatusBar())

        self._sync_filter_editor_from_current_tab()

    # -------------------------------------------------------------------------
    # STARTUP
    # -------------------------------------------------------------------------

    def _startup_load(self):
        try:
            self.statusBar().showMessage("Caricamento iniziale dati...")
            self.all_rows = self.service.startup_load()
            self.apply_filter()
            self._reload_cartesio_tab()
            self._resize_name_columns_to_contents()
            self.statusBar().showMessage(f"Lavori caricati: {len(self.all_rows)}", 5000)
        except Exception as exc:
            logging.exception("Errore _startup_load")
            QMessageBox.critical(self, "Errore", f"Errore durante caricamento iniziale:\n{exc}")

    # -------------------------------------------------------------------------
    # SELEZIONE / CACHE
    # -------------------------------------------------------------------------

    def selected_jobs(self):
        """
        Restituisce la lista dei lavori selezionati nella tabella.
        Funziona sulle righe attualmente visibili nel model.
        """
        selection_model = self.table.selectionModel()
        if not selection_model:
            return []

        selected_indexes = selection_model.selectedRows()
        if not selected_indexes:
            return []

        jobs = []
        seen_ids = set()

        for index in selected_indexes:
            if not index.isValid():
                continue

            job = self.model.get_row(index.row())
            if not job:
                continue

            job_id = job.get("id")
            if job_id in seen_ids:
                continue

            seen_ids.add(job_id)
            jobs.append(job)

        return jobs

    def current_job(self):
        index = self.table.currentIndex()
        if not index.isValid():
            return None
        return self.model.get_row(index.row())

    def open_current_job_history(self):
        self.open_job_history()

    def open_job_history(self, job=None):
        target_job = job or self.current_job()
        if not target_job:
            QMessageBox.information(self, "Nessuna riga", "Seleziona un lavoro.")
            return

        dlg = JobHistoryDialog(self.db, target_job, self)
        dlg.exec()

        if dlg.history_state_changed:
            try:
                updated = self.service.get_row_for_ui(int(target_job["id"]))
                if updated:
                    self._after_job_updated(updated)
                    self.statusBar().showMessage(
                        f"Riga marcata come controllata: {target_job['id']}",
                        4000,
                    )
            except Exception as exc:
                logging.exception("Errore refresh dopo storico")
                QMessageBox.critical(
                    self,
                    "Errore",
                    f"Errore durante aggiornamento riga dopo storico:\n{exc}",
                )

    def _replace_row_in_cache(self, updated_row):
        updated_id = updated_row.get("id")
        for i, row in enumerate(self.all_rows):
            if row.get("id") == updated_id:
                self.all_rows[i] = updated_row
                return True
        return False

    def _apply_local_row_update(self, updated_row, force_refilter: bool = False):
        """
        Aggiorna una singola riga nella cache completa e nella tabella visibile.

        Regola:
        - se l'utente NON ha attivato un ordinamento manuale, vale l'ordine base
          "ultima modifica in alto";
        - se invece ha cliccato una colonna, si rispetta quell'ordinamento manuale.
        """
        if not updated_row:
            return

        replaced = self._replace_row_in_cache(updated_row)
        if not replaced:
            self.all_rows.append(updated_row)

        # Ordine base globale
        self._apply_default_order()

        filter_active = self._jobs_view_has_active_filter()
        if force_refilter or filter_active:
            self.apply_filter()
            return

        if not self.user_sort_active:
            self.model.set_rows(self.all_rows)
            self._resize_name_columns_to_contents()
            self._reload_cartesio_tab()
            return

        updated = self.model.update_row_by_id(updated_row["id"], updated_row)
        if not updated:
            self.apply_filter()
            return

        self._reapply_current_sort()
        self._resize_name_columns_to_contents()
        self._reload_cartesio_tab()

    def _after_job_updated(self, updated_row):
        """
        Dopo una modifica a qualsiasi campo del job, la regola UI è:
        - se NON c'è sort manuale e NON c'è filtro, la riga più recente deve andare in cima
          (ordine DB: updated_at DESC).
        - se invece c'è filtro e/o sort manuale, preserva la vista corrente aggiornando localmente.
        """
        filter_active = self._jobs_view_has_active_filter()
        if (not self.user_sort_active) and (not filter_active):
            self.all_rows = self.service.load_jobs_for_ui()
            self.apply_filter()
            self._reload_cartesio_tab()
            return

        self._apply_local_row_update(updated_row)
        self._reload_cartesio_tab()

    # -------------------------------------------------------------------------
    # REFRESH
    # -------------------------------------------------------------------------

    def refresh_data(self):
        """
        Scan totale + salvataggio DB + ricarica GUI dal DB.
        """
        try:
            self.statusBar().showMessage("Aggiornamento dati completo in corso...")
            self.all_rows = self.service.scan_all_and_persist()
            self.apply_filter()
            self._reload_cartesio_tab()
            self.statusBar().showMessage(f"Lavori caricati: {len(self.all_rows)}", 5000)
        except Exception as exc:
            logging.exception("Errore refresh_data")
            QMessageBox.critical(self, "Errore", f"Errore durante refresh dati:\n{exc}")

    def refresh_selected(self):
        """
        Scan dei lavori selezionati + salvataggio DB + update locale GUI.
        Supporta multiselezione.
        """
        jobs = self.selected_jobs()

        if not jobs:
            QMessageBox.information(
                self,
                "Nessuna riga",
                "Seleziona almeno un lavoro da aggiornare.",
            )
            return

        job_ids = [job["id"] for job in jobs]

        try:
            self.statusBar().showMessage("Aggiornamento righe selezionate in corso...")
            updated_rows = self.service.scan_and_persist_jobs(job_ids)

            if not updated_rows:
                raise RuntimeError("Nessuna riga aggiornata.")

            filter_active = self._jobs_view_has_active_filter()

            # Se non c'è un sort manuale, la regola è "ultima modifica in alto".
            # Dopo il persist, l'ordine più affidabile è quello del DB (ORDER BY updated_at DESC).
            if (not self.user_sort_active) and (not filter_active):
                self.all_rows = self.service.load_jobs_for_ui()
                self.apply_filter()
                self._reload_cartesio_tab()
            else:
                # Con filtro attivo o ordinamento manuale, aggiorna localmente e
                # lascia che la tabella rispetti filtro/sort correnti.
                for row in updated_rows:
                    self._apply_local_row_update(row)

            self.statusBar().showMessage(
                f"Righe aggiornate: {len(updated_rows)}",
                5000,
            )

        except Exception as exc:
            logging.exception("Errore refresh_selected")
            QMessageBox.critical(
                self,
                "Errore",
                f"Errore durante refresh righe selezionate:\n{exc}",
            )

    # -------------------------------------------------------------------------
    # FILTRO
    # -------------------------------------------------------------------------

    def apply_filter(self):
        """
        Applica i filtri salvati delle macro-tab Lavori e Cartesio.
        """
        self._apply_filter_to_all_views()

    # -------------------------------------------------------------------------
    # DEFAULT META
    # -------------------------------------------------------------------------

    def _default_meta_fields(self):
        return {
            "permits_mode": "REQUIRED",
            "cartesio_delivery_scope": "NONE",
            "permits_checklist_json": [],
            "permits_notes": "",
            "cartesio_prg_status": "NON IMPOSTATO",
            "cartesio_prg_notes": "",
            "rilievi_dl_status": "NON IMPOSTATO",
            "rilievi_dl_notes": "",
            "cartesio_cos_status": "NON IMPOSTATO",
            "cartesio_cos_notes": "",
            "project_tracciamento_manual_path": "",
            "psc_path": "",
            "psc_status": "NOT_SET",
            "todo_json": [],
            "exception_mode": "STANDARD",
            "exception_reason": "",
            "exception_group_code": "",
            "manual_project_control_path": "",
            "manual_dl_control_path": "",
            "manual_cartesio_prg_code": "",
            "manual_cartesio_prg_path": "",
            "manual_cartesio_cos_code": "",
            "manual_cartesio_cos_path": "",
            "manual_cartesio_acc_code": "",
            "manual_cartesio_acc_path": "",
        }

    def _reload_all_views_from_db(self) -> None:
        """
        Ricarica tutte le viste leggendo dal DB.
        È più semplice e robusto dei refresh locali quando una modifica
        può toccare più righe correlate.
        """
        self.all_rows = self.service.load_jobs_for_ui()
        self.apply_filter()
        self._reload_cartesio_tab()

    def _sync_exception_group_reason_after_save(
        self,
        job_id: int,
        payload: dict,
    ) -> list[int]:
        """
        Se il lavoro appartiene a un gruppo eccezione, sincronizza il motivo
        su tutte le altre righe dello stesso gruppo.
        """
        exception_mode = str(payload.get("exception_mode", "") or "").strip().upper()
        group_code = str(payload.get("exception_group_code", "") or "").strip()
        reason = str(payload.get("exception_reason", "") or "").strip()

        if exception_mode != "MANUAL":
            return []

        if not group_code or not reason:
            return []

        return self.db.sync_exception_group_reason(
            group_code=group_code,
            reason=reason,
            exclude_job_id=int(job_id),
        )
    # -------------------------------------------------------------------------
    # CRUD LAVORI
    # -------------------------------------------------------------------------

    def add_job(self):
        dlg = JobDialog(
            self,
            exception_groups=self.db.list_exception_groups_in_use(),
        )
        if dlg.exec():
            payload = self._default_meta_fields()
            payload.update(dlg.get_payload())

            if str(payload.get("psc_path", "") or "").strip():
                payload["psc_status"] = "PENDING"
            else:
                payload["psc_status"] = "NOT_SET"

            try:
                job_id = self.db.add_job(payload)

                synced_job_ids = self._sync_exception_group_reason_after_save(
                    job_id,
                    payload,
                )

                updated = self.service.scan_and_persist_job(job_id)
                if not updated:
                    raise RuntimeError(
                        f"Lavoro ID {job_id} non trovato dopo l'inserimento."
                    )

                self._reload_all_views_from_db()

                if synced_job_ids:
                    self.statusBar().showMessage(
                        f"Lavoro creato: {job_id} | sincronizzati {len(synced_job_ids)} lavori del gruppo eccezione",
                        5000,
                    )
                else:
                    self.statusBar().showMessage(
                        f"Lavoro creato: {job_id}",
                        5000,
                    )

            except ValueError as exc:
                QMessageBox.warning(self, "Duplicato", str(exc))
                return
            except Exception as exc:
                logging.exception("Errore add_job")
                QMessageBox.critical(
                    self,
                    "Errore",
                    f"Errore durante inserimento:\n{exc}",
                )


    def _ask_import_mode(self):
        box = QMessageBox(self)
        box.setIcon(QMessageBox.Question)
        box.setWindowTitle("Import massivo cartelle")
        box.setText("Come vuoi importare le sottocartelle della cartella selezionata?")
        btn_prg = box.addButton("Importa come PRG", QMessageBox.AcceptRole)
        btn_dl = box.addButton("Importa come DL", QMessageBox.AcceptRole)
        box.addButton("Annulla", QMessageBox.RejectRole)
        box.exec()

        clicked = box.clickedButton()
        if clicked == btn_prg:
            return "PRG"
        if clicked == btn_dl:
            return "DL"
        return None

    def _build_import_payload(self, folder_path: Path, import_mode: str):
        path_str = str(folder_path)

        payload = {
            "project_base_path": "",
            "project_distretto_anno": "",
            "project_name": "",
            "project_mode": "GTN",
            "dl_base_path": "",
            "dl_distretto_anno": "",
            "dl_name": "",
            "dl_insert_date": "",
            "general_notes": "",
        }

        if import_mode == "PRG":
            payload.update(
                {
                    "project_base_path": path_str,
                    "project_distretto_anno": infer_project_distretto_anno(path_str),
                    "project_name": folder_name_from_path(path_str),
                }
            )
        elif import_mode == "DL":
            payload.update(
                {
                    "dl_base_path": path_str,
                    "dl_distretto_anno": infer_dl_distretto_anno(path_str),
                    "dl_name": folder_name_from_path(path_str),
                }
            )
        else:
            raise ValueError(f"Tipo import non supportato: {import_mode}")

        payload.update(self._default_meta_fields())
        return payload

    def import_jobs_from_parent(self):
        import_mode = self._ask_import_mode()
        if not import_mode:
            return

        parent_path = QFileDialog.getExistingDirectory(
            self,
            "Seleziona la cartella base da cui importare le sottocartelle",
            "",
        )
        if not parent_path:
            return

        parent = Path(parent_path)

        try:
            subfolders = sorted(
                [p for p in parent.iterdir() if p.is_dir()],
                key=lambda p: p.name.lower(),
            )
        except Exception as exc:
            logging.exception("Errore lettura cartella base per import massivo")
            QMessageBox.critical(
                self,
                "Errore",
                f"Impossibile leggere la cartella selezionata:\n{exc}",
            )
            return

        if not subfolders:
            QMessageBox.information(
                self,
                "Nessuna sottocartella",
                "La cartella selezionata non contiene sottocartelle da importare.",
            )
            return

        imported = []
        skipped_duplicates = []
        errors = []
        imported_ids = []

        self.statusBar().showMessage("Import massivo in corso...")

        for subfolder in subfolders:
            try:
                folder_str = str(subfolder)

                if import_mode == "PRG" and self.db.exists_project_path(folder_str):
                    skipped_duplicates.append(subfolder.name)
                    continue

                if import_mode == "DL" and self.db.exists_dl_path(folder_str):
                    skipped_duplicates.append(subfolder.name)
                    continue

                payload = self._build_import_payload(subfolder, import_mode)
                job_id = self.db.add_job(payload)
                imported.append(subfolder.name)
                imported_ids.append(job_id)

            except Exception as exc:
                logging.exception("Errore import sottocartella: %s", subfolder)
                errors.append(f"{subfolder.name}: {exc}")

        if imported_ids:
            try:
                self.service.scan_and_persist_jobs(imported_ids)
            except Exception:
                logging.exception("Errore scansione post-import")

            self.all_rows = self.service.load_jobs_for_ui()
            self.apply_filter()
        else:
            self.statusBar().clearMessage()

        summary_lines = [
            f"Cartella base: {parent}",
            f"Tipo import: {import_mode}",
            f"Sottocartelle trovate: {len(subfolders)}",
            f"Importate: {len(imported)}",
            f"Saltate perché già presenti: {len(skipped_duplicates)}",
            f"Errori: {len(errors)}",
        ]

        if skipped_duplicates:
            preview = "\n".join(f"- {name}" for name in skipped_duplicates[:15])
            summary_lines.append("\nGià presenti:\n" + preview)
            if len(skipped_duplicates) > 15:
                summary_lines.append(f"\n... e altre {len(skipped_duplicates) - 15}")

        if errors:
            preview = "\n".join(f"- {msg}" for msg in errors[:10])
            summary_lines.append("\nErrori:\n" + preview)
            if len(errors) > 10:
                summary_lines.append(f"\n... e altri {len(errors) - 10}")

        QMessageBox.information(self, "Import massivo completato", "\n".join(summary_lines))

    def edit_selected_job(self):
        job = self.current_job()
        if not job:
            QMessageBox.information(self, "Nessuna riga", "Seleziona un lavoro da modificare.")
            return

        dlg = JobDialog(
            self,
            job=job,
            exception_groups=self.db.list_exception_groups_in_use(),
        )
        if dlg.exec():
            payload = dlg.get_payload()

            for key in (
                "permits_checklist_json",
                "permits_notes",
                "cartesio_prg_status",
                "cartesio_prg_notes",
                "rilievi_dl_status",
                "rilievi_dl_notes",
                "cartesio_cos_status",
                "cartesio_cos_notes",
                "cartesio_delivery_scope",
                "todo_json",
            ):
                payload[key] = job.get(key)

            old_psc_path = str(job.get("psc_path") or "").strip()
            new_psc_path = str(payload.get("psc_path") or "").strip()
            if new_psc_path != old_psc_path:
                payload["psc_status"] = "PENDING" if new_psc_path else "NOT_SET"
            else:
                payload["psc_status"] = job.get("psc_status", "NOT_SET")

            try:
                self.db.update_job(job["id"], payload)

                synced_job_ids = self._sync_exception_group_reason_after_save(
                    int(job["id"]),
                    payload,
                )

                updated = self.service.scan_and_persist_job(job["id"])
                if not updated:
                    raise RuntimeError(
                        f"Lavoro ID {job['id']} non trovato dopo l'aggiornamento."
                    )

                self._reload_all_views_from_db()

                if synced_job_ids:
                    self.statusBar().showMessage(
                        f"Lavoro aggiornato: {job['id']} | sincronizzati {len(synced_job_ids)} lavori del gruppo eccezione",
                        5000,
                    )
                else:
                    self.statusBar().showMessage(
                        f"Lavoro aggiornato: {job['id']}",
                        4000,
                    )

            except ValueError as exc:
                QMessageBox.warning(self, "Duplicato", str(exc))
                return
            except Exception as exc:
                logging.exception("Errore edit_selected_job")
                QMessageBox.critical(
                    self,
                    "Errore",
                    f"Errore durante modifica lavoro:\n{exc}",
                )


    def delete_selected_jobs(self):
        jobs = self.selected_jobs()

        if not jobs:
            QMessageBox.information(
                self,
                "Nessuna riga",
                "Seleziona almeno un lavoro da eliminare.",
            )
            return

        count = len(jobs)

        if count == 1:
            msg = f"Eliminare il lavoro ID {jobs[0]['id']}?"
        else:
            msg = f"Eliminare i {count} lavori selezionati?"

        ans = QMessageBox.question(
            self,
            "Conferma eliminazione",
            msg,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if ans != QMessageBox.Yes:
            return

        deleted = 0
        errors = []

        for job in jobs:
            try:
                self.service.delete_job(int(job["id"]))
                deleted += 1
                logging.info("Lavoro eliminato: id=%s", job["id"])
            except Exception as exc:
                logging.exception("Errore eliminazione lavoro id=%s", job.get("id"))
                errors.append(f"ID {job.get('id')}: {exc}")

        self.all_rows = self.service.load_jobs_for_ui()
        self.apply_filter()
        self._reload_cartesio_tab()

        if errors:
            QMessageBox.warning(
                self,
                "Eliminazione completata con errori",
                f"Eliminati: {deleted}\n"
                f"Errori: {len(errors)}\n\n" + "\n".join(errors[:10]),
            )
        else:
            self.statusBar().showMessage(f"Lavori eliminati: {deleted}", 5000)

    # -------------------------------------------------------------------------
    # OVERRIDE CAMPI DA SCAN
    # -------------------------------------------------------------------------

    def edit_scan_override(self, job, field_key: str, column_label: str):
        current_value = "" if job.get(field_key) is None else str(job.get(field_key, ""))

        value, ok = QInputDialog.getText(
            self,
            "Modifica valore cella",
            f"{column_label}\n\nInserisci il valore manuale da mostrare in tabella:",
            text=current_value,
        )
        if not ok:
            return

        value = value.strip()
        if not value:
            QMessageBox.warning(
                self,
                "Valore non valido",
                "Il valore manuale non può essere vuoto. Usa 'Ripristina valore automatico'.",
            )
            return

        try:
            self.db.set_scan_override(job["id"], field_key, value)
            updated = self.service.get_row_for_ui(job["id"])
            if not updated:
                raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo il salvataggio override.")

            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Override salvato: {column_label}", 4000)

        except Exception as exc:
            logging.exception("Errore edit_scan_override")
            QMessageBox.critical(self, "Errore", f"Errore durante salvataggio override:\n{exc}")

    def _quick_override_values_for_field(self, field_key: str) -> list[str]:
        """
        Valori rapidi utili per i campi overrideabili che normalmente mostrano simboli.
        Il campo resta comunque libero: i valori rapidi sono solo una scorciatoia.
        """
        symbol_fields = {
            "project_rilievo",
            "project_enti",
            "project_revision",
            "permessi_revision",
            "project_tracciamento",
            "cartesio_prg_display",
            "rilievi_dl_display",
            "cartesio_cos_display",
        }

        if field_key in symbol_fields:
            return ["✅", "❌", "🔄", "-"]

        return []

    def set_scan_override_quick_value(self, job, field_key: str, column_label: str, value: str):
        value = str(value or "").strip()
        if not value:
            return

        try:
            self.db.set_scan_override(job["id"], field_key, value)
            updated = self.service.get_row_for_ui(job["id"])
            if not updated:
                raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo il salvataggio override.")

            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Override salvato: {column_label} -> {value}", 4000)

        except Exception as exc:
            logging.exception("Errore set_scan_override_quick_value")
            QMessageBox.critical(self, "Errore", f"Errore durante salvataggio override:\n{exc}")

    def edit_project_tracciamento_manual_path(self, job):
        project_mode = str(job.get("project_mode", "GTN") or "GTN").strip().upper()
        if project_mode != "ALTRA_DITTA":
            QMessageBox.information(
                self,
                "Link manuale tracciamento",
                "Il link manuale del tracciamento è previsto solo per lavori in stato 'ALTRA DITTA'.",
            )
            return

        current_path = (job.get("project_tracciamento_manual_path") or "").strip()

        value, ok = QInputDialog.getText(
            self,
            "Link manuale File Tracciamento",
            "Inserisci il percorso manuale del tracciamento:",
            text=current_path,
        )
        if not ok:
            return

        value = value.strip()
        if not value:
            QMessageBox.warning(
                self,
                "Valore non valido",
                "Il link manuale non può essere vuoto.",
            )
            return

        try:
            self.db.update_meta_fields(
                job["id"],
                project_tracciamento_manual_path=value,
            )

            updated = self.service.get_row_for_ui(job["id"])
            if not updated:
                raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo il salvataggio link manuale.")

            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Link manuale tracciamento aggiornato: {job['id']}", 4000)

        except Exception as exc:
            logging.exception("Errore edit_project_tracciamento_manual_path")
            QMessageBox.critical(self, "Errore", f"Errore durante aggiornamento link manuale:\n{exc}")

    def clear_project_tracciamento_manual_path(self, job):
        current_path = (job.get("project_tracciamento_manual_path") or "").strip()
        if not current_path:
            QMessageBox.information(
                self,
                "Link manuale tracciamento",
                "Non esiste alcun link manuale da rimuovere.",
            )
            return

        ans = QMessageBox.question(
            self,
            "Rimuovi link manuale tracciamento",
            "Vuoi rimuovere il link manuale del File Tracciamento?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if ans != QMessageBox.Yes:
            return

        try:
            self.db.update_meta_fields(
                job["id"],
                project_tracciamento_manual_path="",
            )

            updated = self.service.get_row_for_ui(job["id"])
            if not updated:
                raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo la rimozione link manuale.")

            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Link manuale tracciamento rimosso: {job['id']}", 4000)

        except Exception as exc:
            logging.exception("Errore clear_project_tracciamento_manual_path")
            QMessageBox.critical(self, "Errore", f"Errore durante rimozione link manuale:\n{exc}")

    def clear_scan_override(self, job, field_key: str, column_label: str):
        ans = QMessageBox.question(
            self,
            "Ripristina valore automatico",
            f"Ripristinare il valore automatico per la colonna '{column_label}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if ans != QMessageBox.Yes:
            return

        try:
            self.db.clear_scan_override(job["id"], field_key)
            updated = self.service.get_row_for_ui(job["id"])
            if not updated:
                raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo il ripristino override.")

            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Valore automatico ripristinato: {column_label}", 4000)

        except Exception as exc:
            logging.exception("Errore clear_scan_override")
            QMessageBox.critical(self, "Errore", f"Errore durante ripristino valore automatico:\n{exc}")

    # -------------------------------------------------------------------------
    # OPEN PATH
    # -------------------------------------------------------------------------

    def handle_double_click(self, index):
        """
        Doppio click:
        - colonna storico -> apre il dialog storico riga
        - altre colonne -> apre il path coerente con la colonna selezionata
        """
        job = self.model.get_row(index.row())
        if not job:
            return

        column_key = self._column_key(index.column())
        if not column_key:
            return

        if column_key == "history_alert_display":
            self.open_job_history(job)
            return

        path = self._path_for_column_key(job, column_key)

        if path:
            ok, msg = open_in_explorer(path)
            if not ok:
                QMessageBox.warning(self, "Apertura percorso", msg)

    def open_context_menu(self, pos: QPoint):
        """
        Menu contestuale della tabella.
        """
        index = self.table.indexAt(pos)
        if not index.isValid():
            return

        self.table.setCurrentIndex(index)
        self.table.selectRow(index.row())

        job = self.model.get_row(index.row())
        if not job:
            return

        menu = QMenu(self)

        column_key = self._column_key(index.column())
        if not column_key:
            return

        column_label = self.model.headerData(index.column(), Qt.Horizontal)
        field_key = self._scan_override_field_for_column(index.column())

        act_history = menu.addAction("Storico riga...")
        act_edit_job = menu.addAction("Modifica lavoro...")
        menu.addSeparator()

        act_edit_override = None
        act_reset_override = None
        quick_override_actions = {}

        if field_key:
            quick_values = self._quick_override_values_for_field(field_key)
            if quick_values:
                quick_menu = menu.addMenu("Valori rapidi override")
                for quick_value in quick_values:
                    action = quick_menu.addAction(f"Imposta {quick_value}")
                    quick_override_actions[action] = quick_value

            act_edit_override = menu.addAction("Modifica valore cella...")
            if self._job_has_scan_override(job, field_key):
                act_reset_override = menu.addAction("Ripristina valore automatico")

        act_permits = None
        act_psc_path = None
        act_psc_ready = None
        act_psc_unready = None
        act_psc_clear = None
        act_tracciamento_manual_path = None
        act_tracciamento_manual_path_clear = None
        act_cart_prg = None
        act_cart_acc = None
        act_rilievi_dl = None
        act_cart_cos = None

        if column_key == "permits_display":
            if field_key:
                menu.addSeparator()
            act_permits = menu.addAction("Modifica checklist Permessi...")

        elif column_key == "project_tracciamento":
            project_mode = str(job.get("project_mode", "GTN") or "GTN").strip().upper()
            if project_mode == "ALTRA_DITTA":
                menu.addSeparator()
                act_tracciamento_manual_path = menu.addAction("Imposta/Modifica link manuale...")
                if (job.get("project_tracciamento_manual_path") or "").strip():
                    act_tracciamento_manual_path_clear = menu.addAction("Rimuovi link manuale")

        elif column_key == "psc_display":
            act_psc_path = menu.addAction("Imposta/Modifica percorso PSC...")
            if (job.get("psc_path") or "").strip():
                act_psc_ready = menu.addAction("Segna PSC pronto")
                if (job.get("psc_status") or "").strip().upper() == "READY":
                    act_psc_unready = menu.addAction("Rimuovi conferma PSC")
                act_psc_clear = menu.addAction("Cancella percorso PSC")

        elif column_key == "cartesio_prg_display":
            if field_key:
                menu.addSeparator()
            act_cart_prg = menu.addAction("Imposta stato Cartesio PRG...")

        elif column_key in {"cartesio_acc_prg_display", "cartesio_acc_cos_display"}:
            if field_key:
                menu.addSeparator()
            act_cart_acc = menu.addAction("Imposta stato Cartesio ACC...")

        elif column_key == "rilievi_dl_display":
            if field_key:
                menu.addSeparator()
            act_rilievi_dl = menu.addAction("Imposta stato Rilievi DL...")

        elif column_key == "cartesio_cos_display":
            if field_key:
                menu.addSeparator()
            act_cart_cos = menu.addAction("Imposta stato Cartesio COS...")

        menu.addSeparator()
        act_todo = menu.addAction("ToDo generale lavoro...")

        chosen = menu.exec(self.table.viewport().mapToGlobal(pos))
        if not chosen:
            return

        if chosen == act_history:
            self.open_job_history(job)
        elif chosen == act_edit_job:
            self.edit_selected_job()
        elif chosen in quick_override_actions and field_key:
            self.set_scan_override_quick_value(
                job,
                field_key,
                column_label,
                quick_override_actions[chosen],
            )
        elif chosen == act_edit_override and field_key:
            self.edit_scan_override(job, field_key, column_label)
        elif chosen == act_reset_override and field_key:
            self.clear_scan_override(job, field_key, column_label)
        elif chosen == act_todo:
            self.edit_todo(job)
        elif chosen == act_permits:
            self.edit_permessi(job)
        elif chosen == act_tracciamento_manual_path:
            self.edit_project_tracciamento_manual_path(job)
        elif chosen == act_tracciamento_manual_path_clear:
            self.clear_project_tracciamento_manual_path(job)
        elif chosen == act_psc_path:
            self.edit_psc_path(job)
        elif chosen == act_psc_ready:
            self.set_psc_ready(job)
        elif chosen == act_psc_unready:
            self.unset_psc_ready(job)
        elif chosen == act_psc_clear:
            self.clear_psc_path(job)
        elif chosen == act_cart_prg:
            self.edit_cartesio_prg(job)
        elif chosen == act_rilievi_dl:
            self.edit_rilievi_dl(job)
        elif chosen == act_cart_cos:
            self.edit_cartesio_cos(job)
        elif chosen == act_cart_acc:
            self.edit_cartesio_acc(job) 

    # -------------------------------------------------------------------------
    # EDIT META MANUALI: NO SCAN
    # -------------------------------------------------------------------------

    def edit_psc_path(self, job):
        current_path = (job.get("psc_path") or "").strip()

        value, ok = QInputDialog.getText(
            self,
            "Percorso PSC",
            "Inserisci il percorso cartella PSC:",
            text=current_path,
        )
        if not ok:
            return

        value = value.strip()
        if not value:
            QMessageBox.warning(
                self,
                "Valore non valido",
                "Il percorso PSC non può essere vuoto.",
            )
            return

        try:
            self.db.update_meta_fields(
                job["id"],
                psc_path=value,
                psc_status="PENDING",
            )

            updated = self.service.refresh_row_without_rescan(
                job,
                psc_path=value,
                psc_status="PENDING",
            )
            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Percorso PSC aggiornato: {job['id']}", 4000)

        except Exception as exc:
            logging.exception("Errore edit_psc_path")
            QMessageBox.critical(self, "Errore", f"Errore durante aggiornamento percorso PSC:\n{exc}")

    def set_psc_ready(self, job):
        psc_path = (job.get("psc_path") or "").strip()
        if not psc_path:
            QMessageBox.information(
                self,
                "Percorso mancante",
                "Imposta prima un percorso PSC.",
            )
            return

        try:
            self.db.update_meta_fields(job["id"], psc_status="READY")

            updated = self.service.refresh_row_without_rescan(
                job,
                psc_status="READY",
            )
            self._after_job_updated(updated)
            self.statusBar().showMessage(f"PSC pronto confermato: {job['id']}", 4000)

        except Exception as exc:
            logging.exception("Errore set_psc_ready")
            QMessageBox.critical(self, "Errore", f"Errore durante conferma PSC:\n{exc}")

    def unset_psc_ready(self, job):
        psc_path = (job.get("psc_path") or "").strip()
        if not psc_path:
            QMessageBox.information(
                self,
                "Percorso mancante",
                "Non esiste alcun percorso PSC da riportare in stato in corso.",
            )
            return

        try:
            self.db.update_meta_fields(job["id"], psc_status="PENDING")

            updated = self.service.refresh_row_without_rescan(
                job,
                psc_status="PENDING",
            )
            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Conferma PSC rimossa: {job['id']}", 4000)

        except Exception as exc:
            logging.exception("Errore unset_psc_ready")
            QMessageBox.critical(self, "Errore", f"Errore durante rimozione conferma PSC:\n{exc}")

    def clear_psc_path(self, job):
        ans = QMessageBox.question(
            self,
            "Cancella percorso PSC",
            "Vuoi cancellare il percorso PSC salvato?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if ans != QMessageBox.Yes:
            return

        try:
            self.db.update_meta_fields(
                job["id"],
                psc_path="",
                psc_status="NOT_SET",
            )

            updated = self.service.refresh_row_without_rescan(
                job,
                psc_path="",
                psc_status="NOT_SET",
            )
            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Percorso PSC cancellato: {job['id']}", 4000)

        except Exception as exc:
            logging.exception("Errore clear_psc_path")
            QMessageBox.critical(self, "Errore", f"Errore durante cancellazione percorso PSC:\n{exc}")

    def set_permits_required(self, job):
        try:
            self.db.update_meta_fields(job["id"], permits_mode="REQUIRED")

            updated = self.service.scan_and_persist_job(job["id"])
            if not updated:
                raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo aggiornamento permessi.")

            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Permessi attivati: {job['id']}", 4000)

        except Exception as exc:
            logging.exception("Errore set_permits_required")
            QMessageBox.critical(self, "Errore", f"Errore durante attivazione permessi:\n{exc}")

    def set_permits_not_required(self, job):
        ans = QMessageBox.question(
            self,
            "Disattiva gestione permessi",
            "Impostare questo lavoro come 'NO permessi'?\n\n"
            "La revisione permessi non verrà più cercata e la colonna mostrerà '-'.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if ans != QMessageBox.Yes:
            return

        try:
            self.db.update_meta_fields(job["id"], permits_mode="NOT_REQUIRED")

            updated = self.service.scan_and_persist_job(job["id"])
            if not updated:
                raise RuntimeError(f"Lavoro ID {job['id']} non trovato dopo aggiornamento permessi.")

            self._after_job_updated(updated)
            self.statusBar().showMessage(f"Permessi disattivati: {job['id']}", 4000)

        except Exception as exc:
            logging.exception("Errore set_permits_not_required")
            QMessageBox.critical(self, "Errore", f"Errore durante disattivazione permessi:\n{exc}")

    def edit_permessi(self, job):
        permits_mode = str(job.get("permits_mode", "REQUIRED") or "REQUIRED").strip().upper()
        if permits_mode != "REQUIRED":
            QMessageBox.information(
                self,
                "Permessi disattivati",
                "Per questo lavoro i permessi sono impostati su 'NO'.\n"
                "Riattivali prima di modificare la checklist.",
            )
            return

        dlg = PermitsDialog(
            self,
            checklist=job.get("permits_checklist_json"),
            notes=job.get("permits_notes", ""),
        )

        if dlg.exec():
            checklist, notes = dlg.get_payload()

            def _as_bool(value):
                if isinstance(value, bool):
                    return value
                if value is None:
                    return False
                if isinstance(value, (int, float)):
                    return value != 0
                if isinstance(value, str):
                    return value.strip().lower() in {"1", "true", "yes", "y", "on", "si", "sì"}
                return bool(value)

            normalized_checklist = []
            for item in checklist or []:
                normalized_item = {
                    "name": str(item.get("name", "")).strip(),
                    "required": _as_bool(item.get("required")),
                    "obtained": _as_bool(item.get("obtained")),
                    "notes": str(item.get("notes", "")).strip(),
                }

                if normalized_item["name"]:
                    normalized_checklist.append(normalized_item)

            checklist = normalized_checklist
            notes = str(notes or "").strip()

            try:
                self.db.update_meta_fields(
                    job["id"],
                    permits_checklist_json=checklist,
                    permits_notes=notes,
                )

                updated = self.service.refresh_row_without_rescan(
                    job,
                    permits_checklist_json=checklist,
                    permits_notes=notes,
                )
                self._after_job_updated(updated)
                self.statusBar().showMessage(f"Permessi aggiornati: {job['id']}", 4000)

            except Exception as exc:
                logging.exception("Errore edit_permessi")
                QMessageBox.critical(self, "Errore", f"Errore durante aggiornamento permessi:\n{exc}")

    def edit_cartesio_prg(self, job):
        self._open_cartesio_dialog(int(job["id"]), "PRG")

    def edit_rilievi_dl(self, job):
        dlg = StatusDialog(
            "Stato Rilievi DL",
            RILIEVI_DL_STATES,
            current_status=job.get("rilievi_dl_status", "NON IMPOSTATO"),
            notes=job.get("rilievi_dl_notes", ""),
            parent=self,
        )

        if dlg.exec():
            payload = dlg.get_payload()

            try:
                self.db.update_meta_fields(
                    job["id"],
                    rilievi_dl_status=payload["status"],
                    rilievi_dl_notes=payload["notes"],
                )

                updated = self.service.refresh_row_without_rescan(
                    job,
                    rilievi_dl_status=payload["status"],
                    rilievi_dl_notes=payload["notes"],
                )
                self._after_job_updated(updated)
                self.statusBar().showMessage(f"Rilievi DL aggiornati: {job['id']}", 4000)

            except Exception as exc:
                logging.exception("Errore edit_rilievi_dl")
                QMessageBox.critical(
                    self,
                    "Errore",
                    f"Errore durante aggiornamento Rilievi DL:\n{exc}",
                )

    def edit_cartesio_acc(self, job):
        self._open_cartesio_dialog(int(job["id"]), "ACC")

    def edit_cartesio_cos(self, job):
        self._open_cartesio_dialog(int(job["id"]), "COS")

    def edit_todo(self, job):
        dlg = TodoDialog(self, todo_items=job.get("todo_json") or [])

        if dlg.exec():
            todo_items = dlg.get_payload()

            try:
                self.db.update_meta_fields(job["id"], todo_json=todo_items)

                updated = self.service.refresh_row_without_rescan(
                    job,
                    todo_json=todo_items,
                )
                self._after_job_updated(updated)
                self.statusBar().showMessage(f"ToDo aggiornato: {job['id']}", 4000)

            except Exception as exc:
                logging.exception("Errore edit_todo")
                QMessageBox.critical(self, "Errore", f"Errore durante aggiornamento ToDo:\n{exc}")

    # -------------------------------------------------------------------------
    # CHIUSURA
    # -------------------------------------------------------------------------

    def closeEvent(self, event):
        try:
            self.db.close()
        except Exception:
            logging.exception("Errore chiusura DB")
        super().closeEvent(event)


if __name__ == "__main__":
    setup_logging()
    try:
        app = QApplication(sys.argv)
        app.setStyle("Fusion")
        window = MainWindow()
        window.show()
        sys.exit(app.exec())
    except Exception:
        logging.exception("Crash applicazione")
        traceback.print_exc()
        raise