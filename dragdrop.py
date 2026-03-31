# -*- coding: utf-8 -*-
"""Helper condivisi per drag&drop di allegati esterni.

Supporta:
- file locali trascinati da Esplora Risorse / desktop
- mail complete trascinate da Outlook come .msg

Approccio attuale:
- per i file desktop usa i path locali veri
- per Outlook diretto usa un bridge COM che salva la mail in temp come .msg
  e poi restituisce il path temp al flusso Cartesio
"""

from __future__ import annotations

import logging
import os
import struct
from pathlib import Path
from typing import Any, Dict, List, Tuple

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QListWidget

from outlook_drop_bridge import extract_outlook_pending_items_via_com
from utils import safe_filename


WINDOWS_MIME_FILEGROUPDESCRIPTOR_W = 'application/x-qt-windows-mime;value="FileGroupDescriptorW"'
WINDOWS_MIME_FILEGROUPDESCRIPTOR_A = 'application/x-qt-windows-mime;value="FileGroupDescriptor"'

# Layout FILEDESCRIPTOR (offset utile del nome file = 72 byte)
FILEDESCRIPTOR_FILENAME_OFFSET = 72
FILEDESCRIPTORW_SIZE = 592
FILEDESCRIPTORA_SIZE = 332
FILEDESCRIPTORW_FILENAME_BYTES = 520   # 260 WCHAR
FILEDESCRIPTORA_FILENAME_BYTES = 260   # 260 CHAR


def cleanup_temp_drop_file(path: str) -> None:
    """Rimozione difensiva di un file temporaneo generato dal drag&drop."""
    clean = str(path or "").strip()
    if not clean:
        return

    try:
        temp_path = Path(clean)
        if temp_path.is_file():
            temp_path.unlink()
    except Exception:
        logging.warning("Impossibile rimuovere file temporaneo drag&drop: %s", clean, exc_info=True)


def _mime_formats_list(mime_data) -> List[str]:
    """Restituisce l'elenco raw dei formati MIME esposti da Qt."""
    if mime_data is None:
        return []

    try:
        return [str(fmt) for fmt in mime_data.formats()]
    except Exception:
        logging.exception("Impossibile leggere mime_data.formats()")
        return []


def can_extract_attachments_from_mime(mime_data) -> bool:
    """Verifica rapida per dragEnter/dragMove senza fare lavoro pesante."""
    if mime_data is None:
        return False

    try:
        if mime_data.hasUrls():
            for url in mime_data.urls():
                if url.isLocalFile():
                    return True

        formats = {str(fmt) for fmt in mime_data.formats()}
        has_descriptor = any("FileGroupDescriptor" in fmt for fmt in formats)
        return has_descriptor
    except Exception:
        logging.exception("Errore verifica MIME drag&drop")
        return False


def _read_null_terminated_utf16le(raw: bytes) -> str:
    """Estrae una stringa UTF-16LE null-terminated."""
    for index in range(0, len(raw), 2):
        if raw[index:index + 2] == b"\x00\x00":
            raw = raw[:index]
            break

    if len(raw) % 2 == 1:
        raw = raw[:-1]

    return raw.decode("utf-16le", errors="ignore").strip().strip("\x00")


def _read_null_terminated_ansi(raw: bytes) -> str:
    """Estrae una stringa ANSI/null-terminated."""
    raw = raw.split(b"\x00", 1)[0]

    if os.name == "nt":
        try:
            return raw.decode("mbcs", errors="ignore").strip()
        except Exception:
            pass

    return raw.decode("latin-1", errors="ignore").strip()


def _parse_file_group_descriptor_names(descriptor_bytes: bytes, *, wide: bool) -> List[str]:
    """Parsa i nomi dei file presenti in FileGroupDescriptor/FileGroupDescriptorW."""
    names: List[str] = []

    if not descriptor_bytes or len(descriptor_bytes) < 4:
        return names

    try:
        items_count = struct.unpack_from("<I", descriptor_bytes, 0)[0]
    except Exception:
        logging.exception("Impossibile leggere il numero elementi da FileGroupDescriptor")
        return names

    descriptor_size = FILEDESCRIPTORW_SIZE if wide else FILEDESCRIPTORA_SIZE
    filename_bytes = FILEDESCRIPTORW_FILENAME_BYTES if wide else FILEDESCRIPTORA_FILENAME_BYTES

    offset = 4
    for _ in range(items_count):
        chunk = descriptor_bytes[offset:offset + descriptor_size]
        if len(chunk) < descriptor_size:
            logging.warning(
                "FileGroupDescriptor troncato: attesi %s byte, trovati %s",
                descriptor_size,
                len(chunk),
            )
            break

        name_blob = chunk[
            FILEDESCRIPTOR_FILENAME_OFFSET:
            FILEDESCRIPTOR_FILENAME_OFFSET + filename_bytes
        ]

        if wide:
            file_name = _read_null_terminated_utf16le(name_blob)
        else:
            file_name = _read_null_terminated_ansi(name_blob)

        names.append(file_name)
        offset += descriptor_size

    return names


def _extract_descriptor_file_names(mime_data) -> List[str]:
    """Recupera i nomi file dichiarati nel descrittore Outlook/virtual file."""
    if mime_data is None:
        return []

    descriptor_format = None
    descriptor_is_wide = False

    if mime_data.hasFormat(WINDOWS_MIME_FILEGROUPDESCRIPTOR_W):
        descriptor_format = WINDOWS_MIME_FILEGROUPDESCRIPTOR_W
        descriptor_is_wide = True
    elif mime_data.hasFormat(WINDOWS_MIME_FILEGROUPDESCRIPTOR_A):
        descriptor_format = WINDOWS_MIME_FILEGROUPDESCRIPTOR_A
        descriptor_is_wide = False

    if not descriptor_format:
        return []

    try:
        descriptor_bytes = bytes(mime_data.data(descriptor_format))
    except Exception:
        logging.exception("Errore lettura descriptor Outlook da mime_data")
        return []

    return _parse_file_group_descriptor_names(
        descriptor_bytes,
        wide=descriptor_is_wide,
    )


def _extract_local_file_items(mime_data) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Estrae file locali trascinati da desktop/Explorer."""
    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    if mime_data is None or not mime_data.hasUrls():
        return items, errors

    for url in mime_data.urls():
        if not url.isLocalFile():
            continue

        local_path = Path(url.toLocalFile())

        if local_path.is_file():
            items.append(
                {
                    "id": None,
                    "pending": True,
                    "attachment_kind": "file",
                    "source_path": str(local_path),
                    "display_name": local_path.name,
                    "temp_file": False,
                    "subject": "",
                    "sender": "",
                    "received_at": "",
                    "meta_json": {
                        "source": "desktop_drop",
                    },
                }
            )
            continue

        if local_path.exists():
            errors.append(f"Ignorato '{local_path.name}': sono accettati solo file, non cartelle.")

    return items, errors


def _extract_outlook_msg_items(mime_data) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Estrae mail complete .msg trascinate da Outlook.

    Nuova strategia:
    - usa il descrittore Qt solo per capire che il drop arriva da Outlook
      e per recuperare l'elenco nomi dichiarati
    - il file .msg reale viene salvato in temp via Outlook COM
    """
    if mime_data is None:
        return [], []

    # Se Qt ci fornisce già file locali veri, non è il ramo Outlook diretto.
    if mime_data.hasUrls():
        try:
            if any(url.isLocalFile() for url in mime_data.urls()):
                return [], []
        except Exception:
            logging.exception("Errore controllo URL locali nel drop Outlook")
            return [], []

    expected_names = _extract_descriptor_file_names(mime_data)
    if not expected_names:
        return [], []

    msg_names = [
        str(name or "").strip()
        for name in expected_names
        if str(name or "").strip().lower().endswith(".msg")
    ]

    if not msg_names:
        return [], [
            "Drop Outlook rilevato, ma gli elementi trascinati non risultano mail complete .msg."
        ]

    return extract_outlook_pending_items_via_com(expected_names=msg_names)


def extract_attachments_from_mime_data(mime_data) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Estrazione completa e normalizzazione degli allegati trascinati.

    Regola di precedenza:
    - se esistono file locali veri, uso SOLO quelli
    - il ramo Outlook parte solo quando non ci sono URL locali
    """
    local_items, local_errors = _extract_local_file_items(mime_data)

    if local_items:
        return local_items, local_errors

    outlook_items, outlook_errors = _extract_outlook_msg_items(mime_data)
    return outlook_items, local_errors + outlook_errors


class AttachmentDropListWidget(QListWidget):
    """Lista riusabile che accetta drag&drop di allegati esterni."""

    attachments_dropped = Signal(list, list)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.viewport().setAcceptDrops(True)
        self.setDropIndicatorShown(True)
        self.setDragEnabled(False)
        self.setDefaultDropAction(Qt.CopyAction)

    def dragEnterEvent(self, event) -> None:
        if can_extract_attachments_from_mime(event.mimeData()):
            event.acceptProposedAction()
            return
        super().dragEnterEvent(event)

    def dragMoveEvent(self, event) -> None:
        if can_extract_attachments_from_mime(event.mimeData()):
            event.acceptProposedAction()
            return
        super().dragMoveEvent(event)

    def dropEvent(self, event) -> None:
        items, errors = extract_attachments_from_mime_data(event.mimeData())

        if items or errors:
            self.attachments_dropped.emit(items, errors)
            event.acceptProposedAction()
            return

        super().dropEvent(event)