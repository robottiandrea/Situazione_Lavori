# -*- coding: utf-8 -*-
"""Helper condivisi per drag&drop di allegati esterni.

Supporta:
- file locali trascinati da Esplora Risorse / desktop
- mail complete trascinate da Outlook come .msg

Obiettivo architetturale:
- separare tutta la logica di estrazione MIME dal dialog Cartesio
- rendere il componente riusabile anche in altre tab / finestre future
"""

from __future__ import annotations

import logging
import os
import struct
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QListWidget

from utils import safe_filename


WINDOWS_MIME_FILEGROUPDESCRIPTOR_W = 'application/x-qt-windows-mime;value="FileGroupDescriptorW"'
WINDOWS_MIME_FILEGROUPDESCRIPTOR_A = 'application/x-qt-windows-mime;value="FileGroupDescriptor"'
WINDOWS_MIME_FILECONTENTS = 'application/x-qt-windows-mime;value="FileContents"'
WINDOWS_MIME_FILECONTENTS_INDEX_PREFIX = 'application/x-qt-windows-mime;value="FileContents";index='

# Layout FILEDESCRIPTOR (offset utile del nome file = 72 byte)
# FILEDESCRIPTORW: 592 byte totali
# FILEDESCRIPTORA: 332 byte totali
FILEDESCRIPTOR_FILENAME_OFFSET = 72
FILEDESCRIPTORW_SIZE = 592
FILEDESCRIPTORA_SIZE = 332
FILEDESCRIPTORW_FILENAME_BYTES = 520   # 260 WCHAR
FILEDESCRIPTORA_FILENAME_BYTES = 260   # 260 CHAR


def _drop_temp_dir() -> Path:
    """Cartella temporanea locale per payload drag&drop da Outlook."""
    target = Path(tempfile.gettempdir()) / "situazione_lavori_dragdrop"
    target.mkdir(parents=True, exist_ok=True)
    return target


def cleanup_temp_drop_file(path: str) -> None:
    """Rimozione difensiva di un file temporaneo generato dal drag&drop.

    Non deve mai far fallire la GUI.
    """
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


def _find_filecontents_format(mime_data, index: int, total_files: int) -> str:
    """
    Trova in modo robusto il formato MIME corretto per FileContents.

    Perché serve:
    - Outlook/Qt non sempre espongono esattamente la stringa attesa
      da hasFormat('...;index=N')
    - il drag diretto da Outlook spesso fallisce proprio qui

    Strategia:
    1. cerca un FileContents con index corretto
    2. se c'è un solo file totale, accetta anche un FileContents generico
    3. fallback ultra-difensivo: se esiste un solo FileContents totale e sto
       cercando il primo elemento, usa quello
    """
    formats = _mime_formats_list(mime_data)
    if not formats:
        return ""

    index_token = f"index={index}"

    # Match preferito: formato indicizzato corretto
    for fmt in formats:
        if "FileContents" in fmt and index_token in fmt:
            return fmt

    # Alcune sorgenti espongono un solo FileContents non indicizzato
    generic_non_indexed = [
        fmt
        for fmt in formats
        if "FileContents" in fmt and "index=" not in fmt
    ]
    if len(generic_non_indexed) == 1 and total_files == 1:
        return generic_non_indexed[0]

    # Fallback conservativo finale
    generic_all = [fmt for fmt in formats if "FileContents" in fmt]
    if index == 0 and len(generic_all) == 1:
        return generic_all[0]

    logging.warning(
        "Formato FileContents non trovato | index=%s | total_files=%s | formats=%s",
        index,
        total_files,
        formats,
    )
    return ""


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
        has_contents = any("FileContents" in fmt for fmt in formats)
        return has_descriptor and has_contents
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


def _normalize_sender_for_filename(sender: str) -> str:
    value = str(sender or "").strip()

    if "<" in value:
        value = value.split("<", 1)[0].strip()

    return value or "Mittente sconosciuto"


def _normalize_subject_for_filename(subject: str, fallback_name: str) -> str:
    value = str(subject or "").strip()
    if value:
        return value

    stem = Path(str(fallback_name or "").strip()).stem
    return stem or "Messaggio"


def _normalize_received_date_for_filename(received_at: str) -> str:
    text = str(received_at or "").strip()

    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10]

    return datetime.now().strftime("%Y-%m-%d")


def build_outlook_msg_display_name(
    *,
    original_name: str,
    subject: str = "",
    sender: str = "",
    received_at: str = "",
) -> str:
    """Costruisce il nome file finale richiesto:
    YYYY-MM-DD - Mittente - Oggetto.msg
    """
    date_part = _normalize_received_date_for_filename(received_at)
    sender_part = _normalize_sender_for_filename(sender)
    subject_part = _normalize_subject_for_filename(subject, original_name)

    base_name = f"{date_part} - {sender_part} - {subject_part}"
    safe_name = safe_filename(base_name, fallback=Path(str(original_name or "mail.msg")).stem or "mail")

    if not safe_name.lower().endswith(".msg"):
        safe_name += ".msg"

    return safe_name


def read_outlook_msg_metadata(msg_path: str) -> Dict[str, str]:
    """Legge metadati base da un .msg usando Outlook COM.

    Fallback totalmente difensivo:
    - se Outlook/pywin32 non sono disponibili
    - se il file è anomalo
    - se COM fallisce

    ritorna comunque un dict valido.
    """
    result = {
        "subject": "",
        "sender": "",
        "received_at": "",
    }

    pythoncom_module = None

    try:
        import pythoncom  # type: ignore
        import win32com.client  # type: ignore

        pythoncom_module = pythoncom
        pythoncom_module.CoInitialize()

        outlook = win32com.client.Dispatch("Outlook.Application")
        namespace = outlook.Session
        item = namespace.OpenSharedItem(str(msg_path))

        try:
            result["subject"] = str(getattr(item, "Subject", "") or "").strip()

            sender_name = str(getattr(item, "SenderName", "") or "").strip()
            sender_email = str(getattr(item, "SenderEmailAddress", "") or "").strip()
            result["sender"] = sender_name or sender_email

            received_time = getattr(item, "ReceivedTime", None)
            if received_time:
                if hasattr(received_time, "strftime"):
                    result["received_at"] = received_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    result["received_at"] = str(received_time).strip()

        finally:
            try:
                # Difensivo: non tutti gli item esposti da COM reagiscono uguale.
                item.Close(0)  # olSave = 0 / chiusura senza salvataggio
            except Exception:
                pass

    except Exception:
        logging.warning("Impossibile leggere metadati Outlook da .msg: %s", msg_path, exc_info=True)

    finally:
        if pythoncom_module is not None:
            try:
                pythoncom_module.CoUninitialize()
            except Exception:
                pass

    return result


def _write_outlook_temp_msg(content_bytes: bytes, original_name: str) -> Path:
    """Materializza localmente il payload Outlook in un file .msg temporaneo."""
    safe_name = safe_filename(Path(str(original_name or "mail.msg")).name, fallback=f"mail_{uuid.uuid4().hex}")

    if not safe_name.lower().endswith(".msg"):
        safe_name += ".msg"

    target = _drop_temp_dir() / f"{uuid.uuid4().hex}_{safe_name}"
    target.write_bytes(content_bytes)
    return target


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

    Nota importante:
    - questa funzione NON deve attivarsi quando il drag contiene già file locali,
      altrimenti un .msg trascinato dal desktop può venire duplicato.
    """
    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    if mime_data is None:
        return items, errors

    # Guard rail fondamentale:
    # se Qt ci sta già dando file locali veri, allora non siamo nel caso
    # "drag diretto Outlook" che ci interessa qui.
    if mime_data.hasUrls():
        try:
            if any(url.isLocalFile() for url in mime_data.urls()):
                return items, errors
        except Exception:
            logging.exception("Errore controllo URL locali nel drop Outlook")
            return items, errors

    descriptor_format = None
    descriptor_is_wide = False

    if mime_data.hasFormat(WINDOWS_MIME_FILEGROUPDESCRIPTOR_W):
        descriptor_format = WINDOWS_MIME_FILEGROUPDESCRIPTOR_W
        descriptor_is_wide = True
    elif mime_data.hasFormat(WINDOWS_MIME_FILEGROUPDESCRIPTOR_A):
        descriptor_format = WINDOWS_MIME_FILEGROUPDESCRIPTOR_A
        descriptor_is_wide = False

    if not descriptor_format:
        return items, errors

    descriptor_bytes = bytes(mime_data.data(descriptor_format))
    file_names = _parse_file_group_descriptor_names(
        descriptor_bytes,
        wide=descriptor_is_wide,
    )

    if not file_names:
        errors.append("Drop Outlook rilevato ma impossibile leggere il descrittore dei file.")
        return items, errors

    total_files = len(file_names)

    for index, original_name in enumerate(file_names):
        clean_original_name = str(original_name or "").strip() or f"mail_{index + 1}.msg"

        # Vincolo funzionale richiesto:
        # da Outlook accettiamo solo mail complete .msg
        if not clean_original_name.lower().endswith(".msg"):
            errors.append(
                f"Ignorato '{clean_original_name}': da Outlook sono accettate solo mail complete in formato .msg."
            )
            continue

        content_format = _find_filecontents_format(
            mime_data=mime_data,
            index=index,
            total_files=total_files,
        )

        if not content_format:
            errors.append(f"Impossibile leggere il contenuto Outlook per '{clean_original_name}'.")
            continue

        try:
            content_bytes = bytes(mime_data.data(content_format))
        except Exception:
            logging.exception(
                "Errore lettura mime_data.data() per Outlook | file=%s | format=%s",
                clean_original_name,
                content_format,
            )
            content_bytes = b""

        if not content_bytes:
            errors.append(f"Impossibile leggere il contenuto Outlook per '{clean_original_name}'.")
            continue

        try:
            temp_msg_path = _write_outlook_temp_msg(content_bytes, clean_original_name)
            metadata = read_outlook_msg_metadata(str(temp_msg_path))

            final_display_name = build_outlook_msg_display_name(
                original_name=clean_original_name,
                subject=str(metadata.get("subject") or ""),
                sender=str(metadata.get("sender") or ""),
                received_at=str(metadata.get("received_at") or ""),
            )

            items.append(
                {
                    "id": None,
                    "pending": True,
                    "attachment_kind": "outlook_msg",
                    "source_path": str(temp_msg_path),
                    "display_name": final_display_name,
                    "temp_file": True,
                    "subject": str(metadata.get("subject") or ""),
                    "sender": str(metadata.get("sender") or ""),
                    "received_at": str(metadata.get("received_at") or ""),
                    "meta_json": {
                        "source": "outlook_drop",
                        "original_name": clean_original_name,
                        "temp_msg_path": str(temp_msg_path),
                        "mime_content_format": content_format,
                    },
                }
            )
        except Exception:
            logging.exception("Errore estrazione mail Outlook dal drag&drop")
            errors.append(f"Errore durante l'estrazione della mail '{clean_original_name}'.")

    return items, errors
    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    if mime_data is None:
        return items, errors

    descriptor_format = None
    descriptor_is_wide = False

    if mime_data.hasFormat(WINDOWS_MIME_FILEGROUPDESCRIPTOR_W):
        descriptor_format = WINDOWS_MIME_FILEGROUPDESCRIPTOR_W
        descriptor_is_wide = True
    elif mime_data.hasFormat(WINDOWS_MIME_FILEGROUPDESCRIPTOR_A):
        descriptor_format = WINDOWS_MIME_FILEGROUPDESCRIPTOR_A
        descriptor_is_wide = False

    if not descriptor_format:
        return items, errors

    descriptor_bytes = bytes(mime_data.data(descriptor_format))
    file_names = _parse_file_group_descriptor_names(descriptor_bytes, wide=descriptor_is_wide)

    if not file_names:
        errors.append("Drop Outlook rilevato ma impossibile leggere il descrittore dei file.")
        return items, errors

    for index, original_name in enumerate(file_names):
        clean_original_name = str(original_name or "").strip() or f"mail_{index + 1}.msg"

        # Vincolo richiesto: da Outlook accettiamo solo mail complete .msg
        if not clean_original_name.lower().endswith(".msg"):
            errors.append(
                f"Ignorato '{clean_original_name}': da Outlook sono accettate solo mail complete in formato .msg."
            )
            continue

        indexed_contents_format = f"{WINDOWS_MIME_FILECONTENTS_INDEX_PREFIX}{index}"
        if mime_data.hasFormat(indexed_contents_format):
            content_bytes = bytes(mime_data.data(indexed_contents_format))
        elif index == 0 and mime_data.hasFormat(WINDOWS_MIME_FILECONTENTS):
            content_bytes = bytes(mime_data.data(WINDOWS_MIME_FILECONTENTS))
        else:
            content_bytes = b""

        if not content_bytes:
            errors.append(f"Impossibile leggere il contenuto Outlook per '{clean_original_name}'.")
            continue

        try:
            temp_msg_path = _write_outlook_temp_msg(content_bytes, clean_original_name)
            metadata = read_outlook_msg_metadata(str(temp_msg_path))

            final_display_name = build_outlook_msg_display_name(
                original_name=clean_original_name,
                subject=str(metadata.get("subject") or ""),
                sender=str(metadata.get("sender") or ""),
                received_at=str(metadata.get("received_at") or ""),
            )

            items.append(
                {
                    "id": None,
                    "pending": True,
                    "attachment_kind": "outlook_msg",
                    "source_path": str(temp_msg_path),
                    "display_name": final_display_name,
                    "temp_file": True,
                    "subject": str(metadata.get("subject") or ""),
                    "sender": str(metadata.get("sender") or ""),
                    "received_at": str(metadata.get("received_at") or ""),
                    "meta_json": {
                        "source": "outlook_drop",
                        "original_name": clean_original_name,
                        "temp_msg_path": str(temp_msg_path),
                    },
                }
            )
        except Exception:
            logging.exception("Errore estrazione mail Outlook dal drag&drop")
            errors.append(f"Errore durante l'estrazione della mail '{clean_original_name}'.")

    return items, errors


def extract_attachments_from_mime_data(mime_data) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Estrazione completa e normalizzazione degli allegati trascinati.

    Regola di precedenza:
    - se esistono file locali veri, uso SOLO quelli
    - il parser Outlook parte solo quando non ci sono URL locali

    Questo evita il bug dei duplicati sui file .msg trascinati dal desktop.
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