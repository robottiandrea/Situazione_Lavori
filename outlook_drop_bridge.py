# -*- coding: utf-8 -*-
"""Bridge Outlook -> file .msg temporanei per il drag&drop Cartesio.

Obiettivo:
- evitare di dipendere dai byte FileContents esposti da QMimeData/Qt
- salvare la mail trascinata come file .msg temporaneo via Outlook COM
- riutilizzare poi il normale flusso Cartesio:
  temp -> conferma nota -> copia definitiva -> delete temp
"""

from __future__ import annotations

import logging
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from utils import safe_filename


def _drop_temp_dir() -> Path:
    """Cartella temporanea locale per i .msg generati da Outlook."""
    target = Path(tempfile.gettempdir()) / "situazione_lavori_dragdrop"
    target.mkdir(parents=True, exist_ok=True)
    return target


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
    safe_name = safe_filename(
        base_name,
        fallback=Path(str(original_name or "mail.msg")).stem or "mail",
    )

    if not safe_name.lower().endswith(".msg"):
        safe_name += ".msg"

    return safe_name


def _is_supported_mail_item(item: Any) -> bool:
    """Accetta solo mail vere o item compatibili con SaveAs .msg."""
    if item is None:
        return False

    if not hasattr(item, "SaveAs"):
        return False

    try:
        message_class = str(getattr(item, "MessageClass", "") or "").strip().upper()
        if message_class:
            return message_class.startswith("IPM.NOTE")
    except Exception:
        logging.debug("Impossibile leggere MessageClass Outlook", exc_info=True)

    # Fallback permissivo ma prudente
    return hasattr(item, "Subject")


def _extract_sender(item: Any) -> str:
    try:
        sender_name = str(getattr(item, "SenderName", "") or "").strip()
        if sender_name:
            return sender_name
    except Exception:
        pass

    try:
        sender_email = str(getattr(item, "SenderEmailAddress", "") or "").strip()
        if sender_email:
            return sender_email
    except Exception:
        pass

    return ""


def _extract_received_at(item: Any) -> str:
    try:
        received_time = getattr(item, "ReceivedTime", None)
        if received_time:
            if hasattr(received_time, "strftime"):
                return received_time.strftime("%Y-%m-%d %H:%M:%S")
            return str(received_time).strip()
    except Exception:
        logging.debug("Impossibile leggere ReceivedTime Outlook", exc_info=True)

    return ""


def _collect_outlook_candidate_items(outlook_app: Any) -> List[Any]:
    """Recupera gli item Outlook da usare come sorgente del drop.

    Strategia:
    1. preferisce la selezione corrente nell'Explorer
    2. fallback sull'item aperto nell'Inspector
    """
    items: List[Any] = []

    explorer = None
    try:
        explorer = outlook_app.ActiveExplorer()
    except Exception:
        logging.debug("ActiveExplorer non disponibile", exc_info=True)

    if explorer is not None:
        try:
            selection = explorer.Selection
            count = int(getattr(selection, "Count", 0) or 0)

            for index in range(1, count + 1):
                try:
                    item = selection.Item(index)
                except Exception:
                    logging.debug("Impossibile leggere elemento Selection.Item(%s)", index, exc_info=True)
                    continue

                if _is_supported_mail_item(item):
                    items.append(item)
        except Exception:
            logging.debug("Errore lettura selezione Explorer Outlook", exc_info=True)

    if items:
        return items

    inspector = None
    try:
        inspector = outlook_app.ActiveInspector()
    except Exception:
        logging.debug("ActiveInspector non disponibile", exc_info=True)

    if inspector is not None:
        try:
            current_item = inspector.CurrentItem
            if _is_supported_mail_item(current_item):
                items.append(current_item)
        except Exception:
            logging.debug("Errore lettura CurrentItem Inspector Outlook", exc_info=True)

    return items


def _save_item_as_msg(item: Any, dest_path: Path) -> bool:
    """Salva un item Outlook come .msg su disco.

    Prima prova senza tipo esplicito.
    Poi fallback con save type numerici più comuni.
    """
    try:
        item.SaveAs(str(dest_path))
        if dest_path.is_file():
            return True
    except Exception:
        logging.debug("SaveAs Outlook senza type fallito: %s", dest_path, exc_info=True)

    for save_type in (3, 9):
        try:
            item.SaveAs(str(dest_path), save_type)
            if dest_path.is_file():
                return True
        except Exception:
            logging.debug(
                "SaveAs Outlook fallito con save_type=%s: %s",
                save_type,
                dest_path,
                exc_info=True,
            )

    return False


def extract_outlook_pending_items_via_com(
    *,
    expected_names: List[str] | None = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Salva in temp i mail item Outlook correnti e li restituisce come pending attachments.

    Limitazione dichiarata:
    - usa gli elementi selezionati/aperti in Outlook
    - non legge il contenuto direttamente dal QMimeData
    """
    items: List[Dict[str, Any]] = []
    errors: List[str] = []

    pythoncom_module = None

    try:
        import pythoncom  # type: ignore
        import win32com.client  # type: ignore

        pythoncom_module = pythoncom
        pythoncom_module.CoInitialize()

        outlook = win32com.client.Dispatch("Outlook.Application")
        candidate_items = _collect_outlook_candidate_items(outlook)

        if not candidate_items:
            errors.append(
                "Drop Outlook rilevato, ma non trovo una mail selezionata o aperta in Outlook."
            )
            return items, errors

        expected_msg_names = [
            str(name or "").strip()
            for name in (expected_names or [])
            if str(name or "").strip().lower().endswith(".msg")
        ]

        if expected_msg_names and len(expected_msg_names) != len(candidate_items):
            errors.append(
                "Il numero di mail selezionate in Outlook non coincide con il numero di elementi del drop. "
                "Seleziona solo le mail che stai trascinando e riprova."
            )
            return items, errors

        for index, item in enumerate(candidate_items):
            original_name = (
                expected_msg_names[index]
                if index < len(expected_msg_names)
                else f"mail_{index + 1}.msg"
            )

            subject = ""
            try:
                subject = str(getattr(item, "Subject", "") or "").strip()
            except Exception:
                logging.debug("Impossibile leggere Subject Outlook", exc_info=True)

            sender = _extract_sender(item)
            received_at = _extract_received_at(item)

            display_name = build_outlook_msg_display_name(
                original_name=original_name,
                subject=subject,
                sender=sender,
                received_at=received_at,
            )

            safe_name = safe_filename(display_name, fallback=f"mail_{uuid.uuid4().hex}")
            if not safe_name.lower().endswith(".msg"):
                safe_name += ".msg"

            temp_path = _drop_temp_dir() / f"{uuid.uuid4().hex}_{safe_name}"

            if not _save_item_as_msg(item, temp_path):
                errors.append(f"Impossibile salvare in temp la mail Outlook '{display_name}'.")
                continue

            items.append(
                {
                    "id": None,
                    "pending": True,
                    "attachment_kind": "outlook_msg",
                    "source_path": str(temp_path),
                    "display_name": display_name,
                    "temp_file": True,
                    "subject": subject,
                    "sender": sender,
                    "received_at": received_at,
                    "meta_json": {
                        "source": "outlook_com_temp_save",
                        "original_name": original_name,
                        "temp_msg_path": str(temp_path),
                    },
                }
            )

        return items, errors

    except Exception:
        logging.exception("Errore bridge Outlook COM per drag&drop")
        errors.append(
            "Errore durante il recupero della mail da Outlook. "
            "Verifica che Outlook classico sia aperto e che la mail sia selezionata."
        )
        return items, errors

    finally:
        if pythoncom_module is not None:
            try:
                pythoncom_module.CoUninitialize()
            except Exception:
                pass