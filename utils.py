# -*- coding: utf-8 -*-
"""Utility condivise per Situazione Lavori."""
from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from pathlib import Path

APP_NAME = "Situazione Lavori"

# Cartella condivisa dove sta il DB unico usato da tutti
APP_DIR = Path(r"S:\SnamReteGas_Applicativi2009\ARtool\ARTool\Utility\Situazione Lavori")
APP_DIR.mkdir(parents=True, exist_ok=True)

# Log locale per ogni utente, così evitiamo collisioni inutili sul file di log
LOCAL_APP_DIR = Path.home() / "SituazioneLavori"
LOCAL_APP_DIR.mkdir(parents=True, exist_ok=True)

DB_FILE = APP_DIR / "situazione_lavori.sqlite"
LOG_FILE = LOCAL_APP_DIR / "situazione_lavori.log"

REV_REGEX = re.compile(r"(?i)(?:^|[^A-Z0-9])rev(?:isione)?\s*[-_. ]?\s*(\d+)(?=$|[^A-Z0-9])")
PRG_REGEX = re.compile(r"(?i)\b(PRG\d{4,})\b")
COS_REGEX = re.compile(r"(?i)\b(COS\d{4,})\b")
ACC_REGEX = re.compile(r"(?i)\b(ACC\d{6})\b")
DATE_FOLDER_REGEX = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:\b|\s|_|-).*")
YEAR_REGEX = re.compile(r"(20\d{2})")

STATUS_COLORS = {
    "IN CORSO": "#3daee9",
    "INCIDENT": "#d9534f",
    "VERIFICA M.E.": "#f0ad4e",
    "VERIFICA TOTALE": "#2e8b57",
    "APPROVATO": "#198754",
    "INSERIMENTO": "#5bc0de",
    "DEFINITIVO": "#198754",
    "NON IMPOSTATO": "#000000",
}

RILIEVI_DL_STATES = ["NON IMPOSTATO", "IN PROGRESS", "INSERIMENTO", "DEFINITIVO"]
CARTESIO_PRG_STATES = ["NON IMPOSTATO", "IN CORSO", "INCIDENT", "VERIFICA TOTALE", "APPROVATO"]
CARTESIO_COS_STATES = [
    "NON IMPOSTATO",
    "IN CORSO",
    "INCIDENT",
    "VERIFICA M.E.",
    "VERIFICA TOTALE",
    "APPROVATO",
]

PERMIT_DEFAULT_ITEMS = [
    "SCIA",
    "Paesaggistica",
    "Strutturale",
    "Occupazione suolo",
    "Nulla osta comune",
]


AREA_PATTERNS = [
    ("MILANO", "MI"),
    ("TORINO", "TO"),
    ("MI", "MI"),
    ("TO", "TO"),
]


def setup_logging() -> None:
    """Configura logging file + console una sola volta."""
    if logging.getLogger().handlers:
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Avvio applicazione %s", APP_NAME)
    logging.info("Log file: %s", LOG_FILE)


def debug_log(message: str) -> None:
    """Piccolo wrapper utile durante debug rapido."""
    logging.info(message)


def norm_path(value: str) -> str:
    """Normalizza un path utente senza toccare la maiuscolizzazione."""
    if not value:
        return ""
    return str(Path(value.strip().strip('"')))

def path_compare_key(value: str) -> str:
    """
    Restituisce una chiave normalizzata per confrontare path in modo più robusto.
    Utile per evitare duplicati dovuti a slash, maiuscole/minuscole o separatori finali.
    """
    path = norm_path(value)
    if not path:
        return ""
    return os.path.normcase(os.path.normpath(path))
    
def exists_dir(path: str) -> bool:
    return bool(path) and Path(path).is_dir()


def open_in_explorer(path: str) -> tuple[bool, str]:
    """Apre un file/cartella in Esplora Risorse. Ritorna esito e messaggio."""
    try:
        target = Path(path)
        if not target.exists():
            return False, f"Percorso non trovato: {path}"

        if os.name == "nt":
            os.startfile(str(target))  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(target)])
        else:
            subprocess.Popen(["xdg-open", str(target)])
        return True, "OK"
    except Exception as exc:  # pragma: no cover - difensivo
        logging.exception("Errore apertura percorso: %s", path)
        return False, str(exc)


def first_existing_dir(paths: Iterable[str]) -> Optional[str]:
    for path in paths:
        if exists_dir(path):
            return path
    return None


def color_for_status(status: str) -> Optional[str]:
    return STATUS_COLORS.get((status or "").upper())


def parse_date_text(text: str) -> Optional[datetime]:
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except Exception:
        return None


def folder_name_from_path(path: str) -> str:
    """Restituisce il nome cartella finale del path, se disponibile."""
    path = norm_path(path)
    if not path:
        return ""
    try:
        return Path(path).name
    except Exception:
        return ""


def infer_year_from_path(path: str) -> str:
    """
    Estrae l'ultimo anno a 4 cifre dal path.
    Esempi:
    - Progettazioni_Milano_2026 -> 26
    - ...\\2025\\DIREZIONE LAVORI -> 25
    - ..._TORINO_2024 -> 24
    """
    path = norm_path(path)
    if not path:
        return ""

    matches = YEAR_REGEX.findall(path)
    if not matches:
        return ""

    return matches[-1][-2:]


def infer_area_code_from_path(path: str) -> str:
    """Prova a ricavare il distretto dal path: Milano->MI, Torino->TO."""
    path = norm_path(path)
    if not path:
        return ""

    parts = [part.upper() for part in Path(path).parts]
    joined = " | ".join(parts)

    # Prima i match forti su parole complete o stringhe lunghe.
    if "MILANO" in joined:
        return "MI"
    if "TORINO" in joined:
        return "TO"

    # Fallback più debole: token brevi isolati.
    token_regex = re.compile(r"\b(MI|TO)\b", re.IGNORECASE)
    match = token_regex.search(joined)
    if match:
        return match.group(1).upper()
    return ""


def infer_project_distretto_anno(path: str) -> str:
    """Esempio: ...Progettazioni_Milano_2025... -> MI25"""
    area = infer_area_code_from_path(path)
    yy = infer_year_from_path(path)
    if area and yy:
        return f"{area}{yy}"
    if area:
        return area
    return ""


def infer_dl_distretto_anno(path: str) -> str:
    r"""Esempio: ...MILANO\2025\DIREZIONE LAVORI... -> DIRLAV_MI25"""
    base = infer_project_distretto_anno(path)
    if base:
        return f"DIRLAV_{base}"
    return "DIRLAV" if path else ""
