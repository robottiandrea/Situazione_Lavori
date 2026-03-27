# -*- coding: utf-8 -*-
"""Utility condivise per Situazione Lavori."""
from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from typing import Iterable, Optional

from pathlib import Path, PureWindowsPath
import json


PERCORSI_JSON_FILE = Path(
    r"S:\SnamReteGas_Applicativi2009\ARtool\ARTool\JSON\percorsi.json"
)


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

SCAN_OVERRIDEABLE_FIELDS = {
    "project_rilievo",
    "project_enti",
    "project_revision",
    "permessi_revision",
    "project_tracciamento",
    "cartesio_prg_display",
    "rilievi_dl_display",
    "cartesio_cos_display",
}

AREA_PATTERNS = [
    ("MILANO", "MI"),
    ("TORINO", "TO"),
    ("MI", "MI"),
    ("TO", "TO"),
]

# -------------------------------------------------------------------------
# Alias cartelle "semantiche" per evitare dipendenza da formattazioni rigide
# -------------------------------------------------------------------------

FOLDER_LABEL_SEPARATORS_RE = re.compile(r"[\s._-]+", re.UNICODE)

DL_OFFICE_ALIASES = [
    "01.DL ufficio",
    "01. DL ufficio",
    "01 DL ufficio",
]

PROJECT_LINKS_ALIASES = [
    "01.Progetto",
    "01. Progetto",
    "01 Progetto",
]

def normalize_folder_label(name: str) -> str:
    """
    Normalizza un nome cartella per confronti tolleranti.

    Obiettivo:
    - rendere equivalenti varianti come:
      '01.DL ufficio'
      '01. DL ufficio'
      '01_DL ufficio'
      '01-DL ufficio'

    Strategia:
    - trim
    - lowercase
    - sostituzione di separatori multipli con singolo spazio
    """
    if not name:
        return ""

    value = str(name).strip().lower()
    value = FOLDER_LABEL_SEPARATORS_RE.sub(" ", value)
    return value.strip()


def folder_name_matches(name: str, aliases: Iterable[str]) -> bool:
    """
    Confronta il nome di una cartella con una lista di alias in modo tollerante.
    """
    normalized_name = normalize_folder_label(name)
    if not normalized_name:
        return False

    for alias in aliases:
        if normalized_name == normalize_folder_label(alias):
            return True
    return False


def find_child_folder_by_alias(base_path: Path, aliases: Iterable[str]) -> Optional[Path]:
    """
    Cerca tra le sottocartelle immediate di 'base_path' la prima che corrisponde
    semanticamente a uno degli alias forniti.

    Note:
    - non fa ricerca ricorsiva
    - in caso di più match, usa il primo in ordine alfabetico e logga warning
    """
    if not isinstance(base_path, Path):
        base_path = Path(base_path)

    if not base_path.is_dir():
        return None

    matches: list[Path] = []

    try:
        for child in base_path.iterdir():
            if child.is_dir() and folder_name_matches(child.name, aliases):
                matches.append(child)
    except Exception:
        logging.exception("Errore ricerca sottocartella alias in %s", base_path)
        return None

    if not matches:
        return None

    matches.sort(key=lambda p: p.name.lower())

    if len(matches) > 1:
        logging.warning(
            "Trovate più cartelle equivalenti sotto %s per alias %s: %s. Uso la prima.",
            base_path,
            list(aliases),
            [m.name for m in matches],
        )

    return matches[0]

def load_base_paths(root_key: str, json_path: Path | str = PERCORSI_JSON_FILE) -> list[str]:
    """
    Legge il file percorsi.json e restituisce l'elenco dei base path
    della chiave richiesta (es. 'Progetti', 'PSC').

    Note:
    - restituisce path normalizzati
    - ordina i path dal più specifico al meno specifico
    - in caso di errore ritorna lista vuota e logga
    """
    file_path = Path(json_path)
    encodings = ("utf-8-sig", "utf-8", "cp1252")
    data = None
    last_error = None

    for enc in encodings:
        try:
            data = json.loads(file_path.read_text(encoding=enc))
            break
        except Exception as exc:
            last_error = exc

    if not isinstance(data, dict):
        logging.exception(
            "Impossibile leggere percorsi.json come dict: %s",
            file_path,
            exc_info=last_error,
        )
        return []

    raw_paths = data.get(root_key) or []
    if not isinstance(raw_paths, list):
        logging.warning("Chiave %r non valida in %s", root_key, file_path)
        return []

    cleaned: list[str] = []
    seen: set[str] = set()

    for value in raw_paths:
        if not isinstance(value, str):
            continue

        normalized = norm_path(value)
        compare_key = path_compare_key(normalized)

        if normalized and compare_key not in seen:
            cleaned.append(normalized)
            seen.add(compare_key)

    cleaned.sort(key=lambda p: len(path_compare_key(p)), reverse=True)
    return cleaned


def load_project_base_paths(json_path: Path | str = PERCORSI_JSON_FILE) -> list[str]:
    """Wrapper compatibile per i base path della chiave 'Progetti'."""
    return load_base_paths("Progetti", json_path)


def load_psc_base_paths(json_path: Path | str = PERCORSI_JSON_FILE) -> list[str]:
    """Wrapper per i base path della chiave 'PSC'."""
    return load_base_paths("PSC", json_path)

def resolve_windows_shortcut_target(link_path: str) -> str:
    """
    Risolve il target reale di un file .lnk Windows.

    Strategia:
    1. prova via win32com se disponibile
    2. fallback via PowerShell/WScript.Shell
    """
    link = Path(norm_path(link_path))

    if not link.is_file() or link.suffix.lower() != ".lnk":
        return ""

    # Tentativo 1: pywin32
    try:
        import win32com.client  # type: ignore

        shell = win32com.client.Dispatch("WScript.Shell")
        shortcut = shell.CreateShortcut(str(link))
        target = norm_path(shortcut.Targetpath or "")
        if target:
            return target
    except Exception:
        logging.debug("Risoluzione .lnk via win32com fallita: %s", link, exc_info=True)

    # Tentativo 2: PowerShell
    try:
        ps_code = (
            "$ws = New-Object -ComObject WScript.Shell; "
            "$sc = $ws.CreateShortcut($args[0]); "
            "if ($sc.TargetPath) { [Console]::Out.Write($sc.TargetPath) }"
        )

        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_code, str(link)],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )

        target = norm_path((completed.stdout or "").strip())
        if target:
            return target

    except Exception:
        logging.debug("Risoluzione .lnk via PowerShell fallita: %s", link, exc_info=True)

    logging.warning("Impossibile risolvere il collegamento .lnk: %s", link)
    return ""

def extract_first_child_from_target(
    target_path: str,
    base_paths: Iterable[str],
) -> str:
    """
    Dato un target qualunque, restituisce la PRIMA sottocartella sotto
    uno dei base path configurati.

    Esempi:
    - Base: S:\\Base\\Anno
      Target: S:\\Base\\Anno\\029_LAVORO\\Sottocartella\\Altro
      -> S:\\Base\\Anno\\029_LAVORO
    """
    target_norm = norm_path(target_path)
    if not target_norm:
        return ""

    target_parts_original = PureWindowsPath(target_norm).parts
    target_parts_lower = tuple(part.lower() for part in target_parts_original)

    for base_path in base_paths:
        base_norm = norm_path(base_path)
        if not base_norm:
            continue

        base_parts_original = PureWindowsPath(base_norm).parts
        base_parts_lower = tuple(part.lower() for part in base_parts_original)

        if len(target_parts_lower) <= len(base_parts_lower):
            continue

        if target_parts_lower[: len(base_parts_lower)] != base_parts_lower:
            continue

        first_child_name = target_parts_original[len(base_parts_original)]
        candidate = str(PureWindowsPath(base_norm) / first_child_name)
        return norm_path(candidate)

    return ""


def extract_first_project_child_from_target(
    target_path: str,
    project_base_paths: Iterable[str],
) -> str:
    """
    Wrapper compatibile con il vecchio nome funzione.
    """
    return extract_first_child_from_target(target_path, project_base_paths)
def extract_first_project_child_from_target(
    target_path: str,
    project_base_paths: Iterable[str],
) -> str:
    """
    Dato un target qualunque, restituisce la PRIMA sottocartella sotto uno dei base path Progetti.

    Esempi:
    - Base: S:\Disegni\Snam\Progettazioni_Milano_2023
      Target: S:\Disegni\Snam\Progettazioni_Milano_2023\029_LENTATE\PROGETTAZIONE
      -> S:\Disegni\Snam\Progettazioni_Milano_2023\029_LENTATE

    - Base: \\srvdati\dati\Disegni\Snam\Progettazioni_Torino_2025
      Target: \\srvdati\dati\Disegni\Snam\Progettazioni_Torino_2025\003_FENIS\Progettazione\Progetto\Rev 1
      -> \\srvdati\dati\Disegni\Snam\Progettazioni_Torino_2025\003_FENIS
    """
    target_norm = norm_path(target_path)
    if not target_norm:
        return ""

    target_parts_original = PureWindowsPath(target_norm).parts
    target_parts_lower = tuple(part.lower() for part in target_parts_original)

    for base_path in project_base_paths:
        base_norm = norm_path(base_path)
        if not base_norm:
            continue

        base_parts_original = PureWindowsPath(base_norm).parts
        base_parts_lower = tuple(part.lower() for part in base_parts_original)

        # Il target deve stare SOTTO il base path, non coincidere col base path stesso.
        if len(target_parts_lower) <= len(base_parts_lower):
            continue

        if target_parts_lower[: len(base_parts_lower)] != base_parts_lower:
            continue

        # Prima cartella figlia immediata sotto il base path.
        first_child_name = target_parts_original[len(base_parts_original)]
        candidate = str(PureWindowsPath(base_norm) / first_child_name)
        return norm_path(candidate)

    return ""

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
