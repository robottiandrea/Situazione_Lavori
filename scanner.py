# -*- coding: utf-8 -*-
"""Motore di scansione filesystem per le colonne automatiche."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from utils import (
    ACC_REGEX,
    COS_REGEX,
    DATE_FOLDER_REGEX,
    DL_OFFICE_ALIASES,
    PRG_REGEX,
    PROJECT_LINKS_ALIASES,
    REV_REGEX,
    extract_first_child_from_target,
    extract_first_project_child_from_target,
    find_child_folder_by_alias,
    load_project_base_paths,
    load_psc_base_paths,
    resolve_windows_shortcut_target,
)



class FileSystemScanner:
    """Esegue i controlli automatici del lavoro."""

    def find_project_root_from_dl_link(self, dl_base_path: str) -> Dict[str, Any]:
        """
        Cerca collegamenti .lnk nella struttura DL e prova a ricavare
        la cartella radice del progetto PRG.

        Strategia robusta:
        1. parte dal dl_base_path
        2. cerca una sottocartella "equivalente" a '01.DL ufficio'
        3. dentro questa cerca una sottocartella "equivalente" a '01.Progetto'
        4. analizza TUTTI i file .lnk trovati
        5. risolve il target reale del collegamento
        6. ricava la PRIMA sottocartella sotto uno dei base path 'Progetti'
        presenti in percorsi.json
        7. restituisce il primo candidato valido

        Stati possibili:
        - NOT_AVAILABLE
        - DL_BASE_MISSING
        - DL_OFFICE_FOLDER_MISSING
        - PROJECT_LINKS_FOLDER_MISSING
        - LINK_NOT_FOUND
        - TARGET_NOT_RESOLVED
        - OUTSIDE_PROJECT_BASES
        - OK
        """
        result = {
            "lookup_folder": "",
            "link_path": "",
            "target_path": "",
            "project_root": "",
            "status": "NOT_AVAILABLE",
        }

        dl_base_path = str(dl_base_path or "").strip()
        if not dl_base_path:
            return result

        dl_root = Path(dl_base_path)
        if not dl_root.is_dir():
            result["status"] = "DL_BASE_MISSING"
            return result

        dl_office_folder = find_child_folder_by_alias(dl_root, DL_OFFICE_ALIASES)
        if dl_office_folder is None:
            result["status"] = "DL_OFFICE_FOLDER_MISSING"
            return result

        project_links_folder = find_child_folder_by_alias(dl_office_folder, PROJECT_LINKS_ALIASES)
        if project_links_folder is None:
            result["status"] = "PROJECT_LINKS_FOLDER_MISSING"
            return result

        result["lookup_folder"] = str(project_links_folder)

        try:
            links = sorted(
                [
                    p for p in project_links_folder.iterdir()
                    if p.is_file() and p.suffix.lower() == ".lnk"
                ],
                key=lambda p: p.name.lower(),
            )
        except Exception:
            logging.exception(
                "Errore lettura cartella collegamenti progetto: %s",
                project_links_folder,
            )
            result["status"] = "ERROR"
            return result

        if not links:
            result["status"] = "LINK_NOT_FOUND"
            return result

        base_paths = load_project_base_paths()
        if not base_paths:
            logging.warning("Nessun base path 'Progetti' disponibile da percorsi.json")
            result["status"] = "OUTSIDE_PROJECT_BASES"
            return result

        resolved_any_target = False

        for link_path in links:
            target_path = resolve_windows_shortcut_target(str(link_path))
            if not target_path:
                logging.debug("Link .lnk non risolto: %s", link_path)
                continue

            resolved_any_target = True
            project_root = extract_first_project_child_from_target(target_path, base_paths)

            logging.info(
                "Analisi link progetto DL | link=%s | target=%s | project_root=%s",
                link_path,
                target_path,
                project_root,
            )

            if not project_root:
                # Link valido ma fuori dai base path 'Progetti'
                continue

            result["link_path"] = str(link_path)
            result["target_path"] = target_path
            result["project_root"] = project_root
            result["status"] = "OK"
            return result

        result["status"] = "OUTSIDE_PROJECT_BASES" if resolved_any_target else "TARGET_NOT_RESOLVED"
        return result

    def find_psc_root_from_dl_link(self, dl_base_path: str) -> Dict[str, Any]:
        """
        Cerca collegamenti .lnk nella struttura DL e prova a ricavare
        la cartella radice PSC.

        Strategia:
        1. parte dal dl_base_path
        2. cerca '01.DL ufficio'
        3. dentro questa cerca '01.Progetto'
        4. analizza tutti i file .lnk trovati
        5. risolve il target reale del collegamento
        6. ricava la prima sottocartella sotto uno dei base path 'PSC'
        presenti in percorsi.json
        7. restituisce il primo candidato valido
        """
        result = {
            "lookup_folder": "",
            "link_path": "",
            "target_path": "",
            "psc_root": "",
            "status": "NOT_AVAILABLE",
        }

        dl_base_path = str(dl_base_path or "").strip()
        if not dl_base_path:
            return result

        dl_root = Path(dl_base_path)
        if not dl_root.is_dir():
            result["status"] = "DL_BASE_MISSING"
            return result

        dl_office_folder = find_child_folder_by_alias(dl_root, DL_OFFICE_ALIASES)
        if dl_office_folder is None:
            result["status"] = "DL_OFFICE_FOLDER_MISSING"
            return result

        project_links_folder = find_child_folder_by_alias(dl_office_folder, PROJECT_LINKS_ALIASES)
        if project_links_folder is None:
            result["status"] = "PROJECT_LINKS_FOLDER_MISSING"
            return result

        result["lookup_folder"] = str(project_links_folder)

        try:
            links = sorted(
                [
                    p for p in project_links_folder.iterdir()
                    if p.is_file() and p.suffix.lower() == ".lnk"
                ],
                key=lambda p: p.name.lower(),
            )
        except Exception:
            logging.exception(
                "Errore lettura cartella collegamenti PSC: %s",
                project_links_folder,
            )
            result["status"] = "ERROR"
            return result

        if not links:
            result["status"] = "LINK_NOT_FOUND"
            return result

        base_paths = load_psc_base_paths()
        if not base_paths:
            logging.warning("Nessun base path 'PSC' disponibile da percorsi.json")
            result["status"] = "OUTSIDE_PSC_BASES"
            return result

        resolved_any_target = False

        for link_path in links:
            target_path = resolve_windows_shortcut_target(str(link_path))
            if not target_path:
                logging.debug("Link .lnk PSC non risolto: %s", link_path)
                continue

            resolved_any_target = True
            psc_root = extract_first_child_from_target(target_path, base_paths)

            logging.info(
                "Analisi link PSC DL | link=%s | target=%s | psc_root=%s",
                link_path,
                target_path,
                psc_root,
            )

            if not psc_root:
                continue

            result["link_path"] = str(link_path)
            result["target_path"] = target_path
            result["psc_root"] = psc_root
            result["status"] = "OK"
            return result

        result["status"] = "OUTSIDE_PSC_BASES" if resolved_any_target else "TARGET_NOT_RESOLVED"
        return result
        
    def scan_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        logging.info("Scansione job id=%s", job.get("id"))
        return {
            "project_rilievo": self.scan_project_rilievo(job.get("project_base_path", "")),
            "project_enti": self.scan_project_enti(job.get("project_base_path", "")),
            "project_revision": self.scan_project_revision(job.get("project_base_path", "")),
            "permessi_revision": self.scan_permessi_revision(job.get("project_base_path", "")),
            "project_tracciamento": self.scan_project_tracciamento(job.get("project_base_path", "")),
            "cartesio_prg": self.scan_cartesio_prg(job.get("project_base_path", "")),
            "rilievi_dl": self.scan_rilievi_dl(job.get("dl_base_path", "")),
            "cartesio_cos": self.scan_cartesio_cos(job.get("dl_base_path", "")),
            # ACC è condiviso tra Progetto e DL: estrazione unica.
            "cartesio_acc": self.scan_cartesio_acc(
                project_base_path=job.get("project_base_path", ""),
                dl_base_path=job.get("dl_base_path", ""),
            ),
        }

    def _iter_all_files(self, folder: Path) -> Iterable[Path]:
        try:
            for p in folder.rglob("*"):
                if p.is_file():
                    yield p
        except Exception:
            logging.exception("Errore lettura ricorsiva cartella: %s", folder)

    def scan_project_rilievo(self, project_base_path: str) -> Dict[str, Any]:
        rilievi = Path(project_base_path) / "Progettazione" / "Rilievi"
        result = {"status": "❌", "path": str(rilievi), "has_2d": False, "has_3d": False}
        if not rilievi.is_dir():
            return result

        files = list(self._iter_all_files(rilievi))
        if not files:
            return result

        has_2d = any(f.name.lower().endswith("2d.dwg") for f in files)
        has_3d = any(f.name.lower().endswith("3d.dwg") for f in files)
        result["has_2d"] = has_2d
        result["has_3d"] = has_3d
        result["status"] = "✅" if has_2d and has_3d else "🔄"
        return result

    def scan_project_enti(self, project_base_path: str) -> Dict[str, Any]:
        folder = Path(project_base_path) / "Progettazione" / "Per Enti"
        return {"status": "✅" if folder.is_dir() else "❌", "path": str(folder)}

    def scan_project_revision(self, project_base_path: str) -> Dict[str, Any]:
        folder = Path(project_base_path) / "Progettazione" / "Progetto"
        result = {"display": "❌", "path": str(folder), "max_rev": None}
        if not folder.is_dir():
            return result

        revisions: List[int] = []
        try:
            for child in folder.iterdir():
                if child.is_dir():
                    match = REV_REGEX.search(child.name)
                    if match:
                        revisions.append(int(match.group(1)))
        except Exception:
            logging.exception("Errore scansione revisione progetto")
            return result

        if revisions:
            result["max_rev"] = max(revisions)
            result["display"] = str(result["max_rev"])
            return result

        dwg_found = any(p.suffix.lower() == ".dwg" for p in folder.glob("**/*") if p.is_file())
        if dwg_found:
            result["display"] = "🔄"
        return result

    def scan_permessi_revision(self, project_base_path: str) -> Dict[str, Any]:
        folder = Path(project_base_path) / "Permessi_Pubblici"
        result = {"display": "❌", "path": str(folder), "max_rev": None}
        if not folder.is_dir():
            return result

        revisions: List[int] = []

        try:
            for file in folder.iterdir():
                if not file.is_file():
                    continue

                match = REV_REGEX.search(file.stem)
                if match:
                    rev = int(match.group(1))
                    revisions.append(rev)
                    logging.info("Permessi revision: trovato Rev%s in %s", rev, file.name)
        except Exception:
            logging.exception("Errore scansione revisione permessi")
            return result

        if revisions:
            result["max_rev"] = max(revisions)
            result["display"] = str(result["max_rev"])

        return result

    def scan_project_tracciamento(self, project_base_path: str) -> Dict[str, Any]:
        folder = Path(project_base_path) / "Progettazione" / "_Tracciamento"
        return {"status": "✅" if folder.is_dir() else "❌", "path": str(folder)}

    def scan_cartesio_prg(self, project_base_path: str) -> Dict[str, Any]:
        folder = Path(project_base_path) / "Progettazione" / "cartesio"
        result = {"display": "❌", "path": str(folder), "code": "", "codes_found": []}
        if not folder.is_dir():
            return result

        codes: List[str] = []
        for file in self._iter_all_files(folder):
            for match in PRG_REGEX.findall(file.name):
                code = match.upper()
                if code not in codes:
                    codes.append(code)

        result["codes_found"] = codes
        if codes:
            result["code"] = codes[0]
            result["display"] = codes[0]
        else:
            result["display"] = "🔄"
        return result

    def scan_cartesio_acc(self, project_base_path: str, dl_base_path: str) -> Dict[str, Any]:
        """
        Estrae un codice ACC (ACCXXXXXX) valido per entrambi i comparti.
        """
        result = {"display": "❌", "path": "", "code": "", "codes_found": []}

        codes: List[str] = []
        chosen_path: str = ""

        # 1) Progetto: .../Progettazione/cartesio
        prg_folder = Path(project_base_path) / "Progettazione" / "cartesio"
        if prg_folder.is_dir():
            chosen_path = str(prg_folder)
            for file in self._iter_all_files(prg_folder):
                for match in ACC_REGEX.findall(file.name):
                    code = match.upper()
                    if code not in codes:
                        codes.append(code)

        # 2) DL: .../Come costruito/cartesio... (cartesio è trovato tramite ricerca folder)
        cos_folder = self._find_first_folder_contains(dl_base_path, "cartesio")
        if cos_folder:
            # Se non c'è già da Progetto, usa il path DL come riferimento.
            if not chosen_path:
                chosen_path = str(cos_folder)
            for file in self._iter_all_files(cos_folder):
                for match in ACC_REGEX.findall(file.name):
                    code = match.upper()
                    if code not in codes:
                        codes.append(code)

        result["path"] = chosen_path
        result["codes_found"] = codes
        if codes:
            result["code"] = codes[0]
            result["display"] = codes[0]
        else:
            result["display"] = "🔄"

        return result

    def _find_first_folder_contains(self, base_path: str, text: str) -> Optional[Path]:
        base = Path(base_path)
        if not base.is_dir():
            return None
        try:
            for child in base.iterdir():
                if child.is_dir() and text.lower() in child.name.lower():
                    return child
        except Exception:
            logging.exception("Errore ricerca cartella contenente %s", text)
        return None

    def scan_rilievi_dl(self, dl_base_path: str) -> Dict[str, Any]:
        result = {"display": "❌", "path": "", "latest_date": "", "date_folders": []}
        come_costruito = self._find_first_folder_contains(dl_base_path, "Come costruito")
        if not come_costruito:
            return result

        rilievi = come_costruito / "Rilievi"
        result["path"] = str(rilievi)
        if not rilievi.is_dir():
            return result

        date_candidates: List[str] = []
        for child in rilievi.iterdir():
            if child.is_dir():
                match = DATE_FOLDER_REGEX.match(child.name)
                if match:
                    date_candidates.append(match.group(1))

        result["date_folders"] = sorted(date_candidates)
        if date_candidates:
            latest = max(date_candidates)
            result["latest_date"] = latest
            result["display"] = latest
        else:
            result["display"] = "🔄"
        return result

    def scan_cartesio_cos(self, dl_base_path: str) -> Dict[str, Any]:
        result = {"display": "❌", "path": "", "code": "", "codes_found": []}
        folder = self._find_first_folder_contains(dl_base_path, "cartesio")
        if not folder:
            return result

        result["path"] = str(folder)
        codes: List[str] = []
        for file in self._iter_all_files(folder):
            for match in COS_REGEX.findall(file.name):
                code = match.upper()
                if code not in codes:
                    codes.append(code)

        result["codes_found"] = codes
        if codes:
            result["code"] = codes[0]
            result["display"] = codes[0]
        else:
            result["display"] = "🔄"
        return result
