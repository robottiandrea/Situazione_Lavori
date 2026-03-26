# -*- coding: utf-8 -*-
"""Motore di scansione filesystem per le colonne automatiche."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from utils import ACC_REGEX, COS_REGEX, DATE_FOLDER_REGEX, PRG_REGEX, REV_REGEX


class FileSystemScanner:
    """Esegue i controlli automatici del lavoro."""

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
