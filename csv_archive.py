"""
CSV-Archivlader für TPG 366 Tages-CSVs.

Lädt Vortage on-demand aus existierenden Tages-CSV-Dateien und stellt
sie als matplotlib-kompatible Zeitreihen bereit. Cacht geladene Tage
um unnötiges Wiedereinlesen zu vermeiden.

CSV-Format (wie von _start_logging erzeugt):
  Datum_ISO, Zeit_UTC, Zeit_Giessen, MJD, K4_mbar, K4_Status, K5_mbar, ...
  2025-06-15, 14:30:45, 16:30:45, 60480.604688, 7.50E+02, OK, ...
"""

import os
import csv
import math
from datetime import datetime, timedelta, timezone

import matplotlib.dates as mdates


def _ts_print(*args, **kwargs):
    """Zeitstempel-Print (verzögert importiert um Zirkularimport zu vermeiden)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}]", *args, **kwargs)


def load_day_csv(pfad: str, channels: list[int]):
    """Lädt eine einzelne Tages-CSV und gibt (ts_list, {ch: wert_list}) zurück.

    ts_list: matplotlib date numbers (float), sortiert
    wert_list: mbar-Werte (float), parallel zu ts_list; None für fehlende Werte

    Robustes Parsing: überspringe defekte Zeilen, leere Dateien, etc.
    """
    ts_list = []
    werte = {ch: [] for ch in channels}

    if not os.path.exists(pfad):
        return ts_list, werte

    try:
        with open(pfad, newline="", encoding="utf-8") as f:
            inhalt = f.read()
    except OSError:
        return ts_list, werte

    if not inhalt.strip():
        return ts_list, werte

    try:
        reader = csv.DictReader(inhalt.splitlines())
        for row in reader:
            try:
                datum = row.get("Datum_ISO", "").strip()
                zeit = row.get("Zeit_UTC", "").strip()
                if not datum or not zeit:
                    continue
                dt = datetime.strptime(
                    f"{datum} {zeit}", "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                ts_mpl = mdates.date2num(dt)

                kanalwerte = {}
                for ch in channels:
                    p_str = row.get(f"K{ch}_mbar", "").strip()
                    if p_str:
                        try:
                            p = float(p_str)
                            kanalwerte[ch] = p if (math.isfinite(p) and p > 0) else None
                        except ValueError:
                            kanalwerte[ch] = None
                    else:
                        kanalwerte[ch] = None

                ts_list.append(ts_mpl)
                for ch in channels:
                    werte[ch].append(kanalwerte.get(ch))

            except Exception:
                continue  # Defekte Zeile überspringen
    except Exception:
        pass  # Komplett defektes CSV

    return ts_list, werte


class ArchiveCache:
    """On-demand Cache für Tages-CSV-Archivdaten.

    Cacht nur Vortage (nie den aktuellen Tag, der sich noch ändert).
    Maximal (plot_tage - 1) Einträge.
    """

    def __init__(self, channels: list[int]):
        self._channels = channels
        self._cache: dict[str, tuple[list, dict]] = {}  # date_str → (ts, {ch: vals})

    def clear(self):
        self._cache.clear()

    def get_archive_data(self, ordner: str, plot_tage: int,
                         heute: str, live_ts_min: float | None):
        """Gibt zusammengeführte Archivdaten für die benötigten Vortage zurück.

        Args:
            ordner: Verzeichnis mit Tages-CSVs
            plot_tage: Anzahl gewünschter Tage (inkl. heute)
            heute: Datum des aktuellen Tages als "YYYY-MM-DD"
            live_ts_min: kleinster matplotlib-Zeitstempel im Live-Puffer
                         (zur Duplikatvermeidung); None wenn Puffer leer

        Returns:
            (ts_list, {ch: wert_list}) – nur Daten VOR live_ts_min
        """
        if plot_tage <= 1 or not ordner:
            return [], {ch: [] for ch in self._channels}

        # Benötigte Vortage berechnen
        try:
            heute_dt = datetime.strptime(heute, "%Y-%m-%d")
        except ValueError:
            return [], {ch: [] for ch in self._channels}

        benoetigte_tage = []
        for i in range(1, plot_tage):
            tag = (heute_dt - timedelta(days=i)).strftime("%Y-%m-%d")
            benoetigte_tage.append(tag)

        # Cache aufräumen: nur benötigte Tage behalten
        alte_keys = set(self._cache.keys()) - set(benoetigte_tage)
        for k in alte_keys:
            del self._cache[k]

        # Vortage laden (aus Cache oder CSV)
        all_ts = []
        all_werte = {ch: [] for ch in self._channels}

        for tag in sorted(benoetigte_tage):
            if tag not in self._cache:
                pfad = os.path.join(ordner, f"{tag}.csv")
                ts_tag, w_tag = load_day_csv(pfad, self._channels)
                if ts_tag:
                    self._cache[tag] = (ts_tag, w_tag)
                else:
                    self._cache[tag] = ([], {ch: [] for ch in self._channels})

            ts_tag, w_tag = self._cache[tag]
            all_ts.extend(ts_tag)
            for ch in self._channels:
                all_werte[ch].extend(w_tag.get(ch, []))

        # Duplikatvermeidung: nur Daten VOR dem Live-Puffer
        if live_ts_min is not None and all_ts:
            cut = 0
            for i, t in enumerate(all_ts):
                if t >= live_ts_min:
                    cut = i
                    break
            else:
                cut = len(all_ts)

            all_ts = all_ts[:cut]
            for ch in self._channels:
                all_werte[ch] = all_werte[ch][:cut]

        return all_ts, all_werte
