"""
TPG 366 MaxiGauge – Messdaten-GUI  v3
======================================
Erfordert:  pip install pyqt6 matplotlib
            (oder: pip install pyqt5 matplotlib)

Änderungen v3:
  - Plot: matplotlib statt pyqtgraph (NavigationToolbar: Zoom, Pan, Save …)
  - Log-Fenster: wichtige Ereignisse mit Zeitstempel
  - Zeitstempel Zeile 2: größere Schrift, hellere Farbe
"""

import sys
import os
import csv
import math
import time
import socket
import threading
import calendar
import platform
from datetime import datetime, timezone, timedelta
from collections import deque

from device_config import DeviceConfig, validate_config
from unit_utils import zu_mbar, mbar_zu_anzeige
from csv_archive import ArchiveCache


def ts_print(*args, **kwargs):
    """Print mit Zeitstempel-Prefix für alle Konsolenausgaben."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}]", *args, **kwargs)

try:
    from colorama import init as colorama_init
    from pyfiglet import figlet_format
    _BANNER_AVAILABLE = True
except ImportError:
    _BANNER_AVAILABLE = False

# ── Qt-Backend für matplotlib festlegen (vor allen anderen Imports) ───────────
import os as _os
try:
    import PyQt6        # noqa
    _os.environ.setdefault("QT_API", "pyqt6")
except ImportError:
    _os.environ.setdefault("QT_API", "pyqt5")

# ── PyQt6 / PyQt5 Auto-Import ─────────────────────────────────────────────────
try:
    from PyQt6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QGridLayout, QLabel, QPushButton, QLineEdit, QDoubleSpinBox,
        QGroupBox, QCheckBox, QFileDialog, QComboBox, QListWidget,
        QListWidgetItem, QColorDialog, QSlider, QSplitter, QTextEdit,
        QSizePolicy, QDialog, QDialogButtonBox, QSpinBox,
    )
    from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QObject, QSettings
    from PyQt6.QtGui import QFont, QColor, QKeySequence, QShortcut
    PYQT = 6
except ImportError:
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QGridLayout, QLabel, QPushButton, QLineEdit, QDoubleSpinBox,
        QGroupBox, QCheckBox, QFileDialog, QComboBox, QListWidget,
        QListWidgetItem, QColorDialog, QSlider, QSplitter, QTextEdit,
        QSizePolicy, QDialog, QDialogButtonBox, QSpinBox,
    )
    from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QObject, QSettings
    from PyQt5.QtGui import QFont, QColor, QKeySequence, QShortcut
    PYQT = 5

# ── Matplotlib ────────────────────────────────────────────────────────────────
import matplotlib
matplotlib.use("QtAgg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import numpy as np
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavToolbar


# ══════════════════════════════════════════════════════════════════════════════
#  KONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

# Modul-Level-Defaults (werden von MainWindow beim Start aus Config überschrieben).
# Nur noch für AboutDialog und Legacy-Kompatibilität genutzt.
# Die tatsächlichen Verbindungsparameter stecken in DeviceConfig-Instanzen.
HOST        = "192.168.1.71"
PORT        = 8000
TIMEOUT     = 3
CHANNELS    = [4, 5, 6]
MAX_PUNKTE  = 3600  # Legacy-Default, wird von MainWindow dynamisch überschrieben
PUNKTE_PRO_TAG = 86400  # 1 Datenpunkt/Sekunde × 24h

KANAL_FARBEN = {
    4: "#00C8FF",
    5: "#FFB300",
    6: "#69FF47",
}

SENSOR_STATUS = {
    "0": "OK", "1": "UR", "2": "OR",
    "3": "Fehler", "4": "kein Sensor", "5": "AUS", "6": "Ident.",
}

EINHEITEN = {
    "0": "mbar", "1": "Torr", "2": "Pa",
    "3": "Micron", "4": "hPa", "5": "V",
}

# Legacy-Alias: wird noch von parse_druck-Umfeld referenziert.
# Kanonische Quelle ist jetzt unit_utils.GERAET_ZU_MBAR.
from unit_utils import GERAET_ZU_MBAR as HPAMBAR

PLOT_STYLES = ["Linie", "Scatter", "Linie + Scatter"]

SETTINGS_ORG = "JLU-IPI"
SETTINGS_APP = "TPG366"

VGL_FARBEN = ["#FF6B9D", "#C77DFF", "#FF9F43", "#48DBFB", "#FF6B6B", "#1DD1A1"]

# matplotlib Dark-Theme Farben
MPL_BG   = "#0f0f1a"
MPL_FG   = "#cccccc"
MPL_GRID = "#2a2a3a"

# ── Konfigurationsdatei ───────────────────────────────────────────────────────
import json as _json

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tpg366_config.json")

CONFIG_DEFAULTS = {
    "host":       "192.168.1.71",
    "port":       8000,
    "timeout":    3,
    "channels":   [4, 5, 6],
    "log_folder": "",
    "interval":   1.0,
    "plot_style": "Linie",
    "yscale":     "log",
    "auto_start": False,
    "theme":      "dark",
    "anzeige_einheit": "mbar",
    "plot_tage": 1,
    "alarm": {
        "4": {"aktiv": False, "grenze": 1000.0},
        "5": {"aktiv": False, "grenze": 1000.0},
        "6": {"aktiv": False, "grenze": 1000.0},
    }
}


def _deep_merge(defaults: dict, override: dict) -> dict:
    """Rekursiver Merge: Override-Werte überschreiben Defaults, fehlende bleiben."""
    result = dict(defaults)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def config_laden() -> dict:
    """Lädt tpg366_config.json; fehlende Keys werden mit Defaults ergänzt."""
    cfg = dict(CONFIG_DEFAULTS)
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, encoding="utf-8") as f:
                inhalt = f.read()
            if not inhalt.strip():
                ts_print(f"Config-Datei leer, verwende Defaults: {CONFIG_FILE}")
                return cfg
            gespeichert = _json.loads(inhalt)
            if not isinstance(gespeichert, dict):
                ts_print(f"Config-Datei enthält kein Dict, verwende Defaults: {CONFIG_FILE}")
                return cfg
            cfg = _deep_merge(cfg, gespeichert)
        except Exception as e:
            ts_print(f"Config-Ladefehler ({CONFIG_FILE}): {e}")
    return cfg


def config_speichern(cfg: dict):
    """Schreibt die Konfiguration in tpg366_config.json."""
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            _json.dump(cfg, f, indent=2, ensure_ascii=False)
    except Exception as e:
        ts_print(f"Config-Schreibfehler: {e}")


# ── UI-Strings (für spätere Mehrsprachigkeit hier zentral ändern) ─────────────
S = {
    "btn_start":      "▶  Messung starten",
    "btn_stop":       "■  Messung stoppen",
    "btn_logging_on": "● CSV Logging",
    "msg_gestartet":  "Messung gestartet  (Intervall {iv} s)",
    "msg_gestoppt":   "Messung gestoppt.",
    "msg_verbunden":  "Verbunden mit {host}:{port}  |  Einheit: {einheit}",
    "msg_reconnect":  "⚠ Verbindung verloren — Reconnect-Versuch {n} in {t} s …",
    "msg_alarm":      "⚠ ALARM  Kanal {ch}:  {wert:.3E} mbar  [{zeit}]",
    "msg_logging_new":"Logging [Neue Datei]  →  {pfad}",
    "msg_logging_app":"Logging [Weiterschreiben]  →  {pfad}",
    "msg_logging_off":"Logging gestoppt.",
    "msg_sensor":     "Sensor K{ch} {status} geschaltet.",
    "msg_no_sensor":  "Messung starten um Sensoren zu schalten.",
    "msg_pdf_ok":     "PDF gespeichert: {pfad}",
    "msg_pdf_err":    "⚠ PDF-Export fehlgeschlagen: {e}",
    "msg_einheit":    "Anzeigeeinheit geändert: {einheit}",
    "msg_intervall":  "Messintervall geändert: {v} s",
    "msg_theme":      "Theme gewechselt: {name}",
    "lbl_messung_stop": "Sensor einschalten",
    "tt_start":       "Messung starten / stoppen\nVerbindet mit TPG 366 und beginnt Druckerfassung",
    "tt_intervall":   "Messintervall in Sekunden\n(0,5 s – 300 s)\nÄnderungen wirken sofort",
    "tt_fenster":     "Zeitfenster: nur letzte N Minuten anzeigen\n0 = alle gepufferten Daten (max. durch Plot-Tage begrenzt)",
    "tt_yscale":      "Y-Achse: zwischen logarithmischer und linearer Skalierung umschalten",
    "tt_logging":     "CSV-Logging starten / stoppen\nDateiname: YYYY-MM-DD.csv im gewählten Ordner",
    "tt_ordner":      "Speicherordner für CSV-Dateien\nWird automatisch gespeichert",
    "tt_browse":      "Ordner auswählen",
    "tt_einheit":     "Anzeigeeinheit für Plot und Kanalwidgets\nCSV wird immer in mbar gespeichert",
    "tt_vgl":         "Vergleichsdaten-Fenster öffnen\nCSV-Dateien älterer Tage laden",
    "tt_pdf":         "Aktuellen Plot als PDF exportieren",
    "tt_theme":       "Zwischen Dark- und Light-Theme umschalten",
    "tt_autostart":   "Messung automatisch starten wenn das Programm geöffnet wird",
    "tt_about":       "Über dieses Programm",
    "tt_log_fenster": "Ereignis-Log: Verbindung, Messung, Logging, Alarme",
    "tt_kanal":       "Kanal {ch} – Aktueller Druckwert und Sensor-Status",
    "tt_sensor":      "Sensor Kanal {ch} manuell ein-/ausschalten",
    "tt_alarm_chk":   "Alarmgrenze für Kanal {ch} aktivieren",
    "tt_alarm_spin":  "Alarmgrenzwert für Kanal {ch} in mbar",
    "tt_alarm_grp":   "Alarmgrenzen pro Kanal\nBei Überschreitung: blinkt rot, Statusmeldung, CSV-Eintrag",
    "tt_zeitstempel": "Aktuelle Zeit: Gießen (CET/CEST), UTC, Kalenderwoche und MJD",
}


# ══════════════════════════════════════════════════════════════════════════════
#  ZEITFUNKTIONEN
# ══════════════════════════════════════════════════════════════════════════════

def giessen_tz() -> timezone:
    now_utc = datetime.now(timezone.utc)
    year    = now_utc.year

    def last_sunday(yr, mo):
        last_day = calendar.monthrange(yr, mo)[1]
        d = datetime(yr, mo, last_day, 1, 0, tzinfo=timezone.utc)
        d -= timedelta(days=(d.weekday() + 1) % 7)
        return d

    if last_sunday(year, 3) <= now_utc < last_sunday(year, 10):
        return timezone(timedelta(hours=2), "CEST")
    return timezone(timedelta(hours=1), "CET")


def datetime_utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_mjd(dt: datetime) -> float:
    epoch = datetime(1858, 11, 17, tzinfo=timezone.utc)
    return (dt.astimezone(timezone.utc) - epoch).total_seconds() / 86400.0


def fmt_giessen_time(x, pos=None):
    """Matplotlib-Ticker-Formatter: matplotlib-Zahl → hh:mm:ss Gießen."""
    try:
        dt_utc = mdates.num2date(x)                  # UTC-aware datetime
        dt_loc = dt_utc.astimezone(giessen_tz())
        return dt_loc.strftime("%H:%M:%S")
    except Exception:
        return ""


# ══════════════════════════════════════════════════════════════════════════════
#  PROTOKOLL-SCHICHT
# ══════════════════════════════════════════════════════════════════════════════

ACK = b'\x06'
ENQ = b'\x05'


def _recv_until(sock, terminator=b'\r\n', max_bytes=256):
    """Liest vom Socket bis Terminator oder max_bytes erreicht."""
    buf = b''
    while len(buf) < max_bytes:
        chunk = sock.recv(1)
        if not chunk:
            break
        buf += chunk
        if buf.endswith(terminator):
            break
    return buf


def pv_command(sock, befehl: str):
    sock.sendall((befehl + "\r\n").encode("ascii"))
    time.sleep(0.08)
    r = sock.recv(64)
    if ACK not in r:
        return False, f"NAK: {repr(r)}"
    sock.sendall(ENQ)
    time.sleep(0.08)
    return True, _recv_until(sock).decode("ascii").strip()


def parse_druck(antwort: str):
    teile = antwort.split(",")
    if len(teile) == 2:
        code = teile[0].strip()
        try:
            v = float(teile[1].strip())
            if math.isfinite(v):
                return code, v
            return code, None  # NaN/Inf → ungültig
        except ValueError:
            return code, None
    return "?", None


# zu_mbar ist jetzt aus unit_utils importiert


# ══════════════════════════════════════════════════════════════════════════════
#  MESS-THREAD
# ══════════════════════════════════════════════════════════════════════════════

class MeasSignals(QObject):
    new_data     = pyqtSignal(dict, object)   # alle Rohwerte (1s-Takt)
    save_data    = pyqtSignal(dict, object)   # gefilterter Wert (Adaptiv oder Normal)
    connected    = pyqtSignal(str)
    reconnecting = pyqtSignal(int)
    error        = pyqtSignal(str)


RECONNECT_INTERVAL = 5   # Sekunden zwischen Reconnect-Versuchen


class MeasThread(threading.Thread):
    def __init__(self, interval_s: float, signals: MeasSignals,
                 dev_cfg: DeviceConfig):
        super().__init__(daemon=True)
        self._interval = interval_s
        self._interval_lock = threading.Lock()
        self.signals  = signals
        self.dev_cfg  = dev_cfg   # Verbindungsparameter (unveränderlich)
        self._running = False
        self._sock    = None
        self._lock    = threading.Lock()  # schützt Socket-Zugriff (Mess + Sensor)
        self._stop_event = threading.Event()

    @property
    def interval(self) -> float:
        with self._interval_lock:
            return self._interval

    @interval.setter
    def interval(self, value: float):
        with self._interval_lock:
            self._interval = max(0.5, float(value))

    def _connect(self) -> bool:
        """Baut TCP-Verbindung auf. Gibt True bei Erfolg zurück."""
        cfg = self.dev_cfg
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(cfg.timeout)
            s.connect((cfg.host, cfg.port))
            with self._lock:
                self._sock = s
            return True
        except Exception as e:
            self.signals.error.emit(f"Verbindung fehlgeschlagen: {e}")
            try: s.close()
            except Exception: pass
            with self._lock:
                self._sock = None
            return False

    def run(self):
        # _running wird vom Aufrufer VOR start() gesetzt, um Race mit stop() zu vermeiden
        versuch = 0

        while self._running:
            # ── Verbinden (mit Reconnect-Schleife) ──────────────────────────
            if not self._connect():
                versuch += 1
                self.signals.reconnecting.emit(versuch)
                if self._stop_event.wait(RECONNECT_INTERVAL):
                    return  # stop() wurde aufgerufen
                continue

            versuch = 0   # Verbindung erfolgreich → Zähler zurück
            with self._lock:
                try:
                    ok, wert = pv_command(self._sock, "UNI")
                except OSError:
                    ok = False
                    wert = ""
            self.signals.connected.emit(
                EINHEITEN.get(wert.strip(), "?") if ok else "?"
            )

            # ── Mess-Schleife ────────────────────────────────────────────────
            while self._running:
                t0     = time.time()
                ts_utc = datetime_utc_now()
                data   = {}
                conn_lost = False
                with self._lock:
                    for ch in self.dev_cfg.channels:
                        if not self._running:
                            return
                        try:
                            ok, ans = pv_command(self._sock, f"PR{ch}")
                            data[ch] = parse_druck(ans) if ok else ("?", None)
                        except OSError:
                            if not self._running:
                                return
                            conn_lost = True
                            break
                        except Exception as e:
                            if not self._running:
                                return
                            self.signals.error.emit(f"Lesefehler K{ch}: {e}")
                            data[ch] = ("?", None)

                if conn_lost:
                    self.signals.error.emit(
                        f"Verbindung verloren — Reconnect in {RECONNECT_INTERVAL} s"
                    )
                    with self._lock:
                        if self._sock:
                            try: self._sock.close()
                            except Exception: pass
                            self._sock = None
                    break   # innere Schleife verlassen → äußere Schleife reconnectet

                if self._running:
                    self.signals.new_data.emit(data, ts_utc)
                wait = max(0, self.interval - (time.time() - t0))
                if wait > 0 and self._stop_event.wait(wait):
                    return  # stop() während sleep → sofort beenden

    def stop(self):
        self._running = False
        self._stop_event.set()
        # Socket schließen OHNE Lock → entblockt recv() sofort, kein GUI-Freeze
        # socket.close() von anderem Thread erzeugt OSError im blockierenden recv()
        sock = self._sock
        if sock:
            try:
                sock.close()
            except Exception:
                pass
        # Kurzer Join – nach Socket-Close beendet der Thread sich schnell
        if self._started.is_set():
            self.join(timeout=1.0)

    def set_sensor(self, channel: int, on: bool):
        with self._lock:
            if not self._sock:
                return
            try:
                ok, wert = pv_command(self._sock, "SEN")
                if not ok:
                    return
                stati = [s.strip() for s in wert.split(",")]
                if len(stati) < 6:
                    return
                stati[channel - 1] = "1" if on else "0"
                pv_command(self._sock, "SEN=" + ",".join(stati))
            except OSError:
                pass  # Verbindung verloren, Messschleife handled Reconnect


# ══════════════════════════════════════════════════════════════════════════════
#  ADAPTIV-FILTER
# ══════════════════════════════════════════════════════════════════════════════

class AdaptivFilter:
    """
    Entscheidet pro Messzyklus ob ein Wert gespeichert/geplottet wird.

    Speichern wenn:
      |wert - letzter_wert| / letzter_wert > schwelle_pct/100
      ODER
      zeit_seit_letztem_speichern >= max_wartezeit_s
    """
    def __init__(self, schwelle_pct: float = 0.5, max_wartezeit_s: float = 60.0):
        self.schwelle_pct   = schwelle_pct
        self.max_wartezeit  = max_wartezeit_s
        self._letzter: dict = {}      # ch → letzter gespeicherter mbar-Wert
        self._letzter_ts: float = 0.0 # Unix-Timestamp des letzten Speicherns

    def reset(self):
        self._letzter.clear()
        self._letzter_ts = 0.0

    def pruefen(self, data_mbar: dict, ts_unix: float) -> bool:
        """
        Gibt True zurück wenn die Daten gespeichert werden sollen.
        data_mbar: {channel: wert_mbar_or_None}
        """
        dt = ts_unix - self._letzter_ts
        if dt >= self.max_wartezeit:
            self._update(data_mbar, ts_unix)
            return True

        for ch, wert in data_mbar.items():
            if wert is None:
                continue
            letzter = self._letzter.get(ch)
            if letzter is None or letzter == 0:
                self._update(data_mbar, ts_unix)
                return True
            aenderung = abs(wert - letzter) / letzter * 100.0
            if aenderung >= self.schwelle_pct:
                self._update(data_mbar, ts_unix)
                return True
        return False

    def _update(self, data_mbar: dict, ts_unix: float):
        for ch, wert in data_mbar.items():
            if wert is not None:
                self._letzter[ch] = wert
        self._letzter_ts = ts_unix


# ══════════════════════════════════════════════════════════════════════════════
#  KANAL-WIDGET
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  SCIENTIFIC SPINBOX
# ══════════════════════════════════════════════════════════════════════════════

class ScientificSpinBox(QDoubleSpinBox):
    """
    QDoubleSpinBox-Erweiterung mit wissenschaftlicher Notation.

    - Anzeige:  1.23E-05  (immer 3 signifikante Stellen)
    - Eingabe:  1e-5, 1.5E-3, 0.00001, 750 — alles wird akzeptiert
    - Bereich:  1e-12 … 2000 (für Vakuum-Druckwerte geeignet)
    - Pfeile:   ×10 / ÷10 pro Schritt
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setRange(1e-12, 2000.0)
        self.setValue(1000.0)
        self.setDecimals(12)       # intern viele Stellen, Anzeige überschreiben wir
        self.setSingleStep(0)      # Schritt per stepBy überschrieben
        self.setFixedWidth(115)

    # ── Anzeige ───────────────────────────────────────────────────────────────

    def textFromValue(self, value: float) -> str:
        if value <= 0:
            return "0.00E+00"
        return f"{value:.2E}"

    # ── Eingabe-Validierung ───────────────────────────────────────────────────

    def valueFromText(self, text: str) -> float:
        text = text.strip().replace(",", ".")
        try:
            v = float(text)
        except ValueError:
            return self.value()
        return max(self.minimum(), min(self.maximum(), v))

    def validate(self, text: str, pos: int):
        """Akzeptiert alles was wie eine Zahl oder wissenschaftliche Notation aussieht."""
        try:
            from PyQt6.QtGui import QValidator
        except ImportError:
            from PyQt5.QtGui import QValidator
        t = text.strip().replace(",", ".")
        import re
        if re.match(r'^-?[\d]*\.?[\d]*([eE][+-]?[\d]*)?$', t):
            try:
                float(t)
                return QValidator.State.Acceptable, text, pos
            except ValueError:
                return QValidator.State.Intermediate, text, pos
        return QValidator.State.Invalid, text, pos

    # ── Pfeile: ×10 / ÷10 ────────────────────────────────────────────────────

    def stepBy(self, steps: int):
        v = self.value()
        if v <= 0:
            v = 1e-12
        factor = 10.0 ** steps
        new_v  = max(self.minimum(), min(self.maximum(), v * factor))
        self.setValue(new_v)


class KanalWidget(QGroupBox):
    alarm_ausgeloest = pyqtSignal(int, float)

    def __init__(self, channel: int, farbe: str, parent=None):
        super().__init__(f"Kanal {channel}", parent)
        self.channel      = channel
        self.farbe        = farbe
        self.alarm_grenze = None
        self._alarm_aktiv  = False
        self._blink_status = False

        self.setStyleSheet(f"""
            QGroupBox {{
                border: 2px solid {farbe}; border-radius: 6px;
                margin-top: 10px; font-weight: bold; color: {farbe};
            }}
            QGroupBox::title {{ subcontrol-origin: margin; left: 10px; padding: 0 4px; }}
        """)
        lay = QVBoxLayout(self)
        lay.setSpacing(4)

        self.lbl_wert = QLabel("---")
        self.lbl_wert.setFont(QFont("Courier New", 26, QFont.Weight.Bold))
        self.lbl_wert.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_wert.setStyleSheet(f"color: {farbe};")

        self.lbl_einheit = QLabel("mbar")
        self.lbl_einheit.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_einheit.setStyleSheet(f"color: {farbe}99; font-size: 12px;")

        self.lbl_status = QLabel("–")
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_status.setStyleSheet("color: #aaa; font-size: 11px;")

        self.lbl_alarm = QLabel("")
        self.lbl_alarm.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_alarm.setStyleSheet(
            "color: #FF4444; font-weight: bold; font-size: 11px;"
        )

        self.btn_sensor = QPushButton("Sensor EIN")
        self.btn_sensor.setCheckable(True)
        self._theme_dark = True   # initial dunkel
        self._update_sensor_style()
        for w in [self.lbl_wert, self.lbl_einheit, self.lbl_status,
                  self.lbl_alarm, self.btn_sensor]:
            lay.addWidget(w)

        self._blink_timer = QTimer()
        self._blink_timer.setInterval(500)
        self._blink_timer.timeout.connect(self._blink)

    def _update_sensor_style(self):
        if self._theme_dark:
            bg, col, brd = "#222", "#888", "#444"
        else:
            bg, col, brd = "#f0f0f0", "#555", "#bbb"
        self.btn_sensor.setStyleSheet(f"""
            QPushButton {{
                background: {bg}; color: {col};
                border: 1px solid {brd}; border-radius: 4px;
                padding: 3px 8px; font-size: 11px;
            }}
            QPushButton:checked {{
                background: {self.farbe}33; color: {self.farbe};
                border: 1px solid {self.farbe};
            }}
        """)

    def set_theme(self, dark: bool):
        """Aktualisiert das Widget für Dark (True) oder Light (False) Theme."""
        self._theme_dark = dark
        self._update_sensor_style()
        # Status-Label Farbe anpassen
        status_col = "#888" if dark else "#666"
        self.lbl_status.setStyleSheet(f"color: {status_col}; font-size: 11px;")
        # Hintergrund des GroupBox
        bg = "transparent" if dark else "#ffffff"
        self.setStyleSheet(f"""
            QGroupBox {{
                border: 2px solid {self.farbe}; border-radius: 6px;
                margin-top: 10px; font-weight: bold; color: {self.farbe};
                background: {bg};
            }}
            QGroupBox::title {{ subcontrol-origin: margin; left: 10px; padding: 0 4px; }}
        """)

    def _normal_style(self):
        bg = "transparent" if self._theme_dark else "#ffffff"
        return f"""
            QGroupBox {{
                border: 2px solid {self.farbe}; border-radius: 6px;
                margin-top: 10px; font-weight: bold; color: {self.farbe};
                background: {bg};
            }}
            QGroupBox::title {{ subcontrol-origin: margin; left: 10px; padding: 0 4px; }}
        """

    def _alarm_style(self):
        return """
            QGroupBox {
                border: 2px solid #FF4444; border-radius: 6px;
                margin-top: 10px; font-weight: bold; color: #FF4444;
                background: #3d0a0a;
            }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 4px; }
        """

    def _blink(self):
        self._blink_status = not self._blink_status
        self.setStyleSheet(
            self._alarm_style() if self._blink_status else self._normal_style()
        )

    def update_display(self, status_code: str, wert_anzeige, einheit: str = "mbar",
                       wert_mbar=None):
        st = SENSOR_STATUS.get(status_code, status_code)
        if status_code == "0" and wert_anzeige is not None:
            self.lbl_wert.setText(f"{wert_anzeige:.3E}")
            self.lbl_einheit.setText(einheit)
            self.lbl_status.setText(st)
            # alarm_grenze ist immer in mbar → Vergleich in mbar
            alarm_wert = wert_mbar if wert_mbar is not None else wert_anzeige
            if self.alarm_grenze is not None and alarm_wert > self.alarm_grenze:
                self.lbl_alarm.setText(f"⚠ ALARM > {self.alarm_grenze:.2E}")
                self.lbl_wert.setStyleSheet("color: #FF4444;")
                if not self._alarm_aktiv:
                    self._alarm_aktiv = True
                    self._blink_timer.start()
                    self.alarm_ausgeloest.emit(self.channel, alarm_wert)
            else:
                self.lbl_alarm.setText("")
                self.lbl_wert.setStyleSheet(f"color: {self.farbe};")
                if self._alarm_aktiv:
                    self._alarm_aktiv = False
                    self._blink_timer.stop()
                    self.setStyleSheet(self._normal_style())
        else:
            self.lbl_wert.setText(st)
            self.lbl_wert.setStyleSheet("color: #888;")
            self.lbl_einheit.setText("")
            self.lbl_status.setText("")
            self.lbl_alarm.setText("")


# ══════════════════════════════════════════════════════════════════════════════
#  VERGLEICHSDATEN
# ══════════════════════════════════════════════════════════════════════════════

class VergleichsDatei:
    """Geladene CSV eines Vergleichstags als matplotlib-Linien."""

    def __init__(self, pfad: str, farbe: str, ax, canvas):
        self.pfad   = pfad
        self.label  = os.path.basename(pfad)
        self.farbe  = farbe
        self.alpha  = 0.55
        self.lw     = 1.5
        self.ps     = 5
        self._ax    = ax
        self._canvas = canvas
        self._lines: list = []
        self._daten: dict = {}
        self._laden()
        self._draw()

    def _laden(self):
        try:
            with open(self.pfad, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            for ch in CHANNELS:
                ts_list, p_list = [], []
                for row in rows:
                    try:
                        datum = row.get("Datum_ISO", "").strip()
                        zeit  = row.get("Zeit_Giessen", "").strip()
                        dt    = datetime.strptime(
                            f"{datum} {zeit}", "%Y-%m-%d %H:%M:%S"
                        ).replace(tzinfo=giessen_tz())
                        p_str = row.get(f"K{ch}_mbar", "").strip()
                        if p_str:
                            p = float(p_str)
                            if p > 0:
                                ts_list.append(mdates.date2num(dt))
                                p_list.append(p)
                    except Exception:
                        continue
                self._daten[ch] = (ts_list, p_list)
        except Exception as e:
            ts_print(f"Ladefehler {self.pfad}: {e}")

    def _draw(self):
        self._remove()
        c = QColor(self.farbe)
        rgba = (c.red()/255, c.green()/255, c.blue()/255, self.alpha)
        for ch in CHANNELS:
            ts_list, p_list = self._daten.get(ch, ([], []))
            if not ts_list:
                continue
            line, = self._ax.plot(
                ts_list, p_list,
                color=rgba, linewidth=self.lw,
                linestyle="--",
                marker="o", markersize=self.ps,
                label=f"{self.label} K{ch}"
            )
            self._lines.append(line)
        self._canvas.draw_idle()

    def redraw(self):
        self._draw()

    def _remove(self):
        for ln in self._lines:
            try:
                ln.remove()
            except Exception:
                pass
        self._lines.clear()

    def remove(self):
        self._remove()
        self._canvas.draw_idle()


# ══════════════════════════════════════════════════════════════════════════════
#  VERGLEICHSFENSTER
# ══════════════════════════════════════════════════════════════════════════════

class VergleichsFenster(QWidget):
    def __init__(self, ax, canvas, vergleiche: list, start_ordner: str,
                 parent=None):
        super().__init__(parent, Qt.WindowType.Window)
        self.setWindowTitle("TPG 366 – Vergleichsdaten")
        self.resize(420, 480)
        self._ax         = ax
        self._canvas     = canvas
        self._vergleiche = vergleiche
        self._ordner     = start_ordner
        self.setStyleSheet("""
            QWidget { background-color: #1a1a2e; color: #e0e0e0; }
            QGroupBox {
                border: 1px solid #333; border-radius: 6px;
                margin-top: 10px; color: #aaa;
            }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 4px; }
            QListWidget {
                background: #0f0f1a; border: 1px solid #444;
                color: #e0e0e0; border-radius: 4px;
            }
            QPushButton {
                background: #16213e; border: 1px solid #0f3460;
                border-radius: 5px; color: #e0e0e0; padding: 5px 14px;
            }
            QPushButton:hover { background: #0f3460; }
            QSlider::groove:horizontal { background: #333; height: 4px; border-radius: 2px; }
            QSlider::handle:horizontal {
                background: #4f8ef7; width: 14px; height: 14px;
                margin: -5px 0; border-radius: 7px;
            }
        """)
        self._build()

    def _build(self):
        root = QVBoxLayout(self)
        root.setSpacing(8)
        root.setContentsMargins(10, 10, 10, 10)

        grp = QGroupBox("Geladene Vergleichsdateien")
        gl  = QVBoxLayout(grp)

        btn_row = QHBoxLayout()
        btn_add = QPushButton("＋ Datei laden")
        btn_add.clicked.connect(self._add)
        btn_rem = QPushButton("✕ Entfernen")
        btn_rem.clicked.connect(self._remove)
        btn_row.addWidget(btn_add)
        btn_row.addWidget(btn_rem)
        gl.addLayout(btn_row)

        self.vgl_list = QListWidget()
        self.vgl_list.setMinimumHeight(100)
        self.vgl_list.currentRowChanged.connect(self._sel_changed)
        for v in self._vergleiche:
            item = QListWidgetItem(v.label)
            item.setForeground(QColor(v.farbe))
            self.vgl_list.addItem(item)
        gl.addWidget(self.vgl_list)

        self.grp_darst = QGroupBox("Darstellung")
        vsl = QGridLayout(self.grp_darst)
        vsl.setSpacing(6)
        vsl.setColumnMinimumWidth(0, 65)

        vsl.addWidget(QLabel("Farbe:"), 0, 0)
        self.btn_farbe = QPushButton("■")
        self.btn_farbe.setFixedWidth(44)
        self.btn_farbe.clicked.connect(self._pick_color)
        vsl.addWidget(self.btn_farbe, 0, 1)

        for row_i, (label, attr, sld_attr, lbl_attr, lo, hi, init, suffix) in enumerate([
            ("Opacity:", "alpha", "sld_alpha", "lbl_alpha",  5, 100, 55, "%"),
            ("Linie:",   "lw",    "sld_lw",    "lbl_lw",     1,   6,  2, ""),
            ("Punkte:",  "ps",    "sld_ps",    "lbl_ps",     2,  16,  5, ""),
        ], start=1):
            vsl.addWidget(QLabel(label), row_i, 0)
            sld = QSlider(Qt.Orientation.Horizontal)
            sld.setRange(lo, hi); sld.setValue(init)
            lbl = QLabel(f"{init}{suffix}")
            lbl.setStyleSheet("color: #888; font-size: 11px;")
            lbl.setFixedWidth(30)
            row_w = QHBoxLayout()
            row_w.addWidget(sld); row_w.addWidget(lbl)
            vsl.addLayout(row_w, row_i, 1)
            setattr(self, sld_attr, sld)
            setattr(self, lbl_attr, lbl)
            sld.valueChanged.connect(
                lambda val, a=attr, la=lbl_attr, s=suffix:
                    self._sld_changed(a, val, la, s)
            )

        self.grp_darst.setEnabled(False)
        gl.addWidget(self.grp_darst)
        root.addWidget(grp)

        btn_close = QPushButton("Schließen")
        btn_close.clicked.connect(self.close)
        root.addWidget(btn_close)

    def _add(self):
        pfade, _ = QFileDialog.getOpenFileNames(
            self, "Vergleichsdatei(en) laden", self._ordner,
            "CSV-Dateien (*.csv);;Alle Dateien (*)"
        )
        for pfad in pfade:
            if any(v.pfad == pfad for v in self._vergleiche):
                continue
            farbe = VGL_FARBEN[len(self._vergleiche) % len(VGL_FARBEN)]
            vgl   = VergleichsDatei(pfad, farbe, self._ax, self._canvas)
            self._vergleiche.append(vgl)
            item = QListWidgetItem(vgl.label)
            item.setForeground(QColor(farbe))
            self.vgl_list.addItem(item)

    def _remove(self):
        row = self.vgl_list.currentRow()
        if not (0 <= row < len(self._vergleiche)):
            return
        self._vergleiche[row].remove()
        self._vergleiche.pop(row)
        self.vgl_list.takeItem(row)
        self.grp_darst.setEnabled(False)

    def _sel_changed(self, row: int):
        ok = 0 <= row < len(self._vergleiche)
        self.grp_darst.setEnabled(ok)
        if not ok:
            return
        vgl = self._vergleiche[row]
        self.btn_farbe.setStyleSheet(
            f"background: {vgl.farbe}; border: 1px solid #666;"
        )
        for sld, val, lbl, suffix in [
            (self.sld_alpha, int(vgl.alpha * 100), self.lbl_alpha, "%"),
            (self.sld_lw,    int(vgl.lw),          self.lbl_lw,    ""),
            (self.sld_ps,    vgl.ps,                self.lbl_ps,    ""),
        ]:
            sld.blockSignals(True)
            sld.setValue(val)
            lbl.setText(f"{val}{suffix}")
            sld.blockSignals(False)

    def _current(self):
        row = self.vgl_list.currentRow()
        return self._vergleiche[row] if 0 <= row < len(self._vergleiche) else None

    def _pick_color(self):
        vgl = self._current()
        if not vgl:
            return
        col = QColorDialog.getColor(QColor(vgl.farbe), self)
        if col.isValid():
            vgl.farbe = col.name()
            self.btn_farbe.setStyleSheet(
                f"background: {vgl.farbe}; border: 1px solid #666;"
            )
            self.vgl_list.item(self.vgl_list.currentRow()).setForeground(col)
            vgl.redraw()

    def _sld_changed(self, attr: str, val: int, lbl_attr: str, suffix: str):
        getattr(self, lbl_attr).setText(f"{val}{suffix}")
        vgl = self._current()
        if not vgl:
            return
        if attr == "alpha":
            vgl.alpha = val / 100.0
        elif attr == "lw":
            vgl.lw = float(val)
        elif attr == "ps":
            vgl.ps = val
        vgl.redraw()


# ══════════════════════════════════════════════════════════════════════════════
#  ABOUT-DIALOG
# ══════════════════════════════════════════════════════════════════════════════

VERSION = "3.0"
AUTHOR  = "JLU Gießen – Institut für Physik"

class AboutDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Über TPG 366 MaxiGauge GUI")
        self.setFixedWidth(420)
        self.setStyleSheet("""
            QDialog  { background: #1a1a2e; color: #e0e0e0; }
            QLabel   { color: #e0e0e0; }
            QPushButton {
                background: #16213e; border: 1px solid #0f3460;
                border-radius: 5px; color: #e0e0e0; padding: 5px 18px;
            }
            QPushButton:hover { background: #0f3460; }
        """)
        root = QVBoxLayout(self)
        root.setSpacing(10)
        root.setContentsMargins(20, 20, 20, 16)

        # Titel
        lbl_title = QLabel("TPG 366 MaxiGauge – Datenerfassung")
        lbl_title.setStyleSheet(
            "font-size: 15px; font-weight: 700; color: #4f8ef7;"
        )
        root.addWidget(lbl_title)

        # Trennlinie
        line = QLabel(); line.setFixedHeight(1)
        line.setStyleSheet("background: #2e3247;")
        root.addWidget(line)

        # Info-Tabelle
        grid = QGridLayout(); grid.setSpacing(6)
        infos = [
            ("Version:",         VERSION),
            ("Autor / Institut:", AUTHOR),
            ("Gerät:",           "Pfeiffer Vacuum TPG 366 MaxiGauge"),
            ("Protokoll:",       "Pfeiffer Vacuum ASCII, TCP/IP"),
            ("IP / Port:",       f"{HOST}  :  {PORT}"),
            ("Timeout:",         f"{TIMEOUT} s"),
            ("Kanäle:",          ", ".join(str(c) for c in CHANNELS)),
            ("Konfigdatei:",     CONFIG_FILE),
            ("Qt-Binding:",      f"PyQt{PYQT}"),
            ("Python:",          f"{sys.version.split()[0]}"),
        ]
        lbl_style = "color: #8b8fa8; font-size: 11px;"
        val_style = "color: #c0c4d8; font-size: 11px; font-family: Consolas;"
        for row_i, (k, v) in enumerate(infos):
            lk = QLabel(k); lk.setStyleSheet(lbl_style)
            lv = QLabel(v); lv.setStyleSheet(val_style)
            lv.setTextInteractionFlags(
                Qt.TextInteractionFlag.TextSelectableByMouse
            )
            grid.addWidget(lk, row_i, 0)
            grid.addWidget(lv, row_i, 1)
        root.addLayout(grid)

        # OK-Button
        bbox = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok)
        bbox.accepted.connect(self.accept)
        root.addWidget(bbox)


# ══════════════════════════════════════════════════════════════════════════════
#  ADAPTIV-DIALOG
# ══════════════════════════════════════════════════════════════════════════════

class AdaptivDialog(QDialog):
    """Einstellungen für den Adaptiv-Modus."""
    def __init__(self, schwelle_pct: float, max_wartezeit: float,
                 mess_intervall: float, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Adaptiv-Modus Einstellungen")
        self.setFixedWidth(380)
        self.setStyleSheet("""
            QDialog  { background: #1a1a2e; color: #e0e0e0; }
            QLabel   { color: #e0e0e0; }
            QGroupBox { border: 1px solid #333; border-radius:6px;
                        margin-top:8px; color:#aaa; }
            QGroupBox::title { subcontrol-origin:margin; left:8px; padding:0 4px; }
            QDoubleSpinBox, QSpinBox, QComboBox {
                background:#0f0f1a; border:1px solid #444;
                border-radius:4px; color:#e0e0e0; padding:3px 6px; }
            QPushButton { background:#16213e; border:1px solid #0f3460;
                border-radius:5px; color:#e0e0e0; padding:5px 14px; }
            QPushButton:hover { background:#0f3460; }
        """)

        root = QVBoxLayout(self)
        root.setSpacing(10)
        root.setContentsMargins(16, 16, 16, 12)

        # ── Messintervall (intern) ────────────────────────────────────────────
        grp1 = QGroupBox("Messintervall (intern)")
        gl1  = QHBoxLayout(grp1)
        gl1.addWidget(QLabel("Intervall:"))
        self.spin_mess = QDoubleSpinBox()
        self.spin_mess.setRange(0.5, 10.0)
        self.spin_mess.setValue(mess_intervall)
        self.spin_mess.setSuffix(" s")
        self.spin_mess.setSingleStep(0.5)
        self.spin_mess.setToolTip("Wie oft das Gerät abgefragt wird\n(unabhängig vom globalen Intervall-Spinner)")
        gl1.addWidget(self.spin_mess)
        gl1.addStretch()
        root.addWidget(grp1)

        # ── Änderungsschwelle ─────────────────────────────────────────────────
        grp2 = QGroupBox("Änderungsschwelle")
        gl2  = QHBoxLayout(grp2)
        gl2.addWidget(QLabel("Speichern wenn Änderung ≥"))
        self.spin_schwelle = QDoubleSpinBox()
        self.spin_schwelle.setRange(0.01, 50.0)
        self.spin_schwelle.setValue(schwelle_pct)
        self.spin_schwelle.setSuffix(" %")
        self.spin_schwelle.setSingleStep(0.1)
        self.spin_schwelle.setDecimals(2)
        self.spin_schwelle.setToolTip(
            "Prozentualer Unterschied zum zuletzt gespeicherten Wert\n"
            "z.B. 0.5% bei 750 mbar = Speichern wenn Δ > 3.75 mbar"
        )
        gl2.addWidget(self.spin_schwelle)
        gl2.addStretch()
        root.addWidget(grp2)

        # ── Maximale Wartezeit ────────────────────────────────────────────────
        grp3 = QGroupBox("Maximale Wartezeit (Speichern erzwingen)")
        gl3  = QHBoxLayout(grp3)
        gl3.addWidget(QLabel("Spätestens alle"))
        self.cmb_wartezeit = QComboBox()
        self.cmb_wartezeit.addItems(["15 s", "30 s", "45 s", "60 s", "120 s", "Manuell"])
        self.cmb_wartezeit.setToolTip("Auch bei stabilem Druck spätestens nach N Sekunden speichern")
        self.spin_wartezeit = QDoubleSpinBox()
        self.spin_wartezeit.setRange(5.0, 3600.0)
        self.spin_wartezeit.setValue(max_wartezeit)
        self.spin_wartezeit.setSuffix(" s")
        self.spin_wartezeit.setSingleStep(5.0)
        gl3.addWidget(self.cmb_wartezeit)
        gl3.addWidget(self.spin_wartezeit)
        gl3.addStretch()
        # Preset-Auswahl → spin befüllen
        self.cmb_wartezeit.currentTextChanged.connect(self._on_preset)
        self._sync_preset(max_wartezeit)
        root.addWidget(grp3)

        # ── Hinweis ───────────────────────────────────────────────────────────
        hint = QLabel(
            "ℹ  Das Gerät wird immer im eingestellten Messintervall abgefragt.\n"
            "   Gespeichert/geplottet wird nur wenn eine der Bedingungen erfüllt ist."
        )
        hint.setStyleSheet("color: #8b8fa8; font-size: 10px;")
        hint.setWordWrap(True)
        root.addWidget(hint)

        # ── Buttons ───────────────────────────────────────────────────────────
        bbox = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok |
            QDialogButtonBox.StandardButton.Cancel
        )
        bbox.accepted.connect(self.accept)
        bbox.rejected.connect(self.reject)
        root.addWidget(bbox)

    def _on_preset(self, text: str):
        presets = {"15 s": 15, "30 s": 30, "45 s": 45,
                   "60 s": 60, "120 s": 120}
        if text in presets:
            self.spin_wartezeit.setValue(presets[text])

    def _sync_preset(self, wert: float):
        presets = {15: "15 s", 30: "30 s", 45: "45 s", 60: "60 s", 120: "120 s"}
        text = presets.get(int(wert), "Manuell")
        self.cmb_wartezeit.blockSignals(True)
        self.cmb_wartezeit.setCurrentText(text)
        self.cmb_wartezeit.blockSignals(False)

    def werte(self) -> tuple:
        """Gibt (schwelle_pct, max_wartezeit_s, mess_intervall_s) zurück."""
        return (
            self.spin_schwelle.value(),
            self.spin_wartezeit.value(),
            self.spin_mess.value(),
        )


# ══════════════════════════════════════════════════════════════════════════════
#  HAUPTFENSTER
# ══════════════════════════════════════════════════════════════════════════════

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("TPG 366 MaxiGauge – Datenerfassung  v3")
        self.resize(1280, 900)

        # Konfiguration laden + defensiv validieren
        self._cfg = config_laden()
        self.dev_cfg = validate_config(self._cfg)

        # Modul-Globals aktualisieren (Legacy: AboutDialog, _cfg_snapshot, UI-Strings)
        global HOST, PORT, TIMEOUT, CHANNELS
        HOST     = self.dev_cfg.host
        PORT     = self.dev_cfg.port
        TIMEOUT  = self.dev_cfg.timeout
        CHANNELS = self.dev_cfg.channels

        self.einheit         = "hPa"
        self.anzeige_einheit = "mbar"
        self.meas_thread     = None
        self.signals         = MeasSignals()
        self.logging_on      = False
        self.csv_file        = None
        self.csv_writer      = None
        self.log_date        = None
        self.plot_style      = self._cfg.get("plot_style", PLOT_STYLES[0])
        self.log_yscale      = self._cfg.get("yscale", "log")
        self.vergleiche:     list[VergleichsDatei] = []
        self._vgl_win        = None

        # Adaptiv-Modus
        self._adaptiv_aktiv   = False
        self._adaptiv_filter  = AdaptivFilter(
            schwelle_pct  = self._cfg.get("adaptiv_schwelle",  0.5),
            max_wartezeit_s = self._cfg.get("adaptiv_wartezeit", 60.0),
        )
        self._adaptiv_mess_iv = self._cfg.get("adaptiv_mess_iv", 1.0)

        # Plot-Sichtfenster: Tage (1–7), begrenzt RAM-Verbrauch
        try:
            self._plot_tage = max(1, min(7, int(self._cfg.get("plot_tage", 1))))
        except (ValueError, TypeError):
            self._plot_tage = 1
        max_punkte = self._plot_tage * PUNKTE_PRO_TAG

        # Datenpuffer: matplotlib date numbers (float)
        self.ts_puffer  = deque(maxlen=max_punkte)
        self.wertpuffer = {ch: deque(maxlen=max_punkte) for ch in CHANNELS}

        # Archiv-Cache: Vortage aus CSV nachladen
        self._archive = ArchiveCache(CHANNELS)

        self.settings = QSettings(SETTINGS_ORG, SETTINGS_APP)

        # Statusbar-Prioritätssystem
        # Priorität: 0=Info (grün), 1=Warnung (gelb), 2=Fehler (rot)
        self._sb_timer = QTimer()
        self._sb_timer.setSingleShot(True)
        self._sb_timer.timeout.connect(self._sb_clear)
        self._sb_priority = -1

        self.signals.new_data.connect(self._on_new_data)
        self.signals.save_data.connect(self._on_save_data)
        self.signals.connected.connect(self._on_connected)
        self.signals.reconnecting.connect(self._on_reconnecting)
        self.signals.error.connect(self._on_error)

        self._theme_name = self._cfg.get("theme", "dark")
        self._apply_theme(self._theme_name)
        self._build_ui()

        # ── Tastaturkürzel ────────────────────────────────────────────────────
        QShortcut(QKeySequence("Space"), self).activated.connect(self._toggle_messung)
        QShortcut(QKeySequence("L"),     self).activated.connect(
            lambda: self.btn_log.click()
        )
        QShortcut(QKeySequence("T"),     self).activated.connect(self._toggle_theme)
        QShortcut(QKeySequence("R"),     self).activated.connect(
            lambda: self.canvas.figure.gca().autoscale() or self.canvas.draw_idle()
            if hasattr(self, 'canvas') else None
        )
        QShortcut(QKeySequence("A"),     self).activated.connect(
            lambda: self.btn_adaptiv.click()
        )

        # Config-Werte in UI übernehmen
        self._apply_cfg_to_ui()

        # Fensterposition/-größe wiederherstellen
        geom = self.settings.value("window_geometry")
        if geom:
            self.restoreGeometry(geom)

        self._clock_timer = QTimer()
        self._clock_timer.timeout.connect(self._update_clock)
        self._clock_timer.start(500)

    # ── Theme ──────────────────────────────────────────────────────────────────

    # Theme-Definitionen: Qt-Stylesheet-Variablen + matplotlib-Farben
    THEMES = {
        "dark": {
            "bg":        "#1a1a2e",
            "card":      "#0f0f1a",
            "border":    "#333",
            "text":      "#e0e0e0",
            "text_sec":  "#aaa",
            "log_bg":    "#0a0a14",
            "log_text":  "#8b8fa8",
            "tb_bg":     "#1e2235",
            "tb_btn":    "#252a3d",
            "tb_border": "#3a4060",
            "mpl_bg":    "#0f0f1a",
            "mpl_fg":    "#cccccc",
            "mpl_grid":  "#2a2a3a",
            "accent":    "#4f8ef7",
        },
        "light": {
            # VS Code Light+ / Material Light inspired
            "bg":        "#f3f4f8",
            "card":      "#ffffff",
            "border":    "#d0d3de",
            "text":      "#1e2030",
            "text_sec":  "#6b7280",
            "log_bg":    "#ffffff",
            "log_text":  "#374151",
            "tb_bg":     "#e8eaf2",
            "tb_btn":    "#dde1f0",
            "tb_border": "#b8bdd4",
            "mpl_bg":    "#ffffff",
            "mpl_fg":    "#1e2030",
            "mpl_grid":  "#e2e4ef",
            "accent":    "#2563eb",
        },
    }

    def _apply_theme(self, name: str = "dark"):
        t = self.THEMES.get(name, self.THEMES["dark"])
        self._theme_name = name
        accent = t['accent']
        self.setStyleSheet(f"""
            QMainWindow, QWidget {{ background-color: {t['bg']}; color: {t['text']}; }}
            QGroupBox {{
                border: 1px solid {t['border']}; border-radius: 6px;
                margin-top: 10px; color: {t['text_sec']};
            }}
            QGroupBox::title {{ subcontrol-origin: margin; left: 10px; padding: 0 4px; }}
            QLabel {{ color: {t['text']}; }}
            QLineEdit, QComboBox {{
                background: {t['card']}; border: 1px solid {t['border']};
                border-radius: 4px; color: {t['text']}; padding: 3px 6px;
            }}
            QLineEdit:focus, QComboBox:focus {{
                border-color: {accent};
            }}
            QListWidget {{
                background: {t['card']}; border: 1px solid {t['border']};
                color: {t['text']}; border-radius: 4px;
            }}
            QTextEdit {{
                background: {t['log_bg']}; border: 1px solid {t['border']};
                color: {t['log_text']}; font-family: Consolas; font-size: 11px;
                border-radius: 4px;
            }}
            QPushButton {{
                background: {t['tb_btn']}; border: 1px solid {t['border']};
                border-radius: 5px; color: {t['text']}; padding: 5px 14px;
            }}
            QPushButton:hover   {{ background: {t['tb_border']}; border-color: {accent}; }}
            QPushButton:pressed {{ background: {accent}; color: #ffffff; border-color: {accent}; }}
            QCheckBox {{ color: {t['text']}; }}
            QCheckBox::indicator {{
                border: 1px solid {t['border']}; border-radius: 3px;
                background: {t['card']}; width: 13px; height: 13px;
            }}
            QCheckBox::indicator:checked {{
                background: {accent}; border-color: {accent};
            }}
            QStatusBar {{ color: {t['text_sec']}; font-size: 11px; background: {t['bg']}; }}
            QSplitter::handle {{ background: {t['border']}; }}
            QSlider::groove:horizontal {{
                background: {t['border']}; height: 4px; border-radius: 2px;
            }}
            QSlider::handle:horizontal {{
                background: {accent}; width: 14px; height: 14px;
                margin: -5px 0; border-radius: 7px;
            }}
            QToolBar {{
                background: {t['tb_bg']}; border-bottom: 1px solid {t['border']};
                spacing: 3px; padding: 2px;
            }}
            QToolButton {{
                background: {t['tb_btn']}; border: 1px solid {t['tb_border']};
                border-radius: 5px; color: {t['text']};
                padding: 4px 6px; min-width: 28px; min-height: 28px;
            }}
            QToolButton:hover   {{ background: {t['tb_border']}; border-color: {accent}; }}
            QToolButton:checked {{ background: {accent}; border-color: {accent}; color: #fff; }}
            QToolButton:pressed {{ background: {accent}; color: #fff; }}
        """)

        # matplotlib Farben aktualisieren
        mpl_bg   = t['mpl_bg']
        mpl_fg   = t['mpl_fg']
        mpl_grid = t['mpl_grid']
        plt.rcParams.update({
            "text.color":        mpl_fg,
            "axes.labelcolor":   mpl_fg,
            "xtick.color":       mpl_fg,
            "ytick.color":       mpl_fg,
            "xtick.labelcolor":  mpl_fg,
            "ytick.labelcolor":  mpl_fg,
            "axes.edgecolor":    mpl_grid,
            "figure.facecolor":  mpl_bg,
            "axes.facecolor":    mpl_bg,
            "savefig.facecolor": mpl_bg,
        })
        # Wenn Plot bereits aufgebaut: Farben live aktualisieren
        if hasattr(self, 'fig'):
            self.fig.patch.set_facecolor(mpl_bg)
            self.ax.set_facecolor(mpl_bg)
            for spine in self.ax.spines.values():
                spine.set_color(mpl_grid)
            self.ax.xaxis.label.set_color(mpl_fg)
            self.ax.yaxis.label.set_color(mpl_fg)
            self.ax.yaxis.set_tick_params(labelcolor=mpl_fg, color=mpl_fg)
            self.ax.xaxis.set_tick_params(labelcolor=mpl_fg, color=mpl_fg)
            self.ax.grid(True, color=mpl_grid, linestyle="--",
                         linewidth=0.5, alpha=0.6)
            self.ax.legend(
                facecolor=t['bg'], edgecolor=t['border'], labelcolor=mpl_fg,
                loc="upper left", bbox_to_anchor=(1.01, 1), borderaxespad=0,
            )
            self.fig.subplots_adjust(right=0.82)
            # Stats-Text Farben
            if hasattr(self, '_stats_txt'):
                self._stats_txt.set_color(mpl_fg)
                self._stats_txt.get_bbox_patch().set_facecolor(t['bg'])
                self._stats_txt.get_bbox_patch().set_edgecolor(t['border'])
            self.canvas.draw_idle()

        # Btn-Styles neu setzen (da sie eigene setStyleSheet haben)
        if hasattr(self, 'btn_start'):
            if self.meas_thread and self.meas_thread._running:
                self._style_btn(self.btn_start, "red")
            else:
                self._style_btn(self.btn_start, "green")
        # KanalWidgets theme-aware aktualisieren
        if hasattr(self, 'kanal_widgets'):
            dark = (name == "dark")
            for kw in self.kanal_widgets.values():
                kw.set_theme(dark)

    # ── UI ─────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setSpacing(6)
        root.setContentsMargins(8, 8, 8, 8)

        root.addWidget(self._build_ctrl())
        root.addWidget(self._build_zeit())
        root.addLayout(self._build_kanal_row())
        root.addWidget(self._build_stats_panel())

        # Plot + Log in vertikal ziehbarem Splitter
        plot_log_splitter = QSplitter(Qt.Orientation.Vertical)

        plot_container = self._build_plot()
        plot_log_splitter.addWidget(plot_container)

        self.txt_log = QTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setToolTip(S["tt_log_fenster"])
        plot_log_splitter.addWidget(self.txt_log)

        plot_log_splitter.setSizes([520, 100])
        plot_log_splitter.setStretchFactor(0, 4)
        plot_log_splitter.setStretchFactor(1, 1)
        root.addWidget(plot_log_splitter)

    # ── Steuerleiste ──────────────────────────────────────────────────────────

    def _build_ctrl(self) -> QGroupBox:
        ctrl = QGroupBox("Steuerung")
        # Feste Höhe — zieht sich beim Vergrößern des Fensters nicht in die Länge
        ctrl.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        vl = QVBoxLayout(ctrl)
        vl.setSpacing(4)
        vl.setContentsMargins(6, 6, 6, 6)

        # ── Zeile 1: Messung / Zeitsteuerung / Plot-Stil ──────────────────────
        z1 = QHBoxLayout()
        z1.setSpacing(8)

        self.btn_start = QPushButton("▶  Messung starten")
        self.btn_start.setToolTip(S["tt_start"])
        self._style_btn(self.btn_start, "green")
        self.btn_start.clicked.connect(self._toggle_messung)

        self.spin_interval = QDoubleSpinBox()
        self.spin_interval.setRange(0.5, 300.0)
        self.spin_interval.setValue(1.0)
        self.spin_interval.setSuffix(" s")
        self.spin_interval.setSingleStep(0.5)
        self.spin_interval.setMinimumWidth(90)
        self.spin_interval.setMaximumWidth(120)
        self.spin_interval.setStyleSheet("""
            QDoubleSpinBox {
                background: #0f0f1a; border: 1px solid #444;
                border-radius: 4px; color: #e0e0e0; padding: 2px 4px;
            }
            QDoubleSpinBox:focus { border-color: #4f8ef7; }
            QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {
                background: #252a3d; border-left: 1px solid #444; width: 16px;
            }
            QDoubleSpinBox::up-button:hover, QDoubleSpinBox::down-button:hover {
                background: #3a4a7a;
            }
            QDoubleSpinBox::up-button:pressed, QDoubleSpinBox::down-button:pressed {
                background: #4f3483;
            }
        """)
        self.spin_interval.setToolTip(S["tt_intervall"])
        self.spin_interval.valueChanged.connect(self._on_interval_changed)

        self.spin_zeitfenster = QSpinBox()
        self.spin_zeitfenster.setRange(0, 1440)
        self.spin_zeitfenster.setValue(0)
        self.spin_zeitfenster.setSuffix(" min")
        self.spin_zeitfenster.setFixedWidth(80)
        self.spin_zeitfenster.setSpecialValueText("Alle")
        self.spin_zeitfenster.setToolTip(S["tt_fenster"])

        self.cmb_style = QComboBox()
        self.cmb_style.addItems(PLOT_STYLES)
        self.cmb_style.setFixedWidth(145)
        self.cmb_style.setToolTip(S["tt_yscale"])
        self.cmb_style.currentTextChanged.connect(self._on_style_changed)

        self.btn_logscale = QPushButton("Y: Log")
        self.btn_logscale.setCheckable(True)
        self.btn_logscale.setChecked(True)
        self.btn_logscale.setFixedWidth(72)
        self.btn_logscale.setToolTip(S["tt_yscale"])
        self.btn_logscale.setStyleSheet("""
            QPushButton {
                background: #0a2a3d; border: 1px solid #00C8FF;
                color: #00C8FF; font-weight: bold;
                padding: 5px 8px; border-radius: 5px;
            }
            QPushButton:!checked {
                background: #1a1a2e; border: 1px solid #555; color: #888;
            }
        """)
        self.btn_logscale.toggled.connect(self._on_logscale_toggled)

        self.cmb_einheit = QComboBox()
        self.cmb_einheit.addItems(["mbar", "hPa", "Pa", "Torr", "Micron"])
        self.cmb_einheit.setFixedWidth(80)
        self.cmb_einheit.setToolTip(S["tt_einheit"])
        self.cmb_einheit.currentTextChanged.connect(self._on_einheit_changed)

        z1.addWidget(self.btn_start)
        z1.addWidget(QLabel("Intervall:"))
        z1.addWidget(self.spin_interval)
        z1.addSpacing(8)
        # Adaptiv-Modus
        self.btn_adaptiv = QPushButton("⚙ Adaptiv")
        self.btn_adaptiv.setCheckable(True)
        self.btn_adaptiv.setToolTip(
            "Adaptiv-Modus: Gerät wird im 1s-Takt abgefragt,\n"
            "gespeichert/geplottet nur bei Wertänderung oder nach Ablauf\n"
            "der maximalen Wartezeit. Einstellungen per Rechtsklick."
        )
        self.btn_adaptiv.clicked.connect(self._toggle_adaptiv)
        self.btn_adaptiv.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.btn_adaptiv.customContextMenuRequested.connect(
            lambda _: self._open_adaptiv_dialog()
        )
        z1.addWidget(self.btn_adaptiv)
        z1.addSpacing(8)
        z1.addWidget(QLabel("Fenster:"))
        z1.addWidget(self.spin_zeitfenster)
        z1.addSpacing(8)
        z1.addWidget(QLabel("Plot-Stil:"))
        z1.addWidget(self.cmb_style)
        z1.addWidget(self.btn_logscale)
        z1.addSpacing(8)
        z1.addWidget(QLabel("Einheit:"))
        z1.addWidget(self.cmb_einheit)
        z1.addStretch()
        vl.addLayout(z1)

        # ── Zeile 2: Logging / Ordner / Tools ────────────────────────────────
        z2 = QHBoxLayout()
        z2.setSpacing(8)

        self.btn_log = QPushButton("● CSV Logging")
        self.btn_log.setCheckable(True)
        self.btn_log.setToolTip(S["tt_logging"])
        self._style_btn(self.btn_log, "red")
        self.btn_log.clicked.connect(self._toggle_logging)

        default_pfad = self.settings.value(
            "log_folder", os.path.expanduser("~\\Desktop")
        )
        self.edit_pfad = QLineEdit(default_pfad)
        self.edit_pfad.setMinimumWidth(220)
        self.edit_pfad.setToolTip(S["tt_ordner"])
        self.edit_pfad.textChanged.connect(
            lambda t: self.settings.setValue("log_folder", t)
        )
        btn_browse = QPushButton("…")
        btn_browse.setFixedWidth(32)
        btn_browse.setToolTip(S["tt_browse"])
        btn_browse.clicked.connect(self._browse_pfad)

        self.btn_vgl_fenster = QPushButton("⊞ Vergleich")
        self.btn_vgl_fenster.setToolTip(S["tt_vgl"])
        self.btn_vgl_fenster.clicked.connect(self._open_vgl_fenster)

        btn_pdf = QPushButton("↓ PDF")
        btn_pdf.setToolTip(S["tt_pdf"])
        btn_pdf.clicked.connect(self._export_pdf)

        self.btn_theme = QPushButton("☀ Hell")
        self.btn_theme.setFixedWidth(75)
        self.btn_theme.setToolTip(S["tt_theme"])
        self.btn_theme.clicked.connect(self._toggle_theme)

        self.chk_autostart = QCheckBox("Auto-Start")
        self.chk_autostart.setToolTip(S["tt_autostart"])

        btn_about = QPushButton("ⓘ")
        btn_about.setFixedWidth(32)
        btn_about.setToolTip(S["tt_about"])
        btn_about.clicked.connect(self._show_about)

        z2.addWidget(self.btn_log)
        z2.addWidget(QLabel("Ordner:"))
        z2.addWidget(self.edit_pfad)
        z2.addWidget(btn_browse)
        z2.addSpacing(8)
        z2.addWidget(self.btn_vgl_fenster)
        z2.addWidget(btn_pdf)
        z2.addSpacing(8)
        lbl_tage = QLabel("Plot-Tage:")
        lbl_tage.setToolTip(
            "Maximale sichtbare Tage im Plot (1–7)\n"
            "Begrenzt auch den RAM-Verbrauch bei Langzeitmessungen.\n"
            "Wirkt wenn der Minuten-Spinner auf 'Alle' steht."
        )
        z2.addWidget(lbl_tage)
        self.spin_plot_tage = QSpinBox()
        self.spin_plot_tage.setRange(1, 7)
        self.spin_plot_tage.setValue(self._plot_tage)
        self.spin_plot_tage.setSuffix(" d")
        self.spin_plot_tage.setFixedWidth(60)
        self.spin_plot_tage.setToolTip(lbl_tage.toolTip())
        self.spin_plot_tage.valueChanged.connect(self._on_plot_tage_changed)
        z2.addWidget(self.spin_plot_tage)
        z2.addSpacing(8)
        z2.addWidget(self.btn_theme)
        z2.addWidget(self.chk_autostart)
        z2.addWidget(btn_about)
        z2.addStretch()
        vl.addLayout(z2)

        return ctrl

    # ── Zeitstempel ───────────────────────────────────────────────────────────

    def _build_zeit(self) -> QWidget:
        w = QWidget()
        w.setToolTip("Aktuelle Zeit: Gießen (CET/CEST), UTC, Kalenderwoche und MJD")
        w.setStyleSheet(
            "background: #0f1117; border-radius: 6px; border: 1px solid #2e3247;"
        )
        hl = QHBoxLayout(w)
        hl.setContentsMargins(14, 6, 14, 6)
        hl.setSpacing(0)

        self.lbl_clock_main = QLabel("–")
        self.lbl_clock_main.setStyleSheet(
            "font-size: 17px; font-weight: 700; font-family: Consolas; "
            "color: #4f8ef7; background: transparent; border: none;"
        )
        self.lbl_clock_sub = QLabel("–")
        self.lbl_clock_sub.setStyleSheet(
            "font-size: 14px; font-weight: 600; font-family: Consolas; "
            "color: #c0c4d8; background: transparent; border: none;"
        )

        vl = QVBoxLayout()
        vl.setSpacing(1)
        vl.addWidget(self.lbl_clock_main)
        vl.addWidget(self.lbl_clock_sub)
        hl.addLayout(vl)
        hl.addStretch()
        return w

    # ── Kanalwidgets + Alarm ──────────────────────────────────────────────────

    def _build_kanal_row(self) -> QHBoxLayout:
        row = QHBoxLayout()
        self.kanal_widgets = {}
        for ch in CHANNELS:
            kw = KanalWidget(ch, KANAL_FARBEN[ch])
            kw.setToolTip(f"Kanal {ch} – Aktueller Druckwert und Sensor-Status")
            kw.btn_sensor.setToolTip(
                f"Sensor Kanal {ch} manuell ein-/ausschalten\n"
                "(nur aktiv wenn Sensor-Steuerung auf HAND gestellt)"
            )
            kw.btn_sensor.toggled.connect(
                lambda checked, c=ch: self._toggle_sensor(c, checked)
            )
            kw.alarm_ausgeloest.connect(self._on_alarm)
            self.kanal_widgets[ch] = kw
            row.addWidget(kw)

        alarm = QGroupBox("Alarmgrenzen (mbar)")
        alarm.setToolTip("Alarmgrenzen pro Kanal\nBei Überschreitung: Kanal blinkt rot, Statusmeldung, CSV-Eintrag")
        al    = QGridLayout(alarm)
        self.alarm_spins  = {}
        self.alarm_checks = {}
        for i, ch in enumerate(CHANNELS):
            farbe = KANAL_FARBEN[ch]
            chk   = QCheckBox(f"K{ch}:")
            chk.setStyleSheet(f"color: {farbe};")
            chk.setToolTip(f"Alarmgrenze für Kanal {ch} aktivieren")
            spin  = ScientificSpinBox()
            spin.setEnabled(False)
            spin.setToolTip(
                f"Alarmgrenzwert für Kanal {ch} in mbar\n"
                "Eingabe in beliebigem Format: 1e-5, 1.5E-3, 750 …\n"
                "Pfeile: ×10 / ÷10"
            )
            chk.toggled.connect(spin.setEnabled)
            chk.toggled.connect(
                lambda checked, c=ch, s=spin: self._set_alarm(c, checked, s)
            )
            spin.valueChanged.connect(
                lambda val, c=ch, ck=chk: self._set_alarm(c, ck.isChecked(), None, val)
            )
            al.addWidget(chk,  i, 0)
            al.addWidget(spin, i, 1)
            self.alarm_spins[ch]  = spin
            self.alarm_checks[ch] = chk
        alarm.setFixedWidth(220)
        row.addWidget(alarm)
        return row

    # ── Statistik-Panel ───────────────────────────────────────────────────────

    def _build_stats_panel(self) -> QWidget:
        """Qt-Label-Leiste mit Min/Max/Ø für jeden aktiven Kanal."""
        w  = QWidget()
        hl = QHBoxLayout(w)
        hl.setContentsMargins(4, 2, 4, 2)
        hl.setSpacing(24)
        self._stats_labels = {}
        for ch in CHANNELS:
            farbe = KANAL_FARBEN[ch]
            lbl   = QLabel(f"K{ch}: –")
            lbl.setStyleSheet(
                f"color: {farbe}; font-family: Consolas; font-size: 11px;"
            )
            lbl.setToolTip(f"Kanal {ch}: Min / Max / Ø der aktuell sichtbaren Werte")
            self._stats_labels[ch] = lbl
            hl.addWidget(lbl)
        hl.addStretch()
        return w

    # ── matplotlib Plot ────────────────────────────────────────────────────────

    def _build_plot(self) -> QWidget:
        # Farben aus aktuellem Theme
        t        = self.THEMES.get(self._theme_name, self.THEMES["dark"])
        mpl_bg   = t["mpl_bg"]
        mpl_fg   = t["mpl_fg"]
        mpl_grid = t["mpl_grid"]

        # Figure
        self.fig, self.ax = plt.subplots(figsize=(10, 4))
        self.fig.patch.set_facecolor(mpl_bg)
        self.ax.set_facecolor(mpl_bg)

        # Achsen-Styling
        for spine in self.ax.spines.values():
            spine.set_color(mpl_grid)
        self.ax.tick_params(axis='both', colors=mpl_fg, labelsize=9)
        self.ax.yaxis.set_tick_params(labelcolor=mpl_fg, color=mpl_fg)
        self.ax.xaxis.set_tick_params(labelcolor=mpl_fg, color=mpl_fg)
        self.ax.xaxis.label.set_color(mpl_fg)
        self.ax.yaxis.label.set_color(mpl_fg)
        self.ax.set_ylabel("Druck (mbar)", color=mpl_fg)
        self.ax.set_xlabel("Uhrzeit (Gießen)", color=mpl_fg)
        self.ax.grid(True, color=mpl_grid, linestyle="--", linewidth=0.5, alpha=0.6)
        self.ax.set_yscale("log")

        # X-Achse: Gießen-Zeit-Formatter
        self.ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(fmt_giessen_time)
        )
        self.ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        self.fig.autofmt_xdate(rotation=0, ha="center")

        # Live-Linien anlegen
        self._mpl_lines = {}
        for ch in CHANNELS:
            line, = self.ax.plot(
                [], [], color=KANAL_FARBEN[ch],
                linewidth=1.8, label=f"Kanal {ch}"
            )
            self._mpl_lines[ch] = line

        self.ax.legend(
            facecolor=t["bg"], edgecolor=t["border"],
            labelcolor=mpl_fg, fontsize=9,
            loc="upper left",
            bbox_to_anchor=(1.01, 1),
            borderaxespad=0,
        )
        self.fig.tight_layout(pad=1.2)
        # Platz rechts für Legende reservieren
        self.fig.subplots_adjust(right=0.82)

        # Alarm-Grenzlinien (eine pro Kanal, initial unsichtbar)
        self._alarm_lines = {}
        for ch in CHANNELS:
            ln = self.ax.axhline(
                y=1e-9, color=KANAL_FARBEN[ch], linewidth=1.0,
                linestyle=":", alpha=0.7, visible=False, zorder=4
            )
            self._alarm_lines[ch] = ln

        # Statistik-Overlay im Plot ENTFERNT — wird jetzt als Qt-Labels angezeigt

        # Canvas + NavigationToolbar
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setMinimumHeight(240)
        # Pick-Event: Hover über Datenlinie → Wert + Zeit in Statusbar
        for ch in CHANNELS:
            self._mpl_lines[ch].set_picker(8)   # 8px Toleranz
        self.canvas.mpl_connect("pick_event",          self._on_pick)
        self.canvas.mpl_connect("motion_notify_event", self._on_plot_motion)

        # matplotlib Icons sind dunkle SVGs — mit hellem Toolbar-BG sichtbar machen
        # indem wir die Toolbar auf mittleres Grau setzen (Icons bleiben lesbar)
        toolbar = NavToolbar(self.canvas, None)
        toolbar.setStyleSheet("""
            QToolBar {
                background: #2a2d3e;
                border-bottom: 1px solid #3a4060;
                spacing: 3px; padding: 2px;
            }
            QToolButton {
                background: #353850; border: 1px solid #4a5080;
                border-radius: 5px; padding: 4px 6px;
                min-width: 28px; min-height: 28px;
            }
            QToolButton:hover   { background: #4a5a90; border-color: #4f8ef7; }
            QToolButton:checked { background: #5a3a90; border-color: #9f6ef7; }
            QToolButton:pressed { background: #2a1a5e; }
            QLabel { color: #c0c4d8; font-size: 11px; }
        """)

        container = QWidget()
        vl = QVBoxLayout(container)
        vl.setContentsMargins(0, 0, 0, 0)
        vl.setSpacing(0)
        vl.addWidget(toolbar)
        vl.addWidget(self.canvas)
        return container

    # ── Hilfsmethoden ─────────────────────────────────────────────────────────

    def _style_btn(self, btn, color):
        if color == "green":
            btn.setStyleSheet(
                "background:#0a3d0a; border:1px solid #2ea82e; "
                "color:#2ea82e; font-weight:bold; padding:6px 16px; border-radius:5px;"
            )
        else:
            btn.setStyleSheet(
                "background:#3d0a0a; border:1px solid #a82e2e; "
                "color:#a82e2e; font-weight:bold; padding:6px 16px; border-radius:5px;"
            )

    def _status(self, msg: str, prio: int = 0, dauer_ms: int = 8000):
        """
        Zeigt eine Statusbar-Meldung mit Priorität und Farbkodierung.
        prio 0 = Info (grün), 1 = Warnung (gelb), 2 = Fehler (rot)
        Niedrigere Priorität überschreibt keine höhere solange diese aktiv ist.
        """
        if prio < self._sb_priority:
            return
        self._sb_priority = prio
        farben = {0: "#2ea82e", 1: "#FFB300", 2: "#FF4444"}
        self.statusBar().setStyleSheet(
            f"QStatusBar {{ color: {farben.get(prio, '#e0e0e0')}; font-size: 11px; }}"
        )
        self.statusBar().showMessage(msg)
        self._sb_timer.start(dauer_ms)

    def _sb_clear(self):
        self._sb_priority = -1
        self.statusBar().setStyleSheet("QStatusBar { color: #888; font-size: 11px; }")
        self.statusBar().clearMessage()

    def _log(self, msg: str):
        """Schreibt einen Eintrag mit Zeitstempel ins Log-Fenster."""
        ts = datetime.now(giessen_tz()).strftime("%H:%M:%S")
        self.txt_log.append(f"[{ts}]  {msg}")
        # Automatisch ans Ende scrollen
        sb = self.txt_log.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _update_clock(self):
        WOCHENTAGE = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]
        now_utc = datetime_utc_now()
        now_loc = now_utc.astimezone(giessen_tz())
        tz_name = "CEST" if now_loc.utcoffset() == timedelta(hours=2) else "CET"
        kw      = now_loc.isocalendar()[1]
        wd      = WOCHENTAGE[now_loc.weekday()]
        zeile1  = (
            f"{wd}  {now_loc.strftime('%d.%m.%Y')}  "
            f"{now_loc.strftime('%H:%M:%S')} {tz_name}"
        )
        zeile2  = (
            f"KW {kw:02d}   "
            f"UTC {now_utc.strftime('%H:%M:%S')}   "
            f"MJD {to_mjd(now_utc):.6f}"
        )
        self.lbl_clock_main.setText(zeile1)
        self.lbl_clock_sub.setText(zeile2)
        self._update_title()

    def _update_title(self):
        """Fenster-Titel mit aktuellem Status-Indikator."""
        # Messung
        if self.meas_thread and self.meas_thread._running:
            mess_status = "● Messung"
            if self._adaptiv_aktiv:
                mess_status += " [Adaptiv]"
        else:
            mess_status = "○ Gestoppt"

        # Logging
        log_status = "▪ CSV" if self.logging_on else ""

        # Aktuellster Kanalwert (nur erster aktiver Kanal)
        kanal_info = ""
        for ch in CHANNELS:
            kw = self.kanal_widgets.get(ch)
            if kw and kw.lbl_wert.text() not in ("---", "OR", "UR",
                                                   "Fehler", "kein Sensor",
                                                   "AUS", "Ident.", "–"):
                kanal_info = f"K{ch}: {kw.lbl_wert.text()} {self.anzeige_einheit}"
                break

        teile = ["TPG 366", mess_status]
        if log_status:
            teile.append(log_status)
        if kanal_info:
            teile.append(kanal_info)
        self.setWindowTitle("  |  ".join(teile))

    # ── Konfiguration ──────────────────────────────────────────────────────────

    def _apply_cfg_to_ui(self):
        """Überträgt geladene Config-Werte in die UI-Widgets."""
        # Intervall (defensiv: ungültige Typen → Default)
        try:
            iv = float(self._cfg.get("interval", 1.0))
        except (ValueError, TypeError):
            iv = 1.0
        self.spin_interval.blockSignals(True)
        self.spin_interval.setValue(iv)
        self.spin_interval.blockSignals(False)

        # Plot-Stil
        style = self._cfg.get("plot_style", PLOT_STYLES[0])
        idx = self.cmb_style.findText(style)
        if idx >= 0:
            self.cmb_style.setCurrentIndex(idx)

        # Y-Skala
        log = self._cfg.get("yscale", "log") == "log"
        self.btn_logscale.setChecked(log)

        # Log-Ordner (Config hat Vorrang vor QSettings)
        folder = self._cfg.get("log_folder", "").strip()
        if folder:
            self.edit_pfad.setText(folder)

        # Alarmgrenzen (defensiv: alarm-Config könnte None oder kein Dict sein)
        alarm_cfg = self._cfg.get("alarm", {})
        if not isinstance(alarm_cfg, dict):
            alarm_cfg = {}
        for ch in CHANNELS:
            a = alarm_cfg.get(str(ch), {})
            if not isinstance(a, dict):
                continue
            if a.get("aktiv", False):
                self.alarm_checks[ch].setChecked(True)
                try:
                    self.alarm_spins[ch].setValue(float(a.get("grenze", 1000.0)))
                except (ValueError, TypeError):
                    pass

        # Auto-Start
        self.chk_autostart.setChecked(self._cfg.get("auto_start", False))

        # Anzeigeeinheit
        einheit = self._cfg.get("anzeige_einheit", "mbar")
        idx = self.cmb_einheit.findText(einheit)
        if idx >= 0:
            self.cmb_einheit.setCurrentIndex(idx)

        # Plot-Tage
        if hasattr(self, 'spin_plot_tage'):
            self.spin_plot_tage.setValue(self._plot_tage)

        # Theme-Button-Label aktualisieren
        self.btn_theme.setText(
            "☀ Hell" if self._theme_name == "dark" else "🌙 Dunkel"
        )

    def _cfg_snapshot(self) -> dict:
        """Liest aktuelle UI-Werte und gibt Config-Dict zurück."""
        alarm = {}
        for ch in CHANNELS:
            alarm[str(ch)] = {
                "aktiv":  self.alarm_checks[ch].isChecked(),
                "grenze": self.alarm_spins[ch].value(),
            }
        return {
            "host":               HOST,
            "port":               PORT,
            "timeout":            TIMEOUT,
            "channels":           CHANNELS,
            "log_folder":         self.edit_pfad.text().strip(),
            "interval":           self.spin_interval.value(),
            "plot_style":         self.cmb_style.currentText(),
            "yscale":             "log" if self.btn_logscale.isChecked() else "linear",
            "auto_start":         self.chk_autostart.isChecked(),
            "theme":              self._theme_name,
            "anzeige_einheit":    self.cmb_einheit.currentText(),
            "plot_tage":          self._plot_tage,
            "adaptiv_schwelle":   self._adaptiv_filter.schwelle_pct,
            "adaptiv_wartezeit":  self._adaptiv_filter.max_wartezeit,
            "adaptiv_mess_iv":    self._adaptiv_mess_iv,
            "alarm":              alarm,
        }

    def _on_reconnecting(self, versuch: int):
        if not self.meas_thread or not self.meas_thread._running:
            return
        msg = (
            f"⚠ Verbindung verloren — Reconnect-Versuch {versuch} "
            f"in {RECONNECT_INTERVAL} s …"
        )
        self._log(msg)
        self._status(msg, prio=2)

    # ── Einheitenumrechnung ────────────────────────────────────────────────────

    def _mbar_zu_anzeige(self, wert_mbar):
        """Rechnet einen mbar-Wert in die gewählte Anzeigeeinheit um."""
        return mbar_zu_anzeige(wert_mbar, self.anzeige_einheit)

    def _on_plot_tage_changed(self, tage: int):
        self._plot_tage = tage
        max_punkte = tage * PUNKTE_PRO_TAG
        # Deques mit neuer Maximalgröße erzeugen, alte Daten übernehmen
        old_ts = list(self.ts_puffer)
        self.ts_puffer = deque(old_ts, maxlen=max_punkte)
        for ch in CHANNELS:
            old_w = list(self.wertpuffer[ch])
            self.wertpuffer[ch] = deque(old_w, maxlen=max_punkte)
        # Archiv-Cache invalidieren (andere Tage nötig)
        self._archive.clear()
        self._log(f"Plot-Sichtfenster geändert: {tage} Tag(e)")

    def _on_interval_changed(self, v: float):
        self._log(f"Messintervall geändert: {v} s")
        if self.meas_thread and self.meas_thread._running:
            self.meas_thread.interval = v

    def _toggle_adaptiv(self, checked: bool):
        self._adaptiv_aktiv = checked
        if checked:
            if self.meas_thread and self.meas_thread._running:
                self.meas_thread.interval = self._adaptiv_mess_iv
            self._adaptiv_filter.reset()
            self.btn_adaptiv.setStyleSheet("""
                QPushButton {
                    background: #0a3d2a; border: 1px solid #2ea87a;
                    color: #2ea87a; font-weight: bold;
                    padding: 5px 8px; border-radius: 5px;
                }
            """)
            self._log(
                f"Adaptiv-Modus EIN  "
                f"(Schwelle {self._adaptiv_filter.schwelle_pct}%  "
                f"Wartezeit {self._adaptiv_filter.max_wartezeit} s  "
                f"Mess-IV {self._adaptiv_mess_iv} s)"
            )
            self._status("Adaptiv-Modus aktiv", prio=0)
        else:
            if self.meas_thread and self.meas_thread._running:
                self.meas_thread.interval = self.spin_interval.value()
            self.btn_adaptiv.setStyleSheet("")
            self._log("Adaptiv-Modus AUS")
            self._status("Adaptiv-Modus deaktiviert", prio=0)

    def _open_adaptiv_dialog(self):
        dlg = AdaptivDialog(
            schwelle_pct   = self._adaptiv_filter.schwelle_pct,
            max_wartezeit  = self._adaptiv_filter.max_wartezeit,
            mess_intervall = self._adaptiv_mess_iv,
            parent         = self
        )
        if dlg.exec() == QDialog.DialogCode.Accepted:
            schwelle, wartezeit, mess_iv = dlg.werte()
            self._adaptiv_filter.schwelle_pct  = schwelle
            self._adaptiv_filter.max_wartezeit = wartezeit
            self._adaptiv_mess_iv              = mess_iv
            self._adaptiv_filter.reset()
            if self._adaptiv_aktiv and self.meas_thread and self.meas_thread._running:
                self.meas_thread.interval = mess_iv
            self._log(
                f"Adaptiv-Einstellungen: Schwelle {schwelle}%  "
                f"Wartezeit {wartezeit} s  Mess-IV {mess_iv} s"
            )

    def _on_einheit_changed(self, einheit: str):
        self.anzeige_einheit = einheit
        self.ax.set_ylabel(f"Druck ({einheit})", color=self.THEMES[self._theme_name]['mpl_fg'])
        self._log(f"Anzeigeeinheit geändert: {einheit}")
        # Alarmlinien neu skalieren
        if hasattr(self, '_alarm_lines'):
            for ch in CHANNELS:
                kw = self.kanal_widgets[ch]
                if kw.alarm_grenze is not None:
                    v = self._mbar_zu_anzeige(kw.alarm_grenze)
                    self._alarm_lines[ch].set_ydata([v, v])
        # Alle gepufferten Werte sofort mit neuer Einheit neu zeichnen
        ts_arr = list(self.ts_puffer)
        for ch in CHANNELS:
            w_arr   = list(self.wertpuffer[ch])
            t_valid = [t for t, w in zip(ts_arr, w_arr) if w is not None]
            w_valid = [self._mbar_zu_anzeige(w) for w in w_arr if w is not None]
            self._mpl_lines[ch].set_xdata(t_valid)
            self._mpl_lines[ch].set_ydata(w_valid)
        self.ax.relim()
        self.ax.autoscale_view()
        self.canvas.draw_idle()

    def _toggle_theme(self):
        new_theme = "light" if self._theme_name == "dark" else "dark"
        self._apply_theme(new_theme)
        self.btn_theme.setText("☀ Hell" if new_theme == "dark" else "🌙 Dunkel")
        self._log(f"Theme gewechselt: {new_theme}")

    # ── PDF-Export ─────────────────────────────────────────────────────────────

    def _export_pdf(self):
        pfad, _ = QFileDialog.getSaveFileName(
            self, "Plot als PDF speichern",
            os.path.join(self.edit_pfad.text().strip(),
                         datetime.now(giessen_tz()).strftime("%Y-%m-%d_%H-%M-%S") + "_plot.pdf"),
            "PDF-Dateien (*.pdf);;Alle Dateien (*)"
        )
        if not pfad:
            return
        try:
            self.fig.savefig(pfad, format="pdf",
                             facecolor=self.THEMES[self._theme_name]['mpl_bg'],
                             edgecolor="none",
                             bbox_inches="tight")
            self._log(f"Plot exportiert → {pfad}")
            self._status(f"PDF gespeichert: {pfad}", prio=0)
        except Exception as e:
            self._log(f"⚠ PDF-Export fehlgeschlagen: {e}")

    # ── About-Dialog ───────────────────────────────────────────────────────────

    def _show_about(self):
        from datetime import date as _date
        dlg = AboutDialog(self)
        dlg.exec()

    def _update_plot_style(self):
        """Setzt Linien-/Scatter-Stil aller Live-Kurven."""
        style = self.cmb_style.currentText()
        for ch in CHANNELS:
            line = self._mpl_lines[ch]
            if style == "Linie":
                line.set_linestyle("-")
                line.set_marker("None")
            elif style == "Scatter":
                line.set_linestyle("None")
                line.set_marker("o")
                line.set_markersize(4)
            else:  # Linie + Scatter
                line.set_linestyle("-")
                line.set_marker("o")
                line.set_markersize(4)

    # ── Slots: Messung ─────────────────────────────────────────────────────────

    def _toggle_messung(self):
        if getattr(self, '_closing', False):
            return
        if self.meas_thread and self.meas_thread._running:
            self.meas_thread.stop()
            self.meas_thread = None
            self._style_btn(self.btn_start, "green")
            self.btn_start.setText("▶  Messung starten")
            self._stop_logging()
            self._log("Messung gestoppt.")
            self._status("Messung gestoppt.", prio=0)
        else:
            # Alten Thread sicherheitshalber aufräumen
            if self.meas_thread is not None:
                self.meas_thread.stop()
                self.meas_thread = None
            self.ts_puffer.clear()
            for ch in CHANNELS:
                self.wertpuffer[ch].clear()
            iv = self._adaptiv_mess_iv if self._adaptiv_aktiv else self.spin_interval.value()
            self.meas_thread = MeasThread(iv, self.signals, self.dev_cfg)
            self.meas_thread._running = True  # Vor start() setzen → Race mit stop() vermeiden
            self.meas_thread.start()
            self._style_btn(self.btn_start, "red")
            self.btn_start.setText("■  Messung stoppen")
            msg = f"Messung gestartet  (Intervall {iv} s)"
            self._log(msg)
            self._status(msg, prio=0)

    def _on_connected(self, einheit: str):
        if not self.meas_thread or not self.meas_thread._running:
            return
        self.einheit = einheit
        msg = f"Verbunden mit {HOST}:{PORT}  |  Einheit: {einheit}"
        self._log(msg)
        self._status(msg, prio=0)

    def _on_new_data(self, data: dict, ts_utc: datetime):
        """Empfängt jeden Rohwert (1s-Takt). Entscheidet ob gespeichert wird."""
        if not self.meas_thread or not self.meas_thread._running:
            return  # Stale signal nach Stop ignorieren
        ts_local = ts_utc.astimezone(giessen_tz())
        ts_mpl   = mdates.date2num(ts_utc)

        # Kanalwidgets immer aktualisieren (Live-Anzeige)
        for ch in CHANNELS:
            code, wert_gerät = data.get(ch, ("?", None))
            wert_mbar   = zu_mbar(wert_gerät, self.einheit) if code == "0" else None
            wert_anzeige = self._mbar_zu_anzeige(wert_mbar)
            self.kanal_widgets[ch].update_display(
                code, wert_anzeige, self.anzeige_einheit, wert_mbar=wert_mbar
            )

        if self._adaptiv_aktiv:
            # Adaptiv: nur speichern wenn Filter durchlässt
            data_mbar = {}
            for ch in CHANNELS:
                code, wert_gerät = data.get(ch, ("?", None))
                wert_mbar = zu_mbar(wert_gerät, self.einheit) if code == "0" else None
                data_mbar[ch] = wert_mbar if (wert_mbar and wert_mbar > 0) else None

            if self._adaptiv_filter.pruefen(data_mbar, ts_utc.timestamp()):
                self.signals.save_data.emit(data, ts_utc)
        else:
            # Normal: jeden Wert speichern
            self.signals.save_data.emit(data, ts_utc)

    def _on_save_data(self, data: dict, ts_utc: datetime):
        """Speichert gefilterte Werte in Puffer, CSV und Plot."""
        if not self.meas_thread or not self.meas_thread._running:
            return  # Stale signal nach Stop ignorieren
        ts_local = ts_utc.astimezone(giessen_tz())
        ts_mpl   = mdates.date2num(ts_utc)
        self.ts_puffer.append(ts_mpl)
        self._verarbeite_messwerte(data, ts_utc, ts_local)
        self._aktualisiere_plot()

    def _verarbeite_messwerte(self, data: dict, ts_utc: datetime, ts_local):
        """Puffer füllen, Widgets updaten, CSV schreiben."""
        mjd = to_mjd(ts_utc)
        row_csv = [
            ts_utc.strftime("%Y-%m-%d"),
            ts_utc.strftime("%H:%M:%S"),
            ts_local.strftime("%H:%M:%S"),
            f"{mjd:.6f}",
        ]
        # Tageswechsel → neue CSV
        if self.logging_on:
            heute = ts_utc.strftime("%Y-%m-%d")
            if heute != self.log_date:
                self._stop_logging()
                self._start_logging(ts_utc)

        for ch in CHANNELS:
            code, wert_gerät = data.get(ch, ("?", None))
            wert_mbar = zu_mbar(wert_gerät, self.einheit) if code == "0" else None
            self.wertpuffer[ch].append(
                wert_mbar if (wert_mbar is not None and wert_mbar > 0) else None
            )
            row_csv.append(f"{wert_mbar:.6E}" if wert_mbar is not None else "")
            row_csv.append(SENSOR_STATUS.get(code, code))

        if self.logging_on and self.csv_writer:
            try:
                self.csv_writer.writerow(row_csv)
                self.csv_file.flush()
            except OSError as e:
                self._log(f"⚠ CSV-Schreibfehler: {e}")
                self._stop_logging()

    def _aktualisiere_plot(self):
        """Linien, Statistik und Achsen neu zeichnen."""
        ts_arr      = list(self.ts_puffer)
        fenster_min = self.spin_zeitfenster.value()

        # ── Sichtfenster auf Live-Daten anwenden ─────────────────────────
        if fenster_min > 0 and ts_arr:
            grenze    = ts_arr[-1] - fenster_min * 60.0 / 86400.0
            idx_start = next((i for i, t in enumerate(ts_arr) if t >= grenze), 0)
            ts_live   = ts_arr[idx_start:]
        else:
            ts_live   = ts_arr
            idx_start = 0

        live_werte = {}
        for ch in CHANNELS:
            w_arr = list(self.wertpuffer[ch])
            live_werte[ch] = w_arr[idx_start:]

        # ── Archiv-Vortage nachladen (nur wenn plot_tage > 1) ────────────
        ordner = self.edit_pfad.text().strip()
        heute  = datetime_utc_now().strftime("%Y-%m-%d")
        live_ts_min = ts_live[0] if ts_live else None

        arch_ts, arch_w = self._archive.get_archive_data(
            ordner, self._plot_tage, heute, live_ts_min
        )

        # ── Zusammenführen: Archiv + Live ────────────────────────────────
        for ch in CHANNELS:
            a_ts = arch_ts
            a_w  = arch_w.get(ch, [])
            l_ts = ts_live
            l_w  = live_werte[ch]

            # Archiv vor Live anhängen
            merged_t = a_ts + l_ts
            merged_w = a_w + l_w

            t_valid = [t for t, w in zip(merged_t, merged_w) if w is not None]
            w_valid = [self._mbar_zu_anzeige(w) for w in merged_w if w is not None]
            self._mpl_lines[ch].set_xdata(t_valid)
            self._mpl_lines[ch].set_ydata(w_valid)
            # Statistik → Qt-Label (nur Live-Daten für Aktualität)
            if hasattr(self, '_stats_labels'):
                w_live_valid = [self._mbar_zu_anzeige(w)
                                for w in l_w if w is not None]
                if w_live_valid:
                    mn  = min(w_live_valid)
                    mx  = max(w_live_valid)
                    avg = sum(w_live_valid) / len(w_live_valid)
                    u   = self.anzeige_einheit
                    self._stats_labels[ch].setText(
                        f"K{ch}  min {mn:.3G}  max {mx:.3G}  Ø {avg:.3G} {u}"
                    )
                else:
                    self._stats_labels[ch].setText(f"K{ch}: –")

        self.ax.relim()
        self.ax.autoscale_view()
        _fg = self.THEMES.get(self._theme_name, self.THEMES["dark"])["mpl_fg"]
        self.ax.yaxis.set_tick_params(labelcolor=_fg, color=_fg)
        self.ax.xaxis.set_tick_params(labelcolor=_fg, color=_fg)
        self.canvas.draw_idle()

    def _on_error(self, msg: str):
        if not self.meas_thread or not self.meas_thread._running:
            return
        self._log(f"⚠ {msg}")
        self._status(f"⚠  {msg}", prio=2)

    # ── Alarm ──────────────────────────────────────────────────────────────────

    def _on_alarm(self, channel: int, wert: float):
        ts_utc   = datetime_utc_now()
        ts_local = ts_utc.astimezone(giessen_tz())
        msg = (
            f"⚠ ALARM  Kanal {channel}:  {wert:.3E} mbar  "
            f"[{ts_local.strftime('%H:%M:%S')}]"
        )
        self._log(msg)
        self._status(msg, prio=2, dauer_ms=15000)
        if self.logging_on and self.csv_writer:
            try:
                mjd = to_mjd(ts_utc)
                alarm_row = [
                    ts_utc.strftime("%Y-%m-%d"),
                    ts_utc.strftime("%H:%M:%S"),
                    ts_local.strftime("%H:%M:%S"),
                    f"{mjd:.6f}",
                ]
                for ch in CHANNELS:
                    if ch == channel:
                        alarm_row.append(f"{wert:.6E}")
                        alarm_row.append("ALARM")
                    else:
                        alarm_row.append("")
                        alarm_row.append("")
                self.csv_writer.writerow(alarm_row)
                self.csv_file.flush()
            except OSError as e:
                self._log(f"⚠ CSV-Schreibfehler (Alarm): {e}")
                self._stop_logging()

    # ── Plot-Stil / Skala ──────────────────────────────────────────────────────

    def _on_style_changed(self, style: str):
        self.plot_style = style
        self._update_plot_style()
        self.canvas.draw_idle()

    def _on_logscale_toggled(self, log: bool):
        self.log_yscale = "log" if log else "linear"
        self.ax.set_yscale(self.log_yscale)
        self.btn_logscale.setText("Y: Log" if log else "Y: Lin")
        self.canvas.draw_idle()

    # ── Plot Hover / Pick ──────────────────────────────────────────────────────

    def _on_pick(self, event):
        """Pick-Event: nächsten Datenpunkt zur Mausposition anzeigen."""
        line = event.artist
        ind  = event.ind
        if not len(ind):
            return
        # Kanal anhand der Linie ermitteln
        ch = next((c for c, l in self._mpl_lines.items() if l is line), None)
        if ch is None:
            return
        # Nächsten Index nehmen
        i = ind[0]
        xdata = line.get_xdata()
        ydata = line.get_ydata()
        if i >= len(xdata) or i >= len(ydata):
            return
        try:
            dt_loc = mdates.num2date(xdata[i]).astimezone(giessen_tz())
            zeit   = dt_loc.strftime("%H:%M:%S")
        except Exception:
            zeit = "–"
        wert = ydata[i]
        self._status(
            f"K{ch}  {zeit}  →  {wert:.4G} {self.anzeige_einheit}",
            prio=0, dauer_ms=5000
        )

    def _on_plot_motion(self, event):
        """Mausbewegung im Plot: Koordinaten diskret in Statusbar."""
        if event.inaxes != self.ax or event.xdata is None:
            return
        try:
            dt_loc = mdates.num2date(event.xdata).astimezone(giessen_tz())
            zeit   = dt_loc.strftime("%H:%M:%S")
        except Exception:
            return
        y = event.ydata
        if y and y > 0:
            self._status(
                f"⊕  {zeit}   {y:.4G} {self.anzeige_einheit}",
                prio=0, dauer_ms=2000
            )

    # ── Vergleichsfenster ──────────────────────────────────────────────────────

    def _open_vgl_fenster(self):
        if self._vgl_win is None:
            self._vgl_win = VergleichsFenster(
                self.ax, self.canvas, self.vergleiche,
                self.edit_pfad.text(), self
            )
            self._vgl_win.destroyed.connect(self._on_vgl_win_closed)
        self._vgl_win.show()
        self._vgl_win.raise_()
        self._vgl_win.activateWindow()

    def _on_vgl_win_closed(self):
        self._vgl_win = None

    # ── Logging ────────────────────────────────────────────────────────────────

    def _toggle_logging(self, checked: bool):
        if getattr(self, '_closing', False):
            return
        if checked:
            self._start_logging(datetime_utc_now())
        else:
            self._stop_logging()

    def _start_logging(self, ts_utc: datetime):
        ordner = self.edit_pfad.text().strip()
        if not ordner:
            self._log("⚠ Logging-Pfad ist leer – bitte Ordner wählen.")
            self._status("⚠ Kein Logging-Ordner gewählt.", prio=1)
            self.btn_log.setChecked(False)
            self._style_btn(self.btn_log, "red")
            return
        try:
            os.makedirs(ordner, exist_ok=True)
        except OSError as e:
            self._log(f"⚠ Logging-Ordner konnte nicht erstellt werden: {e}")
            self._status(f"⚠ Logging-Ordner ungültig: {e}", prio=2)
            self.btn_log.setChecked(False)
            self._style_btn(self.btn_log, "red")
            return
        self.log_date = ts_utc.strftime("%Y-%m-%d")
        pfad  = os.path.join(ordner, f"{self.log_date}.csv")
        neu   = not os.path.exists(pfad)
        try:
            self.csv_file   = open(pfad, "a", newline="", encoding="utf-8")
        except OSError as e:
            self._log(f"⚠ CSV-Datei konnte nicht geöffnet werden: {e}")
            self._status(f"⚠ CSV-Fehler: {e}", prio=2)
            self.btn_log.setChecked(False)
            self._style_btn(self.btn_log, "red")
            return
        self.csv_writer = csv.writer(self.csv_file, delimiter=",")
        if neu:
            header = ["Datum_ISO", "Zeit_UTC", "Zeit_Giessen", "MJD"]
            for ch in CHANNELS:
                header += [f"K{ch}_mbar", f"K{ch}_Status"]
            try:
                self.csv_writer.writerow(header)
                self.csv_file.flush()
            except OSError as e:
                self._log(f"⚠ CSV-Header konnte nicht geschrieben werden: {e}")
                self._status(f"⚠ CSV-Fehler: {e}", prio=2)
                try:
                    self.csv_file.close()
                except Exception:
                    pass
                self.csv_file = None
                self.csv_writer = None
                self.btn_log.setChecked(False)
                self._style_btn(self.btn_log, "red")
                return
        self.logging_on = True
        self._style_btn(self.btn_log, "green")
        aktion = "Neue Datei" if neu else "Weiterschreiben"
        msg = f"Logging [{aktion}]  →  {pfad}"
        self._log(msg)
        self._status(msg, prio=0)

    def _stop_logging(self):
        was_on = self.logging_on
        self.logging_on = False
        if self.csv_file:
            try:
                self.csv_file.close()
            except Exception:
                pass
            self.csv_file   = None
            self.csv_writer = None
        self.btn_log.setChecked(False)
        self._style_btn(self.btn_log, "red")
        if was_on:
            self._log("Logging gestoppt.")

    def _browse_pfad(self):
        pfad = QFileDialog.getExistingDirectory(self, "Speicherordner wählen")
        if pfad:
            self.edit_pfad.setText(pfad)
            self.settings.setValue("log_folder", pfad)

    # ── Sensor / Alarm ────────────────────────────────────────────────────────

    def _toggle_sensor(self, channel: int, on: bool):
        if getattr(self, '_closing', False):
            return
        if self.meas_thread and self.meas_thread._running:
            threading.Thread(
                target=self.meas_thread.set_sensor,
                args=(channel, on), daemon=True
            ).start()
            self._log(f"Sensor K{channel} {'EIN' if on else 'AUS'} geschaltet.")
        else:
            self._status("Messung starten um Sensoren zu schalten.", prio=1)

    def _set_alarm(self, channel: int, aktiv: bool, spin=None, wert=None):
        kw = self.kanal_widgets[channel]
        if not aktiv:
            kw.alarm_grenze = None
            if hasattr(self, '_alarm_lines'):
                self._alarm_lines[channel].set_visible(False)
                self.canvas.draw_idle()
            return
        if spin is not None:
            wert = spin.value()
        if wert is None:
            wert = self.alarm_spins[channel].value()
        kw.alarm_grenze = wert
        # Alarmlinie im Plot aktualisieren (Wert ist in mbar, Anzeige konvertiert)
        if hasattr(self, '_alarm_lines'):
            linie = self._alarm_lines[channel]
            linie.set_ydata([self._mbar_zu_anzeige(wert),
                             self._mbar_zu_anzeige(wert)])
            linie.set_visible(True)
            self.canvas.draw_idle()

    # ── Close ─────────────────────────────────────────────────────────────────

    def closeEvent(self, event):
        # Reentrancy-Guard: closeEvent kann durch stop() + Event-Processing
        # theoretisch doppelt aufgerufen werden
        if getattr(self, '_closing', False):
            event.accept()
            return
        self._closing = True

        # Timer stoppen (verhindert Callbacks auf halbtoten Widgets)
        try:
            self._clock_timer.stop()
            self._sb_timer.stop()
            for kw in self.kanal_widgets.values():
                kw._blink_timer.stop()
        except Exception:
            pass

        # Messung stoppen
        try:
            if self.meas_thread:
                self.meas_thread.stop()
                self.meas_thread = None
        except Exception:
            self.meas_thread = None

        # Logging stoppen
        try:
            self._stop_logging()
        except Exception:
            # Datei notfalls direkt schließen
            if self.csv_file:
                try:
                    self.csv_file.close()
                except Exception:
                    pass
                self.csv_file = None
                self.csv_writer = None
            self.logging_on = False

        # Vergleichsfenster schließen
        try:
            if self._vgl_win:
                self._vgl_win.close()
        except Exception:
            pass

        # Konfiguration speichern
        try:
            config_speichern(self._cfg_snapshot())
        except Exception:
            pass

        # Fenstergeometrie speichern + Plot freigeben
        try:
            self.settings.setValue("window_geometry", self.saveGeometry())
            self.settings.sync()
        except Exception:
            pass
        try:
            plt.close(self.fig)
        except Exception:
            pass

        event.accept()


def clear_console():
    os.system("cls" if platform.system() == "Windows" else "clear")


def print_banner() -> None:
    if not _BANNER_AVAILABLE:
        print("TPG 366 MaxiGauge – Datenerfassung  v3")
        print("JLU Gießen – IPI")
        print("-" * 80)
        return
    colorama_init()
    GREEN = "\033[92m"
    CYAN  = "\033[96m"
    RESET = "\033[0m"
    banner   = figlet_format("TPG366 pressure", font="slant")
    subtitle = f"\nVersion {VERSION}\nJLU Gießen – IPI\n"
    print(GREEN + banner + RESET)
    print(CYAN + subtitle + RESET)
    print("-" * 80)


# ══════════════════════════════════════════════════════════════════════════════
#  EINSTIEG
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    clear_console()
    print_banner()
    app = QApplication(sys.argv)
    app.setApplicationName("TPG366 MaxiGauge")
    win = MainWindow()
    win.show()
    # Auto-Start: Messung automatisch starten wenn in Config aktiviert
    if win._cfg.get("auto_start", False):
        QTimer.singleShot(500, win._toggle_messung)
    sys.exit(app.exec() if PYQT == 6 else app.exec_())
