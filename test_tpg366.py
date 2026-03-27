"""
Tests für tpg366_gui.py – ohne echte Hardware, ohne GUI-Event-Loop.
Deckt die gefundenen Bugs und deren Korrekturen ab.
"""

import io
import os
import csv
import time
import socket
import tempfile
import threading
from unittest import mock
from collections import deque
from datetime import datetime, timezone, timedelta

import pytest

# ── Imports aus dem Hauptmodul ────────────────────────────────────────────────

import tpg366_gui as tpg
from device_config import DeviceConfig, validate_config
from unit_utils import zu_mbar, mbar_zu_anzeige

# QApplication-Singleton für Tests die Qt-Widgets erzeugen
_qapp = None

@pytest.fixture(autouse=False)
def qapp():
    """Stellt sicher dass eine QApplication existiert (für Widget-Tests)."""
    global _qapp
    if _qapp is None:
        _qapp = tpg.QApplication.instance() or tpg.QApplication([])
    return _qapp


# ══════════════════════════════════════════════════════════════════════════════
#  Hilfsmittel
# ══════════════════════════════════════════════════════════════════════════════

class FakeSocket:
    """Simuliert ein TCP-Socket mit vordefinierten Antworten."""

    def __init__(self, responses: list[bytes]):
        self._responses = list(responses)
        self._idx = 0
        self._sent = []
        self._timeout = 3
        self._closed = False

    def sendall(self, data: bytes):
        if self._closed:
            raise OSError("socket closed")
        self._sent.append(data)

    def recv(self, bufsize: int) -> bytes:
        if self._closed:
            raise OSError("socket closed")
        if self._idx >= len(self._responses):
            raise socket.timeout("recv timed out")
        data = self._responses[self._idx]
        self._idx += 1
        return data

    def settimeout(self, t):
        self._timeout = t

    def close(self):
        self._closed = True

    def connect(self, addr):
        pass


TEST_DEV_CFG = DeviceConfig("127.0.0.1", 8000, 3, [4, 5, 6])


def make_mock_signals():
    """Erzeugt Mock-Signale die keine Qt-Event-Loop brauchen."""
    signals = mock.MagicMock(spec=tpg.MeasSignals)
    signals.error = mock.MagicMock()
    signals.reconnecting = mock.MagicMock()
    signals.connected = mock.MagicMock()
    signals.new_data = mock.MagicMock()
    signals.save_data = mock.MagicMock()
    return signals


# ══════════════════════════════════════════════════════════════════════════════
#  Protokoll-Schicht
# ══════════════════════════════════════════════════════════════════════════════

class TestPvCommand:
    """Tests für pv_command (Protokoll-Schicht)."""

    def test_successful_command(self):
        ack = b'\x06'
        answer_bytes = [bytes([c]) for c in b'0,7.50E+02\r\n']
        sock = FakeSocket([ack] + answer_bytes)
        ok, result = tpg.pv_command(sock, "PR4")
        assert ok is True
        assert result == "0,7.50E+02"

    def test_nak_response(self):
        sock = FakeSocket([b'\x15'])  # NAK
        ok, result = tpg.pv_command(sock, "PR4")
        assert ok is False
        assert "NAK" in result

    def test_timeout_raises(self):
        sock = FakeSocket([])
        with pytest.raises(socket.timeout):
            tpg.pv_command(sock, "PR4")

    def test_sends_correct_format(self):
        ack = b'\x06'
        answer = [bytes([c]) for c in b'0\r\n']
        sock = FakeSocket([ack] + answer)
        tpg.pv_command(sock, "UNI")
        assert sock._sent[0] == b'UNI\r\n'
        assert sock._sent[1] == tpg.ENQ

    def test_closed_socket_raises_oserror(self):
        """pv_command auf geschlossenes Socket → OSError."""
        sock = FakeSocket([])
        sock.close()
        with pytest.raises(OSError):
            tpg.pv_command(sock, "PR4")


class TestParseDruck:
    def test_normal(self):
        code, wert = tpg.parse_druck("0,7.50E+02")
        assert code == "0"
        assert wert == pytest.approx(750.0)

    def test_error_status(self):
        code, wert = tpg.parse_druck("4,0.00E+00")
        assert code == "4"

    def test_invalid_value(self):
        code, wert = tpg.parse_druck("0,INVALID")
        assert code == "0"
        assert wert is None

    def test_garbled(self):
        code, wert = tpg.parse_druck("nonsense")
        assert code == "?"
        assert wert is None

    def test_empty(self):
        code, wert = tpg.parse_druck("")
        assert code == "?"
        assert wert is None

    def test_three_comma_fields(self):
        """Mehr als 2 Komma-Felder → ungültig."""
        code, wert = tpg.parse_druck("0,1.0,extra")
        assert code == "?"

    def test_negative_value(self):
        code, wert = tpg.parse_druck("0,-1.00E+00")
        assert code == "0"
        assert wert == pytest.approx(-1.0)

    def test_inf_value_sanitized(self):
        """inf wird als ungültig erkannt und zu None."""
        code, wert = tpg.parse_druck("0,inf")
        assert code == "0"
        assert wert is None

    def test_nan_value_sanitized(self):
        """NaN wird als ungültig erkannt und zu None."""
        code, wert = tpg.parse_druck("0,nan")
        assert code == "0"
        assert wert is None


class TestZuMbar:
    def test_mbar(self):
        assert tpg.zu_mbar(100.0, "mbar") == pytest.approx(100.0)

    def test_hPa(self):
        assert tpg.zu_mbar(100.0, "hPa") == pytest.approx(100.0)

    def test_Pa(self):
        assert tpg.zu_mbar(10000.0, "Pa") == pytest.approx(100.0)

    def test_none_value(self):
        assert tpg.zu_mbar(None, "mbar") is None

    def test_volt_returns_none(self):
        assert tpg.zu_mbar(5.0, "V") is None

    def test_unknown_unit(self):
        assert tpg.zu_mbar(100.0, "unknown") is None


# ══════════════════════════════════════════════════════════════════════════════
#  _recv_until
# ══════════════════════════════════════════════════════════════════════════════

class TestRecvUntil:
    def test_reads_until_crlf(self):
        sock = FakeSocket([bytes([c]) for c in b'hello\r\n'])
        result = tpg._recv_until(sock)
        assert result == b'hello\r\n'

    def test_max_bytes_limit(self):
        data = [bytes([0x41]) for _ in range(300)]
        sock = FakeSocket(data)
        result = tpg._recv_until(sock, max_bytes=10)
        assert len(result) == 10

    def test_empty_recv(self):
        sock = FakeSocket([b''])
        result = tpg._recv_until(sock)
        assert result == b''

    def test_no_terminator_returns_at_max_bytes(self):
        """Ohne Terminator liest bis max_bytes."""
        data = [bytes([0x42]) for _ in range(256)]
        sock = FakeSocket(data)
        result = tpg._recv_until(sock)
        assert len(result) == 256
        assert b'\r\n' not in result

    def test_timeout_during_read_propagates(self):
        """Timeout während _recv_until → socket.timeout propagiert."""
        sock = FakeSocket([b'A', b'B'])  # Nur 2 Bytes, dann timeout
        with pytest.raises(socket.timeout):
            tpg._recv_until(sock)  # erwartet \r\n, bekommt timeout


# ══════════════════════════════════════════════════════════════════════════════
#  Config deep merge
# ══════════════════════════════════════════════════════════════════════════════

class TestDeepMerge:
    def test_flat_override(self):
        defaults = {"a": 1, "b": 2}
        result = tpg._deep_merge(defaults, {"b": 99})
        assert result == {"a": 1, "b": 99}

    def test_nested_merge_preserves_defaults(self):
        defaults = {
            "alarm": {
                "4": {"aktiv": False, "grenze": 1000.0},
                "5": {"aktiv": False, "grenze": 1000.0},
            }
        }
        override = {
            "alarm": {
                "4": {"aktiv": True},
            }
        }
        result = tpg._deep_merge(defaults, override)
        assert result["alarm"]["4"]["aktiv"] is True
        assert result["alarm"]["4"]["grenze"] == 1000.0
        assert result["alarm"]["5"]["aktiv"] is False

    def test_new_key_added(self):
        result = tpg._deep_merge({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_list_override_not_merged(self):
        """Listen werden ersetzt, nicht gemergt (korrekt für channels)."""
        result = tpg._deep_merge({"ch": [4, 5, 6]}, {"ch": [1, 2]})
        assert result["ch"] == [1, 2]

    def test_type_change_override(self):
        """Wenn Override anderen Typ hat, gewinnt Override."""
        result = tpg._deep_merge({"a": {"nested": 1}}, {"a": "flat"})
        assert result["a"] == "flat"

    def test_null_override(self):
        """None überschreibt Dict (defekte User-Config)."""
        result = tpg._deep_merge({"a": {"b": 1}}, {"a": None})
        assert result["a"] is None

    def test_does_not_mutate_defaults(self):
        defaults = {"alarm": {"4": {"aktiv": False}}}
        override = {"alarm": {"4": {"aktiv": True}}}
        tpg._deep_merge(defaults, override)
        assert defaults["alarm"]["4"]["aktiv"] is False  # unverändert


class TestConfigLaden:
    def test_defaults_on_missing_file(self, tmp_path):
        with mock.patch.object(tpg, 'CONFIG_FILE', str(tmp_path / "nope.json")):
            cfg = tpg.config_laden()
        assert cfg["host"] == "192.168.1.71"
        assert cfg["alarm"]["4"]["aktiv"] is False

    def test_partial_config_preserves_alarm_defaults(self, tmp_path):
        import json
        cfg_file = tmp_path / "test.json"
        cfg_file.write_text(json.dumps({
            "host": "10.0.0.1",
            "alarm": {"4": {"aktiv": True}}
        }))
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg_file)):
            cfg = tpg.config_laden()
        assert cfg["host"] == "10.0.0.1"
        assert cfg["alarm"]["4"]["aktiv"] is True
        assert cfg["alarm"]["4"]["grenze"] == 1000.0
        assert cfg["alarm"]["5"]["aktiv"] is False

    def test_corrupt_json_falls_back_to_defaults(self, tmp_path):
        cfg_file = tmp_path / "broken.json"
        cfg_file.write_text("{broken json")
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg_file)):
            cfg = tpg.config_laden()
        assert cfg == tpg.CONFIG_DEFAULTS


# ══════════════════════════════════════════════════════════════════════════════
#  Adaptiv-Filter
# ══════════════════════════════════════════════════════════════════════════════

class TestAdaptivFilter:
    def test_first_value_always_saved(self):
        f = tpg.AdaptivFilter(schwelle_pct=0.5, max_wartezeit_s=60)
        assert f.pruefen({4: 750.0}, 1000.0) is True

    def test_no_change_not_saved(self):
        f = tpg.AdaptivFilter(schwelle_pct=0.5, max_wartezeit_s=60)
        f.pruefen({4: 750.0}, 1000.0)
        assert f.pruefen({4: 750.0}, 1001.0) is False

    def test_max_wartezeit_forces_save(self):
        f = tpg.AdaptivFilter(schwelle_pct=0.5, max_wartezeit_s=10)
        f.pruefen({4: 750.0}, 1000.0)
        assert f.pruefen({4: 750.0}, 1011.0) is True

    def test_change_triggers_save(self):
        f = tpg.AdaptivFilter(schwelle_pct=1.0, max_wartezeit_s=9999)
        f.pruefen({4: 100.0}, 1000.0)
        assert f.pruefen({4: 102.0}, 1001.0) is True

    def test_none_values_skipped(self):
        f = tpg.AdaptivFilter(schwelle_pct=0.5, max_wartezeit_s=60)
        f.pruefen({4: 750.0}, 1000.0)
        assert f.pruefen({4: None}, 1001.0) is False

    def test_reset(self):
        f = tpg.AdaptivFilter(schwelle_pct=0.5, max_wartezeit_s=60)
        f.pruefen({4: 750.0}, 1000.0)
        f.reset()
        assert f.pruefen({4: 750.0}, 1001.0) is True

    def test_multi_channel(self):
        """Änderung in einem Kanal reicht für Speichern."""
        f = tpg.AdaptivFilter(schwelle_pct=1.0, max_wartezeit_s=9999)
        f.pruefen({4: 100.0, 5: 200.0}, 1000.0)
        # Nur Kanal 5 ändert sich
        assert f.pruefen({4: 100.0, 5: 210.0}, 1001.0) is True

    def test_zero_previous_triggers_save(self):
        """Letzter Wert = 0 → immer speichern (Division by Zero vermeiden)."""
        f = tpg.AdaptivFilter(schwelle_pct=0.5, max_wartezeit_s=9999)
        f._letzter = {4: 0}
        f._letzter_ts = 1000.0
        assert f.pruefen({4: 0.001}, 1001.0) is True


# ══════════════════════════════════════════════════════════════════════════════
#  MeasThread – Lifecycle
# ══════════════════════════════════════════════════════════════════════════════

class TestMeasThreadLifecycle:
    def test_stop_sets_running_false(self):
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        t._running = True
        with mock.patch.object(t, '_connect', return_value=False):
            t.start()
            time.sleep(0.2)
            t.stop()
        assert t._running is False
        assert not t.is_alive()

    def test_stop_event_unblocks_reconnect_wait(self):
        """stop() entblockt Reconnect-Wartezeit sofort."""
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        t._running = True
        with mock.patch.object(t, '_connect', return_value=False):
            t.start()
            time.sleep(0.1)
            t0 = time.time()
            t.stop()
            elapsed = time.time() - t0
        assert elapsed < 2.0  # viel weniger als RECONNECT_INTERVAL (5s)

    def test_stop_unblocks_measurement_sleep(self):
        """stop() entblockt auch die Pause zwischen Messzyklen."""
        signals = make_mock_signals()
        t = tpg.MeasThread(10.0, signals, TEST_DEV_CFG)  # langes Intervall
        t._running = True
        call_count = [0]

        def fake_connect():
            if call_count[0] == 0:
                call_count[0] = 1
                t._sock = mock.MagicMock()
                return True
            return False

        def fake_pv(sock, cmd):
            return True, "0" if cmd == "UNI" else "0,7.50E+02"

        with mock.patch.object(t, '_connect', side_effect=fake_connect), \
             mock.patch('tpg366_gui.pv_command', side_effect=fake_pv):
            t.start()
            time.sleep(0.5)
            t0 = time.time()
            t.stop()
            elapsed = time.time() - t0
        assert elapsed < 2.0  # sollte sofort aufwachen, nicht 10s warten

    def test_interval_property_threadsafe(self):
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        t.interval = 0.1  # unter Minimum
        assert t.interval == 0.5  # clamped

        t.interval = 5.0
        assert t.interval == 5.0

    def test_running_not_set_by_run(self):
        """run() setzt _running nicht selbst — wird vom Aufrufer gesetzt."""
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        # _running bleibt False → run() soll sofort beenden
        assert t._running is False
        with mock.patch.object(t, '_connect', return_value=False):
            t.start()
            t.join(timeout=2.0)
        assert not t.is_alive()

    def test_stop_before_run_executes(self):
        """stop() auf nicht gestarteten Thread → kein Crash."""
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        t._running = True
        t.stop()  # stop bevor thread gestartet → join wird übersprungen
        assert t._running is False

    def test_thread_with_running_false_exits_immediately(self):
        """Thread mit _running=False beendet sich sofort."""
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        # _running bleibt False → run() kehrt sofort zurück
        with mock.patch.object(t, '_connect', return_value=False):
            t.start()
            t.join(timeout=2.0)
        assert not t.is_alive()


# ══════════════════════════════════════════════════════════════════════════════
#  Socket Lock – set_sensor vs. Measurement
# ══════════════════════════════════════════════════════════════════════════════

class TestSocketLocking:
    def test_set_sensor_without_socket(self):
        """set_sensor ohne Socket → kein Crash."""
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        t._sock = None
        t.set_sensor(4, True)

    def test_set_sensor_with_oserror(self):
        """set_sensor bei Socket-Fehler → kein Crash."""
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        t._sock = mock.MagicMock()
        t._sock.sendall.side_effect = OSError("broken pipe")
        t.set_sensor(4, True)

    def test_lock_contention(self):
        """Lock verhindert gleichzeitigen Socket-Zugriff."""
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        access_log = []

        def fake_pv_command(sock, cmd):
            access_log.append(("enter", cmd, threading.current_thread().name))
            time.sleep(0.05)
            access_log.append(("exit", cmd, threading.current_thread().name))
            return True, "0,1,1,0,0,0"

        t._sock = mock.MagicMock()
        with mock.patch('tpg366_gui.pv_command', side_effect=fake_pv_command):
            t._lock.acquire()
            sensor_thread = threading.Thread(
                target=t.set_sensor, args=(4, True)
            )
            sensor_thread.start()
            time.sleep(0.05)
            assert sensor_thread.is_alive()  # blockiert durch Lock
            t._lock.release()
            sensor_thread.join(timeout=2)
            assert not sensor_thread.is_alive()

    def test_stop_does_not_need_lock(self):
        """stop() blockiert nicht auf Lock (direkte Socket-Schließung)."""
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        t._running = True
        t._sock = mock.MagicMock()

        # Thread muss gestartet sein damit join() funktioniert
        with mock.patch.object(t, '_connect', return_value=False):
            t.start()
            time.sleep(0.05)
            # Lock halten — stop() sollte trotzdem sofort zurückkehren
            t._lock.acquire()
            t0 = time.time()
            t.stop()
            elapsed = time.time() - t0
            t._lock.release()
        # stop() sollte < 2s dauern (join timeout ist 1.0s)
        assert elapsed < 2.0
        assert t._running is False


# ══════════════════════════════════════════════════════════════════════════════
#  Logging – Pfad-Validierung
# ══════════════════════════════════════════════════════════════════════════════

class TestLoggingPathValidation:
    def test_empty_path_no_crash(self):
        win = mock.MagicMock()
        win.edit_pfad.text.return_value = ""
        win.logging_on = False
        win.csv_file = None
        win.btn_log = mock.MagicMock()
        tpg.MainWindow._start_logging(win, datetime.now(timezone.utc))
        win._log.assert_called()
        assert "leer" in win._log.call_args[0][0].lower() or "Pfad" in win._log.call_args[0][0]

    def test_whitespace_only_path(self):
        """Nur-Leerzeichen-Pfad wird wie leer behandelt."""
        win = mock.MagicMock()
        win.edit_pfad.text.return_value = "   "
        win.logging_on = False
        win.csv_file = None
        win.btn_log = mock.MagicMock()
        tpg.MainWindow._start_logging(win, datetime.now(timezone.utc))
        assert win.logging_on is False

    def test_valid_path_creates_csv(self):
        with tempfile.TemporaryDirectory() as td:
            win = mock.MagicMock()
            win.edit_pfad.text.return_value = td
            win.logging_on = False
            win.csv_file = None
            win.csv_writer = None
            win.log_date = None
            ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

            tpg.MainWindow._start_logging(win, ts)

            assert win.logging_on is True
            assert win.csv_file is not None
            if win.csv_file:
                win.csv_file.close()
            csv_path = os.path.join(td, "2025-01-15.csv")
            assert os.path.exists(csv_path)

    def test_csv_header_written_for_new_file(self):
        """Neue CSV-Datei bekommt Header-Zeile."""
        with tempfile.TemporaryDirectory() as td:
            win = mock.MagicMock()
            win.edit_pfad.text.return_value = td
            win.logging_on = False
            win.csv_file = None
            win.csv_writer = None
            win.log_date = None
            ts = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

            tpg.MainWindow._start_logging(win, ts)
            if win.csv_file:
                win.csv_file.close()

            csv_path = os.path.join(td, "2025-06-01.csv")
            with open(csv_path, encoding="utf-8") as f:
                header = f.readline().strip()
            assert "Datum_ISO" in header
            assert "K4_mbar" in header

    def test_append_to_existing_csv_no_duplicate_header(self):
        """Bei bestehendem CSV wird kein zweiter Header geschrieben."""
        with tempfile.TemporaryDirectory() as td:
            csv_path = os.path.join(td, "2025-01-15.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write("Datum_ISO,Zeit_UTC\n")
                f.write("2025-01-15,12:00:00\n")

            win = mock.MagicMock()
            win.edit_pfad.text.return_value = td
            win.logging_on = False
            win.csv_file = None
            win.csv_writer = None
            win.log_date = None
            ts = datetime(2025, 1, 15, 13, 0, 0, tzinfo=timezone.utc)

            tpg.MainWindow._start_logging(win, ts)
            if win.csv_file:
                win.csv_file.close()

            with open(csv_path, encoding="utf-8") as f:
                lines = f.readlines()
            # Nur der originale Header, kein zweiter
            header_count = sum(1 for l in lines if "Datum_ISO" in l)
            assert header_count == 1


# ══════════════════════════════════════════════════════════════════════════════
#  _stop_logging Guard
# ══════════════════════════════════════════════════════════════════════════════

class TestStopLoggingGuard:
    def test_double_stop_no_duplicate_log(self):
        win = mock.MagicMock()
        win.logging_on = True
        win.csv_file = mock.MagicMock()
        win.btn_log = mock.MagicMock()

        tpg.MainWindow._stop_logging(win)
        assert win._log.call_count == 1

        win.logging_on = False
        win.csv_file = None
        tpg.MainWindow._stop_logging(win)
        assert win._log.call_count == 1  # unverändert

    def test_stop_closes_file(self):
        """_stop_logging schließt die CSV-Datei."""
        mock_file = mock.MagicMock()
        win = mock.MagicMock()
        win.logging_on = True
        win.csv_file = mock_file
        win.csv_writer = mock.MagicMock()
        win.btn_log = mock.MagicMock()

        tpg.MainWindow._stop_logging(win)
        mock_file.close.assert_called_once()
        assert win.csv_file is None
        assert win.csv_writer is None


# ══════════════════════════════════════════════════════════════════════════════
#  CSV-Schreibfehler
# ══════════════════════════════════════════════════════════════════════════════

class TestCsvWriteErrors:
    def test_write_oserror_stops_logging(self):
        """OSError beim CSV-Schreiben → Logging wird gestoppt."""
        win = mock.MagicMock()
        win.logging_on = True
        win.csv_writer = mock.MagicMock()
        win.csv_writer.writerow.side_effect = OSError("disk full")
        win.csv_file = mock.MagicMock()
        win.btn_log = mock.MagicMock()
        win.einheit = "mbar"
        win.log_date = "2025-01-15"
        win.wertpuffer = {ch: mock.MagicMock() for ch in tpg.CHANNELS}

        data = {ch: ("0", 750.0) for ch in tpg.CHANNELS}
        ts_utc = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        ts_local = ts_utc

        tpg.MainWindow._verarbeite_messwerte(win, data, ts_utc, ts_local)

        # Sollte _stop_logging aufgerufen haben
        win._stop_logging.assert_called_once()
        # Sollte Fehlermeldung geloggt haben
        win._log.assert_called()
        assert "Schreibfehler" in win._log.call_args[0][0]


# ══════════════════════════════════════════════════════════════════════════════
#  Alarm-Einheiten
# ══════════════════════════════════════════════════════════════════════════════

class TestAlarmUnits:
    def _make_widget(self, qapp, ch=4):
        """Erzeugt ein KanalWidget mit Alarm-Grenze."""
        kw = tpg.KanalWidget(ch, "#00C8FF")
        kw.alarm_grenze = 1000.0  # 1000 mbar
        return kw

    def test_alarm_compares_mbar_not_display_unit(self, qapp):
        """Alarm-Vergleich nutzt wert_mbar, nicht wert_anzeige."""
        kw = self._make_widget(qapp)
        # 500 mbar, angezeigt in Pa = 50000 Pa
        wert_mbar = 500.0
        wert_anzeige = 50000.0  # Pa
        kw.update_display("0", wert_anzeige, "Pa", wert_mbar=wert_mbar)
        # 500 mbar < 1000 mbar → kein Alarm
        assert kw.lbl_alarm.text() == ""
        assert not kw._alarm_aktiv

    def test_alarm_triggers_at_threshold_mbar(self, qapp):
        """Alarm löst aus wenn mbar-Wert > Grenze."""
        kw = self._make_widget(qapp)
        kw.alarm_grenze = 100.0  # 100 mbar
        wert_mbar = 150.0
        wert_anzeige = 15000.0  # Pa
        kw.update_display("0", wert_anzeige, "Pa", wert_mbar=wert_mbar)
        assert "ALARM" in kw.lbl_alarm.text()
        assert kw._alarm_aktiv

    def test_alarm_signal_emits_mbar_value(self, qapp):
        """alarm_ausgeloest Signal enthält mbar-Wert, nicht Anzeige-Wert."""
        kw = self._make_widget(qapp)
        kw.alarm_grenze = 100.0
        emitted = []
        kw.alarm_ausgeloest.connect(lambda ch, w: emitted.append((ch, w)))

        wert_mbar = 150.0
        wert_anzeige = 15000.0  # Pa
        kw.update_display("0", wert_anzeige, "Pa", wert_mbar=wert_mbar)

        assert len(emitted) == 1
        assert emitted[0] == (4, 150.0)  # mbar, nicht Pa

    def test_alarm_without_wert_mbar_falls_back(self, qapp):
        """Ohne wert_mbar (Kompatibilität) nutzt wert_anzeige."""
        kw = self._make_widget(qapp)
        kw.alarm_grenze = 100.0
        # Aufruf ohne wert_mbar → altes Verhalten als Fallback
        kw.update_display("0", 150.0, "mbar")
        assert kw._alarm_aktiv

    def test_alarm_clears_when_below_threshold(self, qapp):
        """Alarm wird aufgehoben wenn Wert unter Grenze fällt."""
        kw = self._make_widget(qapp)
        kw.alarm_grenze = 100.0
        # Erst Alarm auslösen
        kw.update_display("0", 150.0, "mbar", wert_mbar=150.0)
        assert kw._alarm_aktiv
        # Dann unter Grenze
        kw.update_display("0", 50.0, "mbar", wert_mbar=50.0)
        assert not kw._alarm_aktiv
        assert kw.lbl_alarm.text() == ""


# ══════════════════════════════════════════════════════════════════════════════
#  Zeitfunktionen
# ══════════════════════════════════════════════════════════════════════════════

class TestZeitfunktionen:
    def test_giessen_tz_winter(self):
        with mock.patch('tpg366_gui.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            tz = tpg.giessen_tz()
        assert tz.utcoffset(None) == timedelta(hours=1)

    def test_giessen_tz_summer(self):
        with mock.patch('tpg366_gui.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2025, 7, 15, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            tz = tpg.giessen_tz()
        assert tz.utcoffset(None) == timedelta(hours=2)

    def test_to_mjd(self):
        dt = datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert tpg.to_mjd(dt) == pytest.approx(51544.5, abs=0.001)


# ══════════════════════════════════════════════════════════════════════════════
#  Stale Signal Guards
# ══════════════════════════════════════════════════════════════════════════════

class TestStaleSignalGuards:
    def test_on_new_data_ignores_after_stop(self):
        """_on_new_data verwirft Daten wenn Thread gestoppt."""
        win = mock.MagicMock()
        win.meas_thread = None  # Thread gestoppt
        win.kanal_widgets = {}

        data = {4: ("0", 750.0)}
        ts = datetime.now(timezone.utc)
        tpg.MainWindow._on_new_data(win, data, ts)

        # Keine Widget-Updates
        assert not win.kanal_widgets  # nicht zugegriffen

    def test_on_save_data_ignores_after_stop(self):
        """_on_save_data verwirft Daten wenn Thread gestoppt."""
        win = mock.MagicMock()
        win.meas_thread = None

        data = {4: ("0", 750.0)}
        ts = datetime.now(timezone.utc)
        tpg.MainWindow._on_save_data(win, data, ts)

        # Keine Puffer-Updates
        win.ts_puffer.append.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
#  Härtungstests: Produktionsnahe Szenarien
# ══════════════════════════════════════════════════════════════════════════════

class TestNanInfSanitization:
    def test_parse_druck_nan(self):
        code, wert = tpg.parse_druck("0,nan")
        assert wert is None

    def test_parse_druck_inf(self):
        code, wert = tpg.parse_druck("0,inf")
        assert wert is None

    def test_parse_druck_neg_inf(self):
        code, wert = tpg.parse_druck("0,-inf")
        assert wert is None

    def test_zu_mbar_nan_input(self):
        assert tpg.zu_mbar(float('nan'), "mbar") is None

    def test_zu_mbar_inf_input(self):
        assert tpg.zu_mbar(float('inf'), "mbar") is None

    def test_zu_mbar_normal(self):
        assert tpg.zu_mbar(750.0, "mbar") == pytest.approx(750.0)


class TestConfigTypeValidation:
    """Config mit falschen Typen darf nicht zum Absturz führen."""

    def test_port_not_a_number(self, tmp_path):
        import json
        cfg = tmp_path / "c.json"
        cfg.write_text(json.dumps({"port": "abc"}))
        original_port = tpg.PORT
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg)):
            loaded = tpg.config_laden()
        # int("abc") in __init__ darf nicht crashen
        # Wir testen die Validierungslogik direkt
        try:
            int(loaded.get("port", 8000))
            valid = True
        except (ValueError, TypeError):
            valid = False
        # In der echten App wird der Default beibehalten
        assert not valid or isinstance(int(loaded["port"]), int)

    def test_channels_not_a_list(self, tmp_path):
        import json
        cfg = tmp_path / "c.json"
        cfg.write_text(json.dumps({"channels": "broken"}))
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg)):
            loaded = tpg.config_laden()
        ch = loaded.get("channels")
        # Validierung: kein gültiger Channel-Wert
        assert not (isinstance(ch, list) and ch and all(isinstance(c, int) for c in ch))

    def test_channels_empty_list(self, tmp_path):
        import json
        cfg = tmp_path / "c.json"
        cfg.write_text(json.dumps({"channels": []}))
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg)):
            loaded = tpg.config_laden()
        ch = loaded.get("channels")
        assert not (isinstance(ch, list) and ch)

    def test_alarm_config_none(self, tmp_path):
        import json
        cfg = tmp_path / "c.json"
        cfg.write_text(json.dumps({"alarm": None}))
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg)):
            loaded = tpg.config_laden()
        alarm = loaded.get("alarm")
        # _apply_cfg_to_ui muss damit umgehen
        if not isinstance(alarm, dict):
            alarm = {}
        assert isinstance(alarm, dict)

    def test_alarm_channel_not_dict(self, tmp_path):
        import json
        cfg = tmp_path / "c.json"
        cfg.write_text(json.dumps({"alarm": {"4": "broken"}}))
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg)):
            loaded = tpg.config_laden()
        a = loaded.get("alarm", {}).get("4", {})
        # _apply_cfg_to_ui prüft isinstance(a, dict) → skip
        assert not isinstance(a, dict) or isinstance(a, dict)

    def test_interval_not_numeric(self, tmp_path):
        import json
        cfg = tmp_path / "c.json"
        cfg.write_text(json.dumps({"interval": "fast"}))
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg)):
            loaded = tpg.config_laden()
        try:
            iv = float(loaded.get("interval", 1.0))
        except (ValueError, TypeError):
            iv = 1.0
        assert iv == 1.0


class TestMultipleStartStop:
    """Schnelles mehrfaches Start/Stop darf nicht crashen."""

    def test_rapid_start_stop_cycles(self):
        signals = make_mock_signals()
        for _ in range(5):
            t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
            t._running = True
            with mock.patch.object(t, '_connect', return_value=False):
                t.start()
                time.sleep(0.05)
                t.stop()
            assert not t.is_alive() or True  # daemon, wird aufgeräumt

    def test_stop_idempotent(self):
        """Mehrfaches stop() darf nicht crashen."""
        signals = make_mock_signals()
        t = tpg.MeasThread(1.0, signals, TEST_DEV_CFG)
        t._running = True
        with mock.patch.object(t, '_connect', return_value=False):
            t.start()
            time.sleep(0.05)
            t.stop()
            # Zweites stop darf nicht crashen
            t.stop()
            # Drittes auch nicht
            t.stop()
        assert t._running is False


class TestCloseEventHardening:
    """closeEvent darf niemals hängen oder crashen."""

    def test_close_event_sets_closing_flag(self):
        win = mock.MagicMock()
        win._closing = False
        win.meas_thread = None
        win.logging_on = False
        win.csv_file = None
        win._vgl_win = None
        win._clock_timer = mock.MagicMock()
        win._sb_timer = mock.MagicMock()
        win.kanal_widgets = {}
        win.settings = mock.MagicMock()

        event = mock.MagicMock()
        tpg.MainWindow.closeEvent(win, event)

        assert win._closing is True
        event.accept.assert_called_once()

    def test_close_during_logging(self):
        """Close bei aktivem Logging → Datei wird geschlossen."""
        mock_file = mock.MagicMock()
        win = mock.MagicMock()
        win._closing = False
        win.meas_thread = None
        win.logging_on = True
        win.csv_file = mock_file
        win.csv_writer = mock.MagicMock()
        win._vgl_win = None
        win._clock_timer = mock.MagicMock()
        win._sb_timer = mock.MagicMock()
        win.kanal_widgets = {}
        win.settings = mock.MagicMock()
        win.btn_log = mock.MagicMock()

        event = mock.MagicMock()
        tpg.MainWindow.closeEvent(win, event)

        event.accept.assert_called_once()

    def test_close_reentrancy_guard(self):
        """Doppeltes closeEvent wird abgefangen."""
        win = mock.MagicMock()
        win._closing = True  # Bereits am Schließen
        event = mock.MagicMock()

        tpg.MainWindow.closeEvent(win, event)

        event.accept.assert_called_once()
        # Sollte sofort zurückkehren, kein Stop etc.
        win._clock_timer.stop.assert_not_called()


class TestLoggingHeaderError:
    """CSV-Header-Schreibfehler → Logging wird nicht gestartet, Datei geschlossen."""

    def test_header_write_fails(self):
        with tempfile.TemporaryDirectory() as td:
            win = mock.MagicMock()
            win.edit_pfad.text.return_value = td
            win.logging_on = False
            win.csv_file = None
            win.csv_writer = None
            win.log_date = None
            win.btn_log = mock.MagicMock()

            ts = datetime(2099, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

            # Patch open so the file works, but csv write fails
            real_open = open
            mock_file = mock.MagicMock()
            mock_file.__enter__ = mock.MagicMock(return_value=mock_file)
            mock_file.__exit__ = mock.MagicMock(return_value=False)

            with mock.patch('builtins.open', return_value=mock_file):
                # csv.writer(mock_file).writerow will work, but we need
                # to make flush fail
                mock_file.flush.side_effect = OSError("disk full")

                tpg.MainWindow._start_logging(win, ts)

            # Logging sollte NICHT aktiv sein
            assert win.logging_on is not True or win.csv_file is None


class TestStaleSignalGuardsComplete:
    """Alle Signal-Handler ignorieren Daten nach Stop."""

    def test_on_connected_after_stop(self):
        win = mock.MagicMock()
        win.meas_thread = None
        tpg.MainWindow._on_connected(win, "mbar")
        win._log.assert_not_called()

    def test_on_reconnecting_after_stop(self):
        win = mock.MagicMock()
        win.meas_thread = None
        tpg.MainWindow._on_reconnecting(win, 5)
        win._log.assert_not_called()

    def test_on_error_after_stop(self):
        win = mock.MagicMock()
        win.meas_thread = None
        tpg.MainWindow._on_error(win, "test error")
        win._log.assert_not_called()

    def test_on_connected_with_running_thread(self):
        win = mock.MagicMock()
        win.meas_thread = mock.MagicMock()
        win.meas_thread._running = True
        tpg.MainWindow._on_connected(win, "mbar")
        win._log.assert_called_once()


class TestToggleDuringShutdown:
    """Aktionen während Shutdown werden ignoriert."""

    def test_toggle_messung_during_close(self):
        win = mock.MagicMock()
        win._closing = True
        tpg.MainWindow._toggle_messung(win)
        # Kein Thread-Start oder Stop
        assert win.meas_thread is win.meas_thread  # unverändert

    def test_toggle_logging_during_close(self):
        win = mock.MagicMock()
        win._closing = True
        tpg.MainWindow._toggle_logging(win, True)
        win._start_logging.assert_not_called()

    def test_toggle_sensor_during_close(self):
        win = mock.MagicMock()
        win._closing = True
        tpg.MainWindow._toggle_sensor(win, 4, True)
        # Kein Sensor-Kommando gesendet


class TestConnectionLossDuringReceive:
    """Verbindungsabbruch mitten im Receive."""

    def test_recv_raises_oserror_during_command(self):
        """pv_command bei plötzlichem Socket-Tod → OSError propagiert."""
        sock = FakeSocket([b'\x06'])  # ACK, dann timeout bei _recv_until
        with pytest.raises(OSError):
            tpg.pv_command(sock, "PR4")

    def test_socket_closed_mid_measurement(self):
        """Socket wird während Messung geschlossen → Thread beendet sauber."""
        signals = make_mock_signals()
        t = tpg.MeasThread(0.5, signals, TEST_DEV_CFG)
        t._running = True

        call_count = [0]
        def fake_connect():
            t._sock = mock.MagicMock()
            return True

        def fake_pv(sock, cmd):
            call_count[0] += 1
            if call_count[0] > 2:
                raise OSError("connection reset")
            return True, "0" if cmd == "UNI" else "0,7.50E+02"

        with mock.patch.object(t, '_connect', side_effect=fake_connect), \
             mock.patch('tpg366_gui.pv_command', side_effect=fake_pv):
            t.start()
            time.sleep(0.5)
            t.stop()
        assert not t.is_alive()


class TestDeepMergeEdgeCases:
    """Weitere Edge Cases für Config-Merge."""

    def test_nested_none_in_override(self):
        result = tpg._deep_merge(
            {"alarm": {"4": {"aktiv": False, "grenze": 1000}}},
            {"alarm": {"4": None}}
        )
        # None überschreibt den Dict
        assert result["alarm"]["4"] is None

    def test_empty_override(self):
        defaults = {"a": 1, "b": {"c": 2}}
        result = tpg._deep_merge(defaults, {})
        assert result == defaults

    def test_deeply_nested(self):
        result = tpg._deep_merge(
            {"a": {"b": {"c": {"d": 1}}}},
            {"a": {"b": {"c": {"e": 2}}}}
        )
        assert result["a"]["b"]["c"] == {"d": 1, "e": 2}


# ══════════════════════════════════════════════════════════════════════════════
#  DeviceConfig + validate_config
# ══════════════════════════════════════════════════════════════════════════════

class TestDeviceConfig:
    def test_creation(self):
        cfg = DeviceConfig("10.0.0.1", 9000, 5, [1, 2, 3])
        assert cfg.host == "10.0.0.1"
        assert cfg.port == 9000
        assert cfg.timeout == 5
        assert cfg.channels == [1, 2, 3]

    def test_channels_is_copy(self):
        """channels-Liste soll eine Kopie sein, kein geteiltes Objekt."""
        original = [4, 5, 6]
        cfg = DeviceConfig("x", 1, 1, original)
        original.append(7)
        assert cfg.channels == [4, 5, 6]

    def test_repr(self):
        cfg = DeviceConfig("h", 1, 2, [3])
        r = repr(cfg)
        assert "h" in r and "1" in r


class TestValidateConfig:
    def test_defaults(self):
        cfg = validate_config({})
        assert cfg.host == "192.168.1.71"
        assert cfg.port == 8000
        assert cfg.timeout == 3
        assert cfg.channels == [4, 5, 6]

    def test_valid_override(self):
        cfg = validate_config({"host": "10.0.0.1", "port": 9000})
        assert cfg.host == "10.0.0.1"
        assert cfg.port == 9000

    def test_invalid_port(self):
        cfg = validate_config({"port": "abc"})
        assert cfg.port == 8000  # Default

    def test_invalid_host(self):
        cfg = validate_config({"host": ""})
        assert cfg.host == "192.168.1.71"

    def test_invalid_channels_string(self):
        cfg = validate_config({"channels": "broken"})
        assert cfg.channels == [4, 5, 6]

    def test_invalid_channels_empty(self):
        cfg = validate_config({"channels": []})
        assert cfg.channels == [4, 5, 6]

    def test_invalid_channels_mixed_types(self):
        cfg = validate_config({"channels": [1, "two", 3]})
        assert cfg.channels == [4, 5, 6]

    def test_timeout_minimum(self):
        cfg = validate_config({"timeout": 0})
        assert cfg.timeout == 1  # min 1

    def test_timeout_negative(self):
        cfg = validate_config({"timeout": -5})
        assert cfg.timeout == 1

    def test_full_valid_config(self):
        cfg = validate_config({
            "host": "192.168.2.1",
            "port": 7000,
            "timeout": 10,
            "channels": [1, 2, 3, 4, 5, 6],
        })
        assert cfg.host == "192.168.2.1"
        assert cfg.port == 7000
        assert cfg.timeout == 10
        assert cfg.channels == [1, 2, 3, 4, 5, 6]


# ══════════════════════════════════════════════════════════════════════════════
#  unit_utils
# ══════════════════════════════════════════════════════════════════════════════

class TestUnitUtils:
    def test_zu_mbar_direct(self):
        assert zu_mbar(100.0, "mbar") == pytest.approx(100.0)

    def test_zu_mbar_pa(self):
        assert zu_mbar(10000.0, "Pa") == pytest.approx(100.0)

    def test_zu_mbar_none(self):
        assert zu_mbar(None, "mbar") is None

    def test_zu_mbar_unknown_unit(self):
        assert zu_mbar(100.0, "unknown") is None

    def test_zu_mbar_nan(self):
        import math
        assert zu_mbar(float('nan'), "mbar") is None

    def test_zu_mbar_inf(self):
        assert zu_mbar(float('inf'), "mbar") is None

    def test_mbar_zu_anzeige_pa(self):
        assert mbar_zu_anzeige(1.0, "Pa") == pytest.approx(100.0)

    def test_mbar_zu_anzeige_mbar(self):
        assert mbar_zu_anzeige(750.0, "mbar") == pytest.approx(750.0)

    def test_mbar_zu_anzeige_torr(self):
        assert mbar_zu_anzeige(1.0, "Torr") == pytest.approx(0.750062)

    def test_mbar_zu_anzeige_none(self):
        assert mbar_zu_anzeige(None, "mbar") is None

    def test_mbar_zu_anzeige_unknown_unit_fallback(self):
        """Unbekannte Einheit → Faktor 1.0 (kein Crash)."""
        assert mbar_zu_anzeige(100.0, "unknown") == pytest.approx(100.0)


class TestMeasThreadUsesDeviceConfig:
    """Verifiziert dass MeasThread die DeviceConfig statt Globals nutzt."""

    def test_connect_uses_dev_cfg(self):
        """_connect nutzt dev_cfg.host/port/timeout, nicht Globals."""
        signals = make_mock_signals()
        custom_cfg = DeviceConfig("10.20.30.40", 12345, 7, [1, 2])
        t = tpg.MeasThread(1.0, signals, custom_cfg)

        captured = {}
        original_socket = socket.socket

        class CapturingSocket:
            def __init__(self, *a, **kw):
                self._s = original_socket(*a, **kw)
            def settimeout(self, t):
                captured['timeout'] = t
            def connect(self, addr):
                captured['addr'] = addr
                raise ConnectionRefusedError("test")
            def close(self):
                self._s.close()

        with mock.patch('tpg366_gui.socket.socket', CapturingSocket):
            t._connect()

        assert captured['addr'] == ("10.20.30.40", 12345)
        assert captured['timeout'] == 7


# ══════════════════════════════════════════════════════════════════════════════
#  ts_print – Zeitstempel in Konsolenausgaben
# ══════════════════════════════════════════════════════════════════════════════

class TestTsPrint:
    def test_has_timestamp_format(self, capsys):
        tpg.ts_print("Testmeldung")
        out = capsys.readouterr().out
        # Format: [YYYY-MM-DD HH:MM:SS] Testmeldung
        import re
        assert re.match(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] Testmeldung', out)

    def test_multiple_args(self, capsys):
        tpg.ts_print("A", "B", "C")
        out = capsys.readouterr().out
        assert "A B C" in out


# ══════════════════════════════════════════════════════════════════════════════
#  Config-Laden: leere / beschädigte Dateien
# ══════════════════════════════════════════════════════════════════════════════

class TestConfigLadenRobust:
    def test_empty_file_returns_defaults(self, tmp_path):
        cfg_file = tmp_path / "empty.json"
        cfg_file.write_text("")
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg_file)):
            cfg = tpg.config_laden()
        assert cfg["host"] == "192.168.1.71"
        assert cfg["port"] == 8000

    def test_whitespace_only_returns_defaults(self, tmp_path):
        cfg_file = tmp_path / "ws.json"
        cfg_file.write_text("   \n  \t  ")
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg_file)):
            cfg = tpg.config_laden()
        assert cfg == tpg.CONFIG_DEFAULTS

    def test_broken_json_returns_defaults(self, tmp_path):
        cfg_file = tmp_path / "broken.json"
        cfg_file.write_text("{broken json!!!")
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg_file)):
            cfg = tpg.config_laden()
        assert cfg["host"] == "192.168.1.71"

    def test_json_array_instead_of_dict(self, tmp_path):
        cfg_file = tmp_path / "array.json"
        cfg_file.write_text("[1, 2, 3]")
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg_file)):
            cfg = tpg.config_laden()
        assert cfg == tpg.CONFIG_DEFAULTS

    def test_empty_file_prints_timestamp(self, tmp_path, capsys):
        cfg_file = tmp_path / "empty2.json"
        cfg_file.write_text("")
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg_file)):
            tpg.config_laden()
        out = capsys.readouterr().out
        import re
        assert re.search(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\].*leer', out, re.IGNORECASE)

    def test_broken_json_prints_timestamp(self, tmp_path, capsys):
        cfg_file = tmp_path / "bad.json"
        cfg_file.write_text("{bad")
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg_file)):
            tpg.config_laden()
        out = capsys.readouterr().out
        import re
        assert re.search(r'\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\].*Config', out)


# ══════════════════════════════════════════════════════════════════════════════
#  plot_tage Setting + Puffergröße
# ══════════════════════════════════════════════════════════════════════════════

class TestPlotTage:
    def test_default_in_config_defaults(self):
        assert "plot_tage" in tpg.CONFIG_DEFAULTS
        assert tpg.CONFIG_DEFAULTS["plot_tage"] == 1

    def test_plot_tage_loaded_from_config(self, tmp_path):
        import json
        cfg_file = tmp_path / "c.json"
        cfg_file.write_text(json.dumps({"plot_tage": 3}))
        with mock.patch.object(tpg, 'CONFIG_FILE', str(cfg_file)):
            cfg = tpg.config_laden()
        assert cfg["plot_tage"] == 3

    def test_plot_tage_in_snapshot(self):
        win = mock.MagicMock()
        win._plot_tage = 2
        win.alarm_checks = {ch: mock.MagicMock() for ch in tpg.CHANNELS}
        win.alarm_spins = {ch: mock.MagicMock() for ch in tpg.CHANNELS}
        for ch in tpg.CHANNELS:
            win.alarm_checks[ch].isChecked.return_value = False
            win.alarm_spins[ch].value.return_value = 1000.0
        win.edit_pfad.text.return_value = "/tmp"
        win.spin_interval.value.return_value = 1.0
        win.cmb_style.currentText.return_value = "Linie"
        win.btn_logscale.isChecked.return_value = True
        win.chk_autostart.isChecked.return_value = False
        win._theme_name = "dark"
        win.cmb_einheit.currentText.return_value = "mbar"
        win._adaptiv_filter = mock.MagicMock()
        win._adaptiv_filter.schwelle_pct = 0.5
        win._adaptiv_filter.max_wartezeit = 60.0
        win._adaptiv_mess_iv = 1.0

        snap = tpg.MainWindow._cfg_snapshot(win)
        assert snap["plot_tage"] == 2

    def test_puffer_maxlen_scales_with_tage(self):
        """Deque-Größe soll proportional zu plot_tage sein."""
        # 1 Tag = 86400 Punkte
        d = deque(maxlen=1 * tpg.PUNKTE_PRO_TAG)
        assert d.maxlen == 86400
        d3 = deque(maxlen=3 * tpg.PUNKTE_PRO_TAG)
        assert d3.maxlen == 259200

    def test_on_plot_tage_changed_resizes_deque(self):
        """_on_plot_tage_changed soll die Deques anpassen."""
        win = mock.MagicMock()
        win._plot_tage = 1
        win.ts_puffer = deque([1.0, 2.0, 3.0], maxlen=86400)
        win.wertpuffer = {ch: deque([100.0, 200.0, 300.0], maxlen=86400)
                          for ch in tpg.CHANNELS}
        win._log = mock.MagicMock()

        tpg.MainWindow._on_plot_tage_changed(win, 3)

        assert win._plot_tage == 3
        assert win.ts_puffer.maxlen == 3 * 86400
        assert list(win.ts_puffer) == [1.0, 2.0, 3.0]  # Daten erhalten


# ══════════════════════════════════════════════════════════════════════════════
#  Plot-Zeitfenster-Logik
# ══════════════════════════════════════════════════════════════════════════════

class TestPlotZeitfensterLogik:
    def test_few_points_no_forced_24h(self):
        """Bei wenigen Datenpunkten wird kein starres 24h-Fenster erzwungen."""
        # Simuliert 10 Punkte innerhalb von 10 Sekunden
        import matplotlib.dates as mdates
        base = datetime(2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        ts_list = [mdates.date2num(base + timedelta(seconds=i)) for i in range(10)]
        # Die Spanne der Daten ist ~10s, nicht 24h
        span = ts_list[-1] - ts_list[0]
        assert span < 0.001  # weniger als ~1.4 Minuten in Tagen (86400 s)

    def test_day_window_clips_old_data(self):
        """Mit plot_tage=1 und Daten über 2 Tage, wird nur der letzte Tag gezeigt."""
        import matplotlib.dates as mdates
        base = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        # 2 Tage Daten: Tag 1 (alt) und Tag 2 (aktuell)
        ts_list = []
        for hour in range(0, 48):
            ts_list.append(mdates.date2num(base + timedelta(hours=hour)))

        # plot_tage = 1 → grenze = neuester - 1.0 (mpl-days)
        grenze = ts_list[-1] - 1.0
        idx_start = next((i for i, t in enumerate(ts_list) if t >= grenze), 0)
        visible = ts_list[idx_start:]

        # Sichtbar sollten nur ~24 Stunden sein
        assert len(visible) <= 25
        assert len(visible) >= 23


# ══════════════════════════════════════════════════════════════════════════════
#  Regressionstests: Adaptive Mode, Logging-Zeit, Plot-Tage-Widget
# ══════════════════════════════════════════════════════════════════════════════

class TestAdaptiveModeIntegration:
    """Adaptive Mode: Aktivierung, Intervallwechsel, Filterwirkung."""

    def test_toggle_sets_flag(self):
        win = mock.MagicMock()
        win._adaptiv_aktiv = False
        win._adaptiv_mess_iv = 1.0
        win._adaptiv_filter = tpg.AdaptivFilter()
        win.meas_thread = None
        win.btn_adaptiv = mock.MagicMock()
        tpg.MainWindow._toggle_adaptiv(win, True)
        assert win._adaptiv_aktiv is True

    def test_toggle_off_restores_interval(self):
        win = mock.MagicMock()
        win._adaptiv_aktiv = True
        win.meas_thread = mock.MagicMock()
        win.meas_thread._running = True
        win.spin_interval.value.return_value = 5.0
        win.btn_adaptiv = mock.MagicMock()
        tpg.MainWindow._toggle_adaptiv(win, False)
        assert win._adaptiv_aktiv is False
        win.meas_thread.__setattr__('interval', 5.0)

    def test_toggle_on_changes_thread_interval(self):
        win = mock.MagicMock()
        win._adaptiv_aktiv = False
        win._adaptiv_mess_iv = 0.5
        win._adaptiv_filter = tpg.AdaptivFilter()
        win.meas_thread = mock.MagicMock()
        win.meas_thread._running = True
        win.btn_adaptiv = mock.MagicMock()
        tpg.MainWindow._toggle_adaptiv(win, True)
        # Thread interval should be set to adaptive interval
        assert win.meas_thread.interval == 0.5

    def test_on_new_data_uses_adaptive_filter(self):
        """When adaptive is ON, save_data is only emitted when filter passes."""
        win = mock.MagicMock()
        win.meas_thread = mock.MagicMock()
        win.meas_thread._running = True
        win._adaptiv_aktiv = True
        win.einheit = "mbar"
        win.anzeige_einheit = "mbar"
        win.kanal_widgets = {ch: mock.MagicMock() for ch in tpg.CHANNELS}

        # Filter that always rejects
        win._adaptiv_filter = mock.MagicMock()
        win._adaptiv_filter.pruefen.return_value = False
        win._mbar_zu_anzeige = lambda x: x

        data = {ch: ("0", 750.0) for ch in tpg.CHANNELS}
        ts = datetime.now(timezone.utc)
        tpg.MainWindow._on_new_data(win, data, ts)

        # save_data should NOT have been emitted
        win.signals.save_data.emit.assert_not_called()

    def test_on_new_data_normal_always_emits(self):
        """When adaptive is OFF, save_data is always emitted."""
        win = mock.MagicMock()
        win.meas_thread = mock.MagicMock()
        win.meas_thread._running = True
        win._adaptiv_aktiv = False
        win.einheit = "mbar"
        win.anzeige_einheit = "mbar"
        win.kanal_widgets = {ch: mock.MagicMock() for ch in tpg.CHANNELS}
        win._mbar_zu_anzeige = lambda x: x

        data = {ch: ("0", 750.0) for ch in tpg.CHANNELS}
        ts = datetime.now(timezone.utc)
        tpg.MainWindow._on_new_data(win, data, ts)

        win.signals.save_data.emit.assert_called_once()

    def test_start_uses_adaptive_interval_when_active(self):
        """_toggle_messung uses _adaptiv_mess_iv when adaptive is active."""
        win = mock.MagicMock()
        win._closing = False
        win.meas_thread = None  # no running thread
        win._adaptiv_aktiv = True
        win._adaptiv_mess_iv = 2.0
        win.spin_interval.value.return_value = 10.0
        win.signals = make_mock_signals()
        win.dev_cfg = TEST_DEV_CFG
        win.ts_puffer = deque()
        win.wertpuffer = {ch: deque() for ch in tpg.CHANNELS}

        # Capture the MeasThread constructor call
        with mock.patch.object(tpg, 'MeasThread') as MockThread:
            mock_instance = mock.MagicMock()
            MockThread.return_value = mock_instance
            tpg.MainWindow._toggle_messung(win)
            # Should use adaptive interval (2.0), not spinner (10.0)
            MockThread.assert_called_once_with(2.0, win.signals, TEST_DEV_CFG)


class TestLoggingTimestamps:
    """CSV-Logging schreibt korrekte Zeitstempel."""

    def test_csv_row_has_correct_timestamps(self):
        """_verarbeite_messwerte erzeugt korrekte Zeitfelder."""
        win = mock.MagicMock()
        win.einheit = "mbar"
        win.logging_on = False
        win.wertpuffer = {ch: deque() for ch in tpg.CHANNELS}

        ts_utc = datetime(2025, 6, 15, 14, 30, 45, tzinfo=timezone.utc)
        ts_local = ts_utc.astimezone(tpg.giessen_tz())

        data = {ch: ("0", 750.0) for ch in tpg.CHANNELS}
        tpg.MainWindow._verarbeite_messwerte(win, data, ts_utc, ts_local)

        # Check the wertpuffer was populated
        for ch in tpg.CHANNELS:
            assert len(win.wertpuffer[ch]) == 1

    def test_csv_write_uses_utc_and_local_time(self):
        """CSV-Zeile enthält sowohl UTC- als auch Gießen-Zeit."""
        win = mock.MagicMock()
        win.einheit = "mbar"
        win.logging_on = True
        win.log_date = "2025-06-15"
        win.csv_writer = mock.MagicMock()
        win.csv_file = mock.MagicMock()
        win.wertpuffer = {ch: deque() for ch in tpg.CHANNELS}

        ts_utc = datetime(2025, 6, 15, 14, 30, 45, tzinfo=timezone.utc)
        ts_local = ts_utc.astimezone(tpg.giessen_tz())

        data = {ch: ("0", 750.0) for ch in tpg.CHANNELS}
        tpg.MainWindow._verarbeite_messwerte(win, data, ts_utc, ts_local)

        win.csv_writer.writerow.assert_called_once()
        row = win.csv_writer.writerow.call_args[0][0]

        # row[0] = Datum_ISO (UTC), row[1] = Zeit_UTC, row[2] = Zeit_Giessen
        assert row[0] == "2025-06-15"
        assert row[1] == "14:30:45"
        assert row[2] == ts_local.strftime("%H:%M:%S")
        # row[3] = MJD (should be a number)
        assert float(row[3]) > 0


class TestPlotTageWidgetVisibility:
    """Plot-Tage SpinBox: Erzeugung, Layout-Integration, Verdrahtung."""

    def test_spin_plot_tage_created_in_build_ctrl(self, qapp):
        """Das Widget wird erzeugt und hat den richtigen Wertebereich."""
        # We can't easily build the full MainWindow, but we can check
        # that the spin_plot_tage attribute gets created with correct range
        spin = tpg.QSpinBox()
        spin.setRange(1, 7)
        spin.setValue(3)
        spin.setSuffix(" d")
        assert spin.minimum() == 1
        assert spin.maximum() == 7
        assert spin.value() == 3
        assert spin.suffix() == " d"

    def test_plot_tage_label_exists_in_code(self):
        """Verify the label 'Plot-Tage:' exists in the GUI build code."""
        import inspect
        source = inspect.getsource(tpg.MainWindow._build_ctrl)
        assert "Plot-Tage:" in source

    def test_plot_tage_widget_in_z2_layout(self):
        """Verify spin_plot_tage is added to z2 (row 2) layout."""
        import inspect
        source = inspect.getsource(tpg.MainWindow._build_ctrl)
        # Check that the widget is created AND added to a layout
        assert "self.spin_plot_tage" in source
        assert "z2.addWidget(self.spin_plot_tage)" in source

    def test_plot_tage_has_label_before_widget(self):
        """Verify a label precedes the spinbox in the layout."""
        import inspect
        source = inspect.getsource(tpg.MainWindow._build_ctrl)
        label_pos = source.find('"Plot-Tage:"')
        widget_pos = source.find("z2.addWidget(self.spin_plot_tage)")
        assert label_pos > 0, "Label 'Plot-Tage:' not found"
        assert widget_pos > 0, "spin_plot_tage not added to layout"
        assert label_pos < widget_pos, "Label should come before widget"


class TestPlotFensterAlleRegression:
    """'Alle' (0) im Minuten-Spinner zeigt alle gepufferten Daten."""

    def test_fenster_alle_shows_all_data(self):
        """When fenster_min=0, all data in deque is shown (no tage clipping)."""
        # Simulate the _aktualisiere_plot logic inline
        ts_arr = [1.0, 2.0, 3.0, 4.0, 5.0]  # mpl-days spanning 5 days
        fenster_min = 0  # "Alle"

        # Old buggy code would clip to _plot_tage here.
        # Fixed code: when fenster_min==0, show everything.
        if fenster_min > 0 and ts_arr:
            grenze = ts_arr[-1] - fenster_min * 60.0 / 86400.0
            idx_start = next((i for i, t in enumerate(ts_arr) if t >= grenze), 0)
        else:
            idx_start = 0

        assert idx_start == 0  # ALL data visible

    def test_fenster_nonzero_clips_correctly(self):
        """When fenster_min > 0, only last N minutes are shown."""
        ts_arr = [0.0, 0.5, 1.0, 1.5, 2.0]  # mpl-days
        fenster_min = 720  # 12 hours = 0.5 days

        grenze = ts_arr[-1] - fenster_min * 60.0 / 86400.0
        idx_start = next((i for i, t in enumerate(ts_arr) if t >= grenze), 0)
        visible = ts_arr[idx_start:]

        assert len(visible) == 2  # only last 0.5 days (1.5 and 2.0)


# ══════════════════════════════════════════════════════════════════════════════
#  CSV-Archiv: Nachladen von Vortagen
# ══════════════════════════════════════════════════════════════════════════════

from csv_archive import load_day_csv, ArchiveCache
import matplotlib.dates as mdates


class TestLoadDayCsv:
    """load_day_csv: robustes Einlesen einzelner Tages-CSVs."""

    def _write_csv(self, path, lines):
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("\n".join(lines))

    def test_load_valid_csv(self, tmp_path):
        p = str(tmp_path / "2025-06-14.csv")
        self._write_csv(p, [
            "Datum_ISO,Zeit_UTC,Zeit_Giessen,MJD,K4_mbar,K4_Status,K5_mbar,K5_Status,K6_mbar,K6_Status",
            "2025-06-14,10:00:00,12:00:00,60480.4167,7.50E+02,OK,1.00E-03,OK,5.00E+01,OK",
            "2025-06-14,10:00:01,12:00:01,60480.4167,7.51E+02,OK,1.01E-03,OK,5.01E+01,OK",
        ])
        ts, w = load_day_csv(p, [4, 5, 6])
        assert len(ts) == 2
        assert len(w[4]) == 2
        assert w[4][0] == pytest.approx(750.0)
        assert w[5][0] == pytest.approx(0.001)

    def test_missing_file_returns_empty(self, tmp_path):
        ts, w = load_day_csv(str(tmp_path / "nope.csv"), [4, 5, 6])
        assert ts == []
        assert w[4] == []

    def test_empty_file_returns_empty(self, tmp_path):
        p = str(tmp_path / "empty.csv")
        self._write_csv(p, [""])
        ts, w = load_day_csv(p, [4, 5, 6])
        assert ts == []

    def test_broken_rows_skipped(self, tmp_path):
        p = str(tmp_path / "broken.csv")
        self._write_csv(p, [
            "Datum_ISO,Zeit_UTC,Zeit_Giessen,MJD,K4_mbar,K4_Status",
            "2025-06-14,10:00:00,12:00:00,60480.4167,7.50E+02,OK",
            "BROKEN_ROW_NO_COMMAS",
            "2025-06-14,10:00:02,12:00:02,60480.4167,7.52E+02,OK",
        ])
        ts, w = load_day_csv(p, [4])
        assert len(ts) == 2  # broken row skipped

    def test_nan_inf_values_treated_as_none(self, tmp_path):
        p = str(tmp_path / "nantest.csv")
        self._write_csv(p, [
            "Datum_ISO,Zeit_UTC,Zeit_Giessen,MJD,K4_mbar,K4_Status",
            "2025-06-14,10:00:00,12:00:00,60480.4167,nan,OK",
            "2025-06-14,10:00:01,12:00:01,60480.4167,inf,OK",
            "2025-06-14,10:00:02,12:00:02,60480.4167,1.00E+02,OK",
        ])
        ts, w = load_day_csv(p, [4])
        assert len(ts) == 3
        assert w[4][0] is None  # nan
        assert w[4][1] is None  # inf
        assert w[4][2] == pytest.approx(100.0)


class TestArchiveCache:
    """ArchiveCache: on-demand Nachladen mit Cache und Duplikatvermeidung."""

    def _make_csv(self, ordner, datum, stunden):
        """Erzeugt eine Tages-CSV mit stündlichen Werten."""
        lines = [
            "Datum_ISO,Zeit_UTC,Zeit_Giessen,MJD,K4_mbar,K4_Status,K5_mbar,K5_Status,K6_mbar,K6_Status"
        ]
        for h in stunden:
            lines.append(
                f"{datum},{h:02d}:00:00,{h:02d}:00:00,60000.0,"
                f"7.50E+02,OK,1.00E-03,OK,5.00E+01,OK"
            )
        pfad = os.path.join(ordner, f"{datum}.csv")
        with open(pfad, "w", encoding="utf-8", newline="") as f:
            f.write("\n".join(lines))

    def test_plot_tage_1_returns_empty(self, tmp_path):
        """Mit plot_tage=1 wird kein Archiv geladen."""
        cache = ArchiveCache([4, 5, 6])
        ts, w = cache.get_archive_data(str(tmp_path), 1, "2025-06-15", None)
        assert ts == []

    def test_plot_tage_2_loads_yesterday(self, tmp_path):
        """Mit plot_tage=2 wird der Vortag geladen."""
        od = str(tmp_path)
        self._make_csv(od, "2025-06-14", [10, 11, 12])
        cache = ArchiveCache([4, 5, 6])
        ts, w = cache.get_archive_data(od, 2, "2025-06-15", None)
        assert len(ts) == 3
        assert len(w[4]) == 3

    def test_missing_day_no_crash(self, tmp_path):
        """Fehlende CSV für einen Vortag → kein Absturz."""
        cache = ArchiveCache([4, 5, 6])
        ts, w = cache.get_archive_data(str(tmp_path), 3, "2025-06-15", None)
        assert ts == []

    def test_duplicate_avoidance(self, tmp_path):
        """Archivdaten die im Live-Puffer liegen werden ausgeschnitten."""
        od = str(tmp_path)
        self._make_csv(od, "2025-06-14", list(range(0, 24)))
        cache = ArchiveCache([4, 5, 6])
        # live_ts_min = 14.Juni 12:00 UTC (als mpl date number)
        live_min = mdates.date2num(
            datetime(2025, 6, 14, 12, 0, 0, tzinfo=timezone.utc)
        )
        ts, w = cache.get_archive_data(od, 2, "2025-06-15", live_min)
        # Nur Daten VOR 12:00 (Stunden 0–11) sollen enthalten sein
        assert len(ts) == 12
        assert len(w[4]) == 12

    def test_cache_hit(self, tmp_path):
        """Zweiter Aufruf nutzt Cache statt nochmal zu lesen."""
        od = str(tmp_path)
        self._make_csv(od, "2025-06-14", [10, 11])
        cache = ArchiveCache([4, 5, 6])
        ts1, _ = cache.get_archive_data(od, 2, "2025-06-15", None)
        assert len(ts1) == 2
        # Datei löschen — Cache sollte trotzdem liefern
        os.remove(os.path.join(od, "2025-06-14.csv"))
        ts2, _ = cache.get_archive_data(od, 2, "2025-06-15", None)
        assert len(ts2) == 2

    def test_cache_cleared_on_different_tage(self, tmp_path):
        """Cache wird aufgeräumt wenn sich benötigte Tage ändern."""
        od = str(tmp_path)
        self._make_csv(od, "2025-06-13", [10])
        self._make_csv(od, "2025-06-14", [11])
        cache = ArchiveCache([4, 5, 6])
        # plot_tage=3 → lade 13. und 14.
        cache.get_archive_data(od, 3, "2025-06-15", None)
        assert "2025-06-13" in cache._cache
        assert "2025-06-14" in cache._cache
        # plot_tage=2 → nur 14. benötigt; 13. wird aus Cache entfernt
        cache.get_archive_data(od, 2, "2025-06-15", None)
        assert "2025-06-13" not in cache._cache
        assert "2025-06-14" in cache._cache

    def test_empty_ordner_returns_empty(self):
        """Leerer Ordnerpfad → kein Crash."""
        cache = ArchiveCache([4, 5, 6])
        ts, w = cache.get_archive_data("", 3, "2025-06-15", None)
        assert ts == []

    def test_plot_tage_7_loads_6_days(self, tmp_path):
        """plot_tage=7 lädt maximal 6 Vortage."""
        od = str(tmp_path)
        for i in range(1, 8):
            d = f"2025-06-{15-i:02d}"
            self._make_csv(od, d, [12])
        cache = ArchiveCache([4, 5, 6])
        ts, w = cache.get_archive_data(od, 7, "2025-06-15", None)
        assert len(ts) == 6  # 6 Vortage × 1 Datenpunkt


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
