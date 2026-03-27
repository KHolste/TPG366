"""
Verbindungs- und Gerätekonfiguration für TPG 366.

Kapselt alle Verbindungsparameter in einer unveränderlichen Struktur,
damit MeasThread und MainWindow keine globalen Variablen mehr teilen.
Enthält außerdem die Config-Validierung (aus MainWindow extrahiert).
"""


class DeviceConfig:
    """Unveränderliche Verbindungs- und Gerätekonfiguration.

    Ersetzt die globalen Variablen HOST, PORT, TIMEOUT, CHANNELS.
    Wird einmal beim Start aus der JSON-Config erzeugt und an
    MeasThread durchgereicht.
    """
    __slots__ = ('host', 'port', 'timeout', 'channels')

    def __init__(self, host: str, port: int, timeout: int, channels: list[int]):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.channels = list(channels)

    def __repr__(self):
        return (f"DeviceConfig(host={self.host!r}, port={self.port}, "
                f"timeout={self.timeout}, channels={self.channels})")


# Unveränderliche Defaults
DEFAULT_HOST = "192.168.1.71"
DEFAULT_PORT = 8000
DEFAULT_TIMEOUT = 3
DEFAULT_CHANNELS = [4, 5, 6]


def validate_config(cfg: dict) -> DeviceConfig:
    """Erzeugt ein DeviceConfig aus einem geladenen Config-Dict.

    Validiert Typen defensiv und fällt auf Defaults zurück.
    Reine Funktion, ohne Seiteneffekte, direkt testbar.
    """
    # Host
    h = cfg.get("host", DEFAULT_HOST)
    host = h if isinstance(h, str) and h.strip() else DEFAULT_HOST

    # Port
    try:
        port = int(cfg.get("port", DEFAULT_PORT))
    except (ValueError, TypeError):
        port = DEFAULT_PORT

    # Timeout
    try:
        timeout = max(1, int(cfg.get("timeout", DEFAULT_TIMEOUT)))
    except (ValueError, TypeError):
        timeout = DEFAULT_TIMEOUT

    # Channels
    ch = cfg.get("channels", DEFAULT_CHANNELS)
    if isinstance(ch, list) and ch and all(isinstance(c, int) for c in ch):
        channels = ch
    else:
        channels = DEFAULT_CHANNELS

    return DeviceConfig(host, port, timeout, channels)
