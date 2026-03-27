"""
Einheiten-Umrechnung für TPG 366 Druckmesswerte.

Konsolidiert die verstreute Umrechnungslogik (HPAMBAR, _MBAR_ZU)
in einen einzigen, testbaren Ort.
"""
import math

# Geräteeinheit → mbar  (Faktor; None = nicht konvertierbar)
GERAET_ZU_MBAR = {
    "mbar": 1.0, "hPa": 1.0, "Pa": 0.01,
    "Torr": 1.33322, "Micron": 0.00133322, "V": None,
}

# mbar → Anzeigeeinheit  (Faktor)
MBAR_ZU_ANZEIGE = {
    "mbar":   1.0,
    "hPa":    1.0,
    "Pa":     100.0,
    "Torr":   0.750062,
    "Micron": 750.062,
}


def zu_mbar(wert, geraete_einheit: str):
    """Konvertiert einen Gerätewert in mbar. Gibt None bei ungültigem Input."""
    if wert is None:
        return None
    f = GERAET_ZU_MBAR.get(geraete_einheit)
    if f is None:
        return None
    result = wert * f
    return result if math.isfinite(result) else None


def mbar_zu_anzeige(wert_mbar, anzeige_einheit: str):
    """Konvertiert mbar in die gewählte Anzeigeeinheit."""
    if wert_mbar is None:
        return None
    return wert_mbar * MBAR_ZU_ANZEIGE.get(anzeige_einheit, 1.0)
