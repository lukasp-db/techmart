"""California Sunset design system — shared across all Techmart demo artifacts.

Palette tokens are the canonical DMC floss->RGB values for the referenced
Golden-Gate-sunset swatch. `ui_theme()` emits the Lakeview `uiSettings.theme`
block; the same tokens are reused by the Excel report and apps later.
"""
from __future__ import annotations

# token -> hex (DMC 792/793/794/758/223/3740)
PALETTE: dict[str, str] = {
    "blue-dark": "#47527B",   # DMC 792 Dark Cornflower
    "blue-med": "#707DA3",    # DMC 793 Medium Cornflower
    "blue-light": "#8F9CC1",  # DMC 794 Light Cornflower
    "terra": "#ECA991",       # DMC 758 Very Light Terra Cotta
    "pink": "#CC928C",        # DMC 223 Light Shell Pink
    "violet-dark": "#78566A", # DMC 3740 Dark Antique Violet
}

# semantic role -> token
ROLES: dict[str, str] = {
    "primary": "blue-dark",
    "secondary": "blue-med",
    "tertiary": "blue-light",
    "accent": "terra",
    "warn": "pink",
    "negative": "violet-dark",
    "text": "violet-dark",
    "selection": "blue-dark",
}

_CANVAS_LIGHT = "#FAF7F3"   # warm off-white
_WIDGET_LIGHT = "#FFFFFF"

# Categorical series order: three cornflowers, then the warm accents.
_SERIES_ORDER = ["blue-dark", "blue-med", "blue-light", "terra", "pink", "violet-dark"]


def ui_theme() -> dict:
    """The Lakeview `uiSettings.theme` block for the California Sunset look."""
    return {
        "canvasBackgroundColor": {"light": _CANVAS_LIGHT, "dark": "#241E28"},
        "widgetBackgroundColor": {"light": _WIDGET_LIGHT, "dark": "#2E2733"},
        "fontColor": {"light": PALETTE["violet-dark"], "dark": "#E8E2E6"},
        "selectionColor": {"light": PALETTE[ROLES["selection"]], "dark": PALETTE["blue-light"]},
        "visualizationColors": [PALETTE[t] for t in _SERIES_ORDER],
    }
