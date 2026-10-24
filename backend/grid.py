"""Section grid helpers (no task spawning, safe for the thin API)."""
from typing import Any

from .settings_schema import DEFAULT_GRID_STEP_PCT

_GRID_TOL = 1e-4


def _grid_step(symbol: str) -> float:
    from .store import load_settings
    from .settings_schema import normalize_settings

    settings = normalize_settings(symbol, load_settings(symbol))
    step = settings.get("grid_step_pct", DEFAULT_GRID_STEP_PCT)
    try:
        step = float(step)
    except (TypeError, ValueError):
        step = DEFAULT_GRID_STEP_PCT
    return step if step > 0 else DEFAULT_GRID_STEP_PCT


def grid_is_valid(base_x0: float, x0: float,
                  grid_step_pct: float = DEFAULT_GRID_STEP_PCT) -> tuple[bool, int]:
    """Return (is_valid, n) where n is the grid step index."""
    pct = (x0 / base_x0 - 1.0) * 100.0
    if abs(pct) < _GRID_TOL:
        return False, 0
    step = grid_step_pct if grid_step_pct > 0 else DEFAULT_GRID_STEP_PCT
    n = pct / step
    n_round = round(n)
    return abs(n - n_round) < _GRID_TOL, n_round


def grid_price(base_x0: float, n: int,
               grid_step_pct: float = DEFAULT_GRID_STEP_PCT) -> dict[str, Any]:
    step = grid_step_pct if grid_step_pct > 0 else DEFAULT_GRID_STEP_PCT
    return {
        "n": n,
        "pct": n * step,
        "target_x": base_x0 * (1.0 + n * step / 100.0),
    }


def nearest_grid_prices(base_x0: float, x0: float,
                        grid_step_pct: float = DEFAULT_GRID_STEP_PCT) -> list[dict[str, Any]]:
    step = grid_step_pct if grid_step_pct > 0 else DEFAULT_GRID_STEP_PCT
    pct = (x0 / base_x0 - 1.0) * 100.0
    n_raw = pct / step
    candidates = sorted({int(n_raw) - 1, int(n_raw), int(n_raw) + 1, int(n_raw) + 2})
    return [grid_price(base_x0, n, step) for n in candidates if n != 0]


def get_valid_x0_for_symbol(symbol: str) -> dict[str, Any]:
    """Return grid info for a new section of this symbol."""
    from .store import load_sections

    step = _grid_step(symbol)
    sections = load_sections(symbol)
    if not sections:
        return {
            "requires_validation": False,
            "base_x0": None,
            "grid_step_pct": step,
            "sample_prices": [],
            "first_section": None,
        }

    first = sections[0]
    base_x0 = first["x0"]
    sample = [grid_price(base_x0, n, step) for n in range(-3, 4) if n != 0]
    return {
        "requires_validation": True,
        "first_section": {"id": first["id"], "name": first["name"]},
        "base_x0": base_x0,
        "grid_step_pct": step,
        "sample_prices": sample,
    }
