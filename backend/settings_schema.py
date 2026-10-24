"""Shared settings defaults and validation.

This module is safe to import from the thin VPS API: it contains no spawn or
order logic.
"""
from typing import Any

DEFAULT_SELL_DOWN_PCT = 50.0
DEFAULT_SELL_UP_PCT = 50.0
DEFAULT_SELL_DOWN_OFFSET = -2.0
DEFAULT_SELL_UP_OFFSET = 2.0
DEFAULT_BUY_AFTER_SELL_DOWN = -2.0
DEFAULT_BUY_AFTER_SELL_UP = -2.0
DEFAULT_SUPPLEMENT_SELL_UP = 2.0
DEFAULT_GRID_STEP_PCT = 3.0
DEFAULT_BUY_CANCEL_DISTANCE_PCT = 35.0

FIELDS = (
    "sell_down_pct",
    "sell_up_pct",
    "sell_down_offset_pct",
    "sell_up_offset_pct",
    "buy_after_sell_down_offset_pct",
    "buy_after_sell_up_offset_pct",
    "supplement_sell_up_offset_pct",
    "grid_step_pct",
    "buy_cancel_distance_pct",
)

DEFAULTS: dict[str, float] = {
    "sell_down_pct": DEFAULT_SELL_DOWN_PCT,
    "sell_up_pct": DEFAULT_SELL_UP_PCT,
    "sell_down_offset_pct": DEFAULT_SELL_DOWN_OFFSET,
    "sell_up_offset_pct": DEFAULT_SELL_UP_OFFSET,
    "buy_after_sell_down_offset_pct": DEFAULT_BUY_AFTER_SELL_DOWN,
    "buy_after_sell_up_offset_pct": DEFAULT_BUY_AFTER_SELL_UP,
    "supplement_sell_up_offset_pct": DEFAULT_SUPPLEMENT_SELL_UP,
    "grid_step_pct": DEFAULT_GRID_STEP_PCT,
    "buy_cancel_distance_pct": DEFAULT_BUY_CANCEL_DISTANCE_PCT,
}


def _check(field: str, value: float) -> str | None:
    if field in ("sell_down_pct", "sell_up_pct"):
        if not (0 < value <= 100):
            return f"{field} must be > 0 and <= 100"
        return None
    if field in ("sell_down_offset_pct", "buy_after_sell_down_offset_pct", "buy_after_sell_up_offset_pct"):
        if value >= 0:
            return f"{field} must be < 0"
        return None
    if field in ("sell_up_offset_pct", "supplement_sell_up_offset_pct"):
        if value <= 0:
            return f"{field} must be > 0"
        return None
    if field in ("grid_step_pct", "buy_cancel_distance_pct"):
        if value <= 0:
            return f"{field} must be > 0"
        return None
    return None


def validate_settings(values: dict[str, Any], partial: bool = True) -> str | None:
    """Validate numeric settings. Returns an error message or None."""
    for field in FIELDS:
        if field not in values:
            if partial:
                continue
            return f"{field} is required"
        raw = values.get(field)
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return f"{field} must be a number"
        if not (value == value) or value in (float("inf"), float("-inf")):
            return f"{field} must be a finite number"
        error = _check(field, value)
        if error:
            return error
    return None


def normalize_settings(symbol: str, raw: dict[str, Any] | None) -> dict[str, Any]:
    """Merge stored values with defaults and return a fully populated row."""
    data: dict[str, Any] = {"symbol": symbol.strip().upper()}
    raw = raw or {}
    for field in FIELDS:
        value = raw.get(field)
        try:
            data[field] = float(value) if value is not None else float(DEFAULTS[field])
        except (TypeError, ValueError):
            data[field] = float(DEFAULTS[field])
    return data


def merge_settings(symbol: str, current: dict[str, Any] | None,
                   updates: dict[str, Any]) -> dict[str, Any]:
    """PATCH-like merge: only provided fields change, defaults fill the rest."""
    merged = normalize_settings(symbol, current)
    for field in FIELDS:
        if field in updates and updates[field] is not None:
            try:
                merged[field] = float(updates[field])
            except (TypeError, ValueError):
                pass
    return merged
