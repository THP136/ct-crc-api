"""Shared settings defaults and validation.

This module is safe to import from the thin VPS API: it contains no spawn or
order logic.
"""
import re
from decimal import Decimal, InvalidOperation
from typing import Any

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{2,20}$")
COIN_QTY_PATTERN = re.compile(r"^\d+(\.\d{1,8})?$")
MAX_COIN_QTY_DECIMALS = 8

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


def validate_symbol(symbol: Any) -> str | None:
    """Return an error message for an invalid symbol, or None."""
    text = str(symbol or "").strip().upper()
    if not SYMBOL_PATTERN.match(text):
        return "symbol must be 2-20 characters, uppercase letters and digits"
    return None


def parse_coin_qty(value: Any) -> tuple[Decimal | None, str | None]:
    """Parse a section coin quantity. Returns (Decimal, error)."""
    if value is None or isinstance(value, bool):
        return None, "coin_qty must be a positive number"
    if isinstance(value, (int, float, Decimal)):
        try:
            text = format(Decimal(str(value)), "f")
        except (InvalidOperation, ValueError):
            return None, "coin_qty must be a positive number with at most 8 decimals"
    else:
        text = str(value).strip()
    if not COIN_QTY_PATTERN.match(text):
        return None, "coin_qty must be a positive number with at most 8 decimals"
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return None, "coin_qty must be a positive number with at most 8 decimals"
    if not parsed.is_finite() or parsed <= 0:
        return None, "coin_qty must be > 0"
    return parsed, None


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
    if "sell_down_pct" in values and "sell_up_pct" in values:
        try:
            total = float(values.get("sell_down_pct")) + float(values.get("sell_up_pct"))
        except (TypeError, ValueError):
            return "sell_down_pct and sell_up_pct must be numbers"
        if total > 100:
            return "sell_down_pct + sell_up_pct must be <= 100"
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
