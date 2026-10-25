"""Read-only projections shared by the API and the worker.

Nothing in this module spawns tasks, mutates the engine or places orders.
"""
from decimal import Decimal, InvalidOperation
from typing import Any

from .settings_schema import normalize_settings

_QTY_QUANTUM = Decimal("0.00000001")


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value if value is not None else 0))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _display_qty(value: Any) -> float:
    try:
        return float(_decimal(value).quantize(_QTY_QUANTUM))
    except (InvalidOperation, ValueError):
        return 0.0


def _serialize_section(section: dict[str, Any]) -> dict[str, Any]:
    return {
        **section,
        "coin_qty": _display_qty(section.get("coin_qty")),
        "quote_balance": _display_qty(section.get("quote_balance")),
    }


def list_sections(symbol: str | None = None) -> list[dict[str, Any]]:
    from .store import load_sections
    return [_serialize_section(s) for s in load_sections(symbol)]


def get_section_info(section_id: int) -> dict[str, Any]:
    from .store import load_section_info_batched

    info = load_section_info_batched(section_id)
    sec = info["section"]
    if sec is None:
        return {
            "section": None, "up_tasks": [], "down_tasks": [],
            "passed_tasks": [], "closed_tasks": [],
        }

    tasks = info["tasks"]
    up_tasks = sorted([t for t in tasks if t["direction"] == "UP"],
                      key=lambda t: t["target_pct"])
    down_tasks = sorted([t for t in tasks if t["direction"] == "DOWN"],
                        key=lambda t: t["target_pct"], reverse=True)
    return {
        "section": _serialize_section(sec),
        "up_tasks": up_tasks,
        "down_tasks": down_tasks,
        "passed_tasks": info["passed"],
        "closed_tasks": info["closed"],
    }


def get_all_engine_symbols() -> list[str]:
    from .store import load_all_task_engine_symbols
    return load_all_task_engine_symbols()


def get_engine_info(symbol: str) -> dict[str, Any]:
    from .store import load_engine_info_batched

    info = load_engine_info_batched(symbol)
    state = info["state"]
    if state is None:
        return {
            "state": None, "up_tasks": [], "down_tasks": [],
            "passed_tasks": [], "closed_tasks": [],
        }

    tasks = info["tasks"]
    up_tasks = sorted([t for t in tasks if t["direction"] == "UP"],
                      key=lambda t: t["target_pct"])
    down_tasks = sorted([t for t in tasks if t["direction"] == "DOWN"],
                        key=lambda t: t["target_pct"], reverse=True)
    state = {**state, "coin_qty": _display_qty(state.get("coin_qty"))}
    return {
        "state": state,
        "up_tasks": up_tasks,
        "down_tasks": down_tasks,
        "passed_tasks": info["passed"],
        "closed_tasks": info["closed"],
    }


def get_price_history(symbol: str, limit: int = 20) -> list[dict[str, Any]]:
    from .store import load_price_history
    return load_price_history(symbol, limit)


def build_summary() -> list[dict[str, Any]]:
    from .store import ensure_settings, load_section_info_batched, load_sections

    sections = list_sections()
    coins_map: dict[str, list[dict[str, Any]]] = {}
    for sec in sections:
        coins_map.setdefault(sec["symbol"], []).append(sec)

    result = []
    for sym in sorted(coins_map.keys()):
        settings = normalize_settings(sym, ensure_settings(sym))
        sell_down_pct = settings["sell_down_pct"]
        sell_up_pct = settings["sell_up_pct"]

        sell_down_rows = []
        buy_down_rows = []
        sell_up_rows = []
        total_sell_down = Decimal(0)
        total_buy_down = Decimal(0)
        total_sell_up = Decimal(0)

        for sec in coins_map[sym]:
            info = load_section_info_batched(sec["id"])
            tasks = info["tasks"]
            coin_qty = _decimal(sec.get("coin_qty"))

            for t in tasks:
                row = {**t, "coin_qty": _display_qty(coin_qty),
                       "section_name": sec["name"], "section_id": sec["id"]}
                target_price = sec["x0"] * (1 + t["target_pct"] / 100.0)
                if t["action"] == "SELL" and t["direction"] == "DOWN":
                    coins = coin_qty * _decimal(sell_down_pct) / Decimal(100)
                    row["coins_to_trade"] = _display_qty(coins)
                    row["target_price"] = target_price
                    sell_down_rows.append(row)
                    total_sell_down += coins
                elif t["action"] == "SELL" and t["direction"] == "UP":
                    coins = coin_qty * _decimal(sell_up_pct) / Decimal(100)
                    row["coins_to_trade"] = _display_qty(coins)
                    row["target_price"] = target_price
                    sell_up_rows.append(row)
                    total_sell_up += coins
                elif t["action"] == "BUY":
                    origin = t.get("sell_origin", "")
                    pct = sell_up_pct if origin == "SELL_UP" else sell_down_pct
                    coins = coin_qty * _decimal(pct) / Decimal(100)
                    row["coins_to_trade"] = _display_qty(coins)
                    row["target_price"] = target_price
                    buy_down_rows.append(row)
                    total_buy_down += coins

        result.append({
            "symbol": sym,
            "sections": coins_map[sym],
            "settings": settings,
            "sell_down": sell_down_rows,
            "buy_down": buy_down_rows,
            "sell_up": sell_up_rows,
            "total_sell_down_coins": _display_qty(total_sell_down),
            "total_buy_down_coins": _display_qty(total_buy_down),
            "total_sell_up_coins": _display_qty(total_sell_up),
        })
    return result
