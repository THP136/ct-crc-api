import json
import logging
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .config import DATA_DIR, UTC7
from .settings_schema import FIELDS, normalize_settings

logger = logging.getLogger(__name__)


def _dec_str(value: Any) -> str | None:
    if value is None:
        return None
    try:
        text = format(Decimal(str(value)), "f")
    except (InvalidOperation, ValueError):
        return None
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _atomic_save_json_file(path, data: Any) -> None:
    _ensure_dir()
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def _section_snapshot(values: dict[str, Any]) -> dict[str, Any]:
    return {
        "coin_qty": _dec_str(values.get("coin_qty")),
        "quote_balance": _dec_str(values.get("quote_balance")),
    }


def _section_matches(section: dict[str, Any], snapshot: dict[str, Any] | None) -> bool:
    if not snapshot:
        return True
    return (
        _dec(section.get("coin_qty")) == _dec(snapshot.get("coin_qty"))
        and _dec(section.get("quote_balance")) == _dec(snapshot.get("quote_balance"))
    )


def _apply_journal(op: dict[str, Any]) -> None:
    """Finish one ledger operation using its before/after snapshots."""
    after = op.get("section_after")
    if after:
        sections = _load_json_file(SECTIONS_FILE) or []
        section = next((s for s in sections if s.get("id") == op.get("section_id")), None)
        if section is not None:
            if not _section_matches(section, after):
                before = op.get("section_before")
                if _section_matches(section, before):
                    section["coin_qty"] = after.get("coin_qty")
                    section["quote_balance"] = after.get("quote_balance")
                    _atomic_save_json_file(SECTIONS_FILE, sections)
                else:
                    logger.warning(
                        "ledger journal: section %s changed outside the journal",
                        op.get("section_id"),
                    )
    order_fields = op.get("order_fields")
    if order_fields:
        orders = _load_json_file(TASK_ORDERS_FILE) or []
        for order in orders:
            if order.get("task_id") == op.get("task_id"):
                order.update(order_fields)
                _atomic_save_json_file(TASK_ORDERS_FILE, orders)
                break
    queue_after = op.get("queue_quote_budget")
    if queue_after is not None:
        queue = _load_json_file(TASK_QUEUE_FILE) or []
        for task in queue:
            if task.get("id") == op.get("task_id"):
                task["quote_budget"] = queue_after
                _atomic_save_json_file(TASK_QUEUE_FILE, queue)
                break


def _recover_journal() -> None:
    op = _load_json_file(LEDGER_JOURNAL_FILE)
    if not isinstance(op, dict):
        return
    try:
        _apply_journal(op)
    except Exception:
        logger.exception("ledger journal recovery failed; keeping journal")
        return
    try:
        LEDGER_JOURNAL_FILE.unlink()
    except FileNotFoundError:
        pass


def _commit_journal(op: dict[str, Any]) -> None:
    _atomic_save_json_file(LEDGER_JOURNAL_FILE, op)
    _apply_journal(op)
    try:
        LEDGER_JOURNAL_FILE.unlink()
    except FileNotFoundError:
        pass

TASK_ENGINE_STATE_FILE = DATA_DIR / "task_engine_state.json"
TASK_QUEUE_FILE = DATA_DIR / "task_queue.json"
TASK_PASSED_FILE = DATA_DIR / "task_passed.json"
TASK_CLOSED_FILE = DATA_DIR / "task_closed.json"
TASK_ORDERS_FILE = DATA_DIR / "task_orders.json"
COMMANDS_FILE = DATA_DIR / "commands.json"
WORKER_STATUS_FILE = DATA_DIR / "worker_status.json"
LEDGER_JOURNAL_FILE = DATA_DIR / "ledger_journal.json"

_task_queue_id_counter = 0


def _use_db() -> bool:
    return bool(os.getenv("DATABASE_URL", "").strip())


def _ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _load_json_file(path) -> Any:
    _ensure_dir()
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("_load_json_file %s: %s", path, e)
        return None


def _save_json_file(path, data: Any) -> None:
    _ensure_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# --------------- Task Engine State ---------------

def load_task_engine_state(symbol: str) -> dict[str, Any] | None:
    if _use_db():
        from .db import load_task_engine_state as _load
        return _load(symbol)
    data = _load_json_file(TASK_ENGINE_STATE_FILE)
    if not data or not isinstance(data, dict):
        return None
    return data.get(symbol.strip().upper())


def save_task_engine_state(symbol: str, state: dict[str, Any]) -> None:
    if _use_db():
        from .db import save_task_engine_state as _save
        return _save(symbol, state)
    data = _load_json_file(TASK_ENGINE_STATE_FILE) or {}
    data[symbol.strip().upper()] = state
    _save_json_file(TASK_ENGINE_STATE_FILE, data)


def load_all_task_engine_symbols() -> list[str]:
    if _use_db():
        from .db import load_all_task_engine_symbols as _load
        return _load()
    data = _load_json_file(TASK_ENGINE_STATE_FILE)
    if not data or not isinstance(data, dict):
        return []
    return sorted(data.keys())


# --------------- Task Queue ---------------

def load_task_queue(symbol: str) -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_task_queue as _load
        return _load(symbol)
    data = _load_json_file(TASK_QUEUE_FILE)
    if not data or not isinstance(data, list):
        return []
    sym = symbol.strip().upper()
    return [t for t in data if (t.get("symbol") or "").upper() == sym]


def add_task_to_queue(symbol: str, direction: str, target_pct: float,
                      action: str, note: str,
                      sibling_id: int | None = None,
                      sell_origin: str = "") -> dict[str, Any] | None:
    if target_pct < -98 or target_pct > 98:
        return None
    if _use_db():
        from .db import add_task_to_queue as _add
        return _add(symbol, direction, target_pct, action, note, sibling_id, sell_origin)
    global _task_queue_id_counter
    data = _load_json_file(TASK_QUEUE_FILE) or []
    max_id = max((t.get("id", 0) for t in data), default=0)
    _task_queue_id_counter = max(max_id, _task_queue_id_counter) + 1
    task = {
        "id": _task_queue_id_counter,
        "symbol": symbol.strip().upper(),
        "direction": direction,
        "target_pct": target_pct,
        "action": action,
        "note": note,
        "sibling_id": sibling_id,
        "sell_origin": sell_origin,
    }
    data.append(task)
    _save_json_file(TASK_QUEUE_FILE, data)
    return task


def update_task_sibling_id(task_id: int, sibling_id: int) -> None:
    if _use_db():
        from .db import update_task_sibling_id as _update
        return _update(task_id, sibling_id)
    data = _load_json_file(TASK_QUEUE_FILE) or []
    for t in data:
        if t.get("id") == task_id:
            t["sibling_id"] = sibling_id
            break
    _save_json_file(TASK_QUEUE_FILE, data)


def load_task_by_id(task_id: int) -> dict[str, Any] | None:
    if _use_db():
        from .db import load_task_by_id as _load
        return _load(task_id)
    data = _load_json_file(TASK_QUEUE_FILE) or []
    return next((t for t in data if t.get("id") == task_id), None)


def remove_task_from_queue(task_id: int) -> None:
    if _use_db():
        from .db import remove_task_from_queue as _remove
        return _remove(task_id)
    data = _load_json_file(TASK_QUEUE_FILE) or []
    data = [t for t in data if t.get("id") != task_id]
    _save_json_file(TASK_QUEUE_FILE, data)


def delete_all_buy_tasks_for_symbol(symbol: str) -> int:
    """Remove all pending BUY tasks for a symbol. Returns count deleted."""
    if _use_db():
        from .db import delete_all_buy_tasks_for_symbol as _delete
        return _delete(symbol)
    data = _load_json_file(TASK_QUEUE_FILE) or []
    sym = symbol.strip().upper()
    before = len(data)
    data = [t for t in data if not ((t.get("symbol") or "").upper() == sym and t.get("action") == "BUY")]
    _save_json_file(TASK_QUEUE_FILE, data)
    return before - len(data)


def clear_task_queue_for_symbol(symbol: str) -> None:
    if _use_db():
        from .db import clear_task_queue_for_symbol as _clear
        return _clear(symbol)
    data = _load_json_file(TASK_QUEUE_FILE) or []
    sym = symbol.strip().upper()
    data = [t for t in data if (t.get("symbol") or "").upper() != sym]
    _save_json_file(TASK_QUEUE_FILE, data)


# --------------- Passed Tasks ---------------

def add_passed_task(symbol: str, direction: str, action: str, target_pct: float,
                    hit_pct: float, hit_price: float, note: str,
                    task_id: int | None = None,
                    sell_origin: str = "") -> None:
    if _use_db():
        from .db import add_passed_task as _add
        return _add(symbol, direction, action, target_pct, hit_pct, hit_price, note,
                    task_id=task_id, sell_origin=sell_origin)
    data = _load_json_file(TASK_PASSED_FILE) or []
    data.insert(0, {
        "symbol": symbol.strip().upper(),
        "task_id": task_id,
        "direction": direction,
        "action": action,
        "target_pct": target_pct,
        "hit_pct": hit_pct,
        "hit_price": hit_price,
        "note": note,
        "sell_origin": sell_origin,
        "at": datetime.now(UTC7).strftime("%Y-%m-%d %H:%M:%S"),
    })
    data = data[:500]
    _save_json_file(TASK_PASSED_FILE, data)


def load_passed_tasks(symbol: str) -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_passed_tasks as _load
        return _load(symbol)
    data = _load_json_file(TASK_PASSED_FILE) or []
    sym = symbol.strip().upper()
    return [t for t in data if (t.get("symbol") or "").upper() == sym][:200]


def clear_passed_tasks_for_symbol(symbol: str) -> None:
    if _use_db():
        from .db import clear_passed_tasks_for_symbol as _clear
        return _clear(symbol)
    data = _load_json_file(TASK_PASSED_FILE) or []
    sym = symbol.strip().upper()
    data = [t for t in data if (t.get("symbol") or "").upper() != sym]
    _save_json_file(TASK_PASSED_FILE, data)


# --------------- Closed Tasks (sibling cancelled) ---------------

def add_closed_task(symbol: str, closed_task_id: int,
                    sibling_triggered_id: int | None, direction: str, action: str,
                    target_pct: float, at_pct: float, at_price: float,
                    reason: str, note: str) -> None:
    if _use_db():
        from .db import add_closed_task as _add
        return _add(symbol, closed_task_id, sibling_triggered_id, direction,
                     action, target_pct, at_pct, at_price, reason, note)
    data = _load_json_file(TASK_CLOSED_FILE) or []
    data.insert(0, {
        "symbol": symbol.strip().upper(),
        "closed_task_id": closed_task_id,
        "sibling_triggered_id": sibling_triggered_id,
        "direction": direction,
        "action": action,
        "target_pct": target_pct,
        "at_pct": at_pct,
        "at_price": at_price,
        "reason": reason,
        "note": note,
        "at": datetime.now(UTC7).strftime("%Y-%m-%d %H:%M:%S"),
    })
    data = data[:500]
    _save_json_file(TASK_CLOSED_FILE, data)


def load_closed_tasks(symbol: str) -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_closed_tasks as _load
        return _load(symbol)
    data = _load_json_file(TASK_CLOSED_FILE) or []
    sym = symbol.strip().upper()
    return [t for t in data if (t.get("symbol") or "").upper() == sym][:200]


def clear_closed_tasks_for_symbol(symbol: str) -> None:
    if _use_db():
        from .db import clear_closed_tasks_for_symbol as _clear
        return _clear(symbol)
    data = _load_json_file(TASK_CLOSED_FILE) or []
    sym = symbol.strip().upper()
    data = [t for t in data if (t.get("symbol") or "").upper() != sym]
    _save_json_file(TASK_CLOSED_FILE, data)


# --------------- Live Prices ---------------

LIVE_PRICES_FILE = DATA_DIR / "live_prices.json"


def save_live_prices(prices: dict[str, float]) -> None:
    if not prices:
        return
    if _use_db():
        from .db import save_live_prices as _save
        return _save(prices)
    data = _load_json_file(LIVE_PRICES_FILE) or {}
    data.update({k.strip().upper(): v for k, v in prices.items()})
    _save_json_file(LIVE_PRICES_FILE, data)


def load_live_prices() -> dict[str, float]:
    if _use_db():
        from .db import load_live_prices as _load
        return _load()
    data = _load_json_file(LIVE_PRICES_FILE)
    if not data or not isinstance(data, dict):
        return {}
    return {k: float(v) for k, v in data.items()}


# --------------- Batched Info (single DB connection) ---------------

def load_engine_info_batched(symbol: str) -> dict[str, Any]:
    if _use_db():
        from .db import load_engine_info_batched as _load
        return _load(symbol)
    state = load_task_engine_state(symbol)
    tasks = load_task_queue(symbol)
    passed = load_passed_tasks(symbol)
    closed = load_closed_tasks(symbol)
    return {"state": state, "tasks": tasks, "passed": passed, "closed": closed}


# --------------- Settings ---------------

SETTINGS_FILE = DATA_DIR / "settings.json"


def load_settings(symbol: str) -> dict[str, Any] | None:
    if _use_db():
        from .db import load_settings as _load
        row = _load(symbol)
    else:
        data = _load_json_file(SETTINGS_FILE)
        row = data.get(symbol.strip().upper()) if isinstance(data, dict) else None
    if row is None:
        return None
    return normalize_settings(symbol, row)


def save_settings_config(symbol: str, values: dict[str, Any]) -> dict[str, Any]:
    """Write a full settings row. Missing fields inherit current/default values."""
    sym = symbol.strip().upper()
    current = load_settings(sym)
    merged = normalize_settings(sym, current)
    for field in FIELDS:
        if field in values and values[field] is not None:
            try:
                merged[field] = float(values[field])
            except (TypeError, ValueError):
                pass
    if _use_db():
        from .db import save_settings_config as _save
        _save(sym, merged)
    else:
        data = _load_json_file(SETTINGS_FILE) or {}
        data[sym] = merged
        _save_json_file(SETTINGS_FILE, data)
    return merged


def save_settings(symbol: str, sell_down_pct: float, sell_up_pct: float) -> None:
    """Backward-compatible 2-column save; other settings are preserved."""
    save_settings_config(symbol, {
        "sell_down_pct": sell_down_pct,
        "sell_up_pct": sell_up_pct,
    })


def load_all_settings() -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_all_settings as _load
        rows = _load()
    else:
        data = _load_json_file(SETTINGS_FILE)
        rows = list(data.values()) if isinstance(data, dict) else []
    return sorted(
        (normalize_settings(r.get("symbol", ""), r) for r in rows),
        key=lambda s: s.get("symbol", ""),
    )


def ensure_settings(symbol: str) -> dict[str, Any]:
    existing = load_settings(symbol)
    if existing:
        if not _use_db():
            data = _load_json_file(SETTINGS_FILE) or {}
            raw = data.get(symbol.strip().upper()) or {}
            if any(field not in raw for field in FIELDS):
                data[symbol.strip().upper()] = existing
                _save_json_file(SETTINGS_FILE, data)
        return existing
    return save_settings_config(symbol, {})


# --------------- Sections ---------------

SECTIONS_FILE = DATA_DIR / "sections.json"
_section_id_counter = 0


def create_section(symbol: str, name: str, x0: float, coin_qty: float,
                   request_id: str | None = None) -> dict[str, Any] | None:
    if _use_db():
        from .db import create_section as _create
        return _create(symbol, name, x0, coin_qty, request_id=request_id)
    global _section_id_counter
    data = _load_json_file(SECTIONS_FILE) or []
    if request_id:
        existing = next((s for s in data if s.get("request_id") == request_id), None)
        if existing:
            return existing
    max_id = max((s.get("id", 0) for s in data), default=0)
    _section_id_counter = max(max_id, _section_id_counter) + 1
    sec = {
        "id": _section_id_counter,
        "name": name,
        "symbol": symbol.strip().upper(),
        "x0": x0,
        "coin_qty": _dec_str(coin_qty) or "0",
        "quote_balance": "0",
        "current_x": x0,
        "current_pct": 0.0,
        "seeded": True,
        "request_id": request_id,
    }
    data.append(sec)
    _save_json_file(SECTIONS_FILE, data)
    return sec


def load_section_by_request_id(request_id: str) -> dict[str, Any] | None:
    if not request_id:
        return None
    if _use_db():
        from .db import load_section_by_request_id as _load
        return _load(request_id)
    data = _load_json_file(SECTIONS_FILE) or []
    return next((s for s in data if s.get("request_id") == request_id), None)


def load_sections(symbol: str | None = None) -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_sections as _load
        return _load(symbol)
    data = _load_json_file(SECTIONS_FILE) or []
    if symbol:
        sym = symbol.strip().upper()
        return [s for s in data if (s.get("symbol") or "").upper() == sym]
    return data


def load_section(section_id: int) -> dict[str, Any] | None:
    if _use_db():
        from .db import load_section as _load
        return _load(section_id)
    data = _load_json_file(SECTIONS_FILE) or []
    return next((s for s in data if s.get("id") == section_id), None)


def save_section_state(section_id: int, current_x: float, current_pct: float) -> None:
    if _use_db():
        from .db import save_section_state as _save
        return _save(section_id, current_x, current_pct)
    data = _load_json_file(SECTIONS_FILE) or []
    for s in data:
        if s.get("id") == section_id:
            s["current_x"] = current_x
            s["current_pct"] = current_pct
            break
    _save_json_file(SECTIONS_FILE, data)


def delete_section(section_id: int) -> None:
    if _use_db():
        from .db import delete_section_db as _delete
        return _delete(section_id)
    data = _load_json_file(SECTIONS_FILE) or []
    data = [s for s in data if s.get("id") != section_id]
    _save_json_file(SECTIONS_FILE, data)
    # Also clean tasks
    for fpath in (TASK_QUEUE_FILE, TASK_PASSED_FILE, TASK_CLOSED_FILE):
        items = _load_json_file(fpath) or []
        items = [t for t in items if t.get("section_id") != section_id]
        _save_json_file(fpath, items)


def load_task_queue_by_section(section_id: int) -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_task_queue_by_section as _load
        return _load(section_id)
    data = _load_json_file(TASK_QUEUE_FILE) or []
    return [t for t in data if t.get("section_id") == section_id]


def add_task_to_queue_for_section(section_id: int, symbol: str, direction: str,
                                   target_pct: float, action: str, note: str,
                                   sibling_id: int | None = None,
                                   sell_origin: str = "",
                                   quote_budget: Any = None,
                                   spawn_key: str | None = None) -> dict[str, Any] | None:
    if target_pct < -98 or target_pct > 98:
        return None
    if _use_db():
        from .db import add_task_to_queue_for_section as _add
        return _add(section_id, symbol, direction, target_pct, action, note,
                    sibling_id, sell_origin, quote_budget, spawn_key)
    global _task_queue_id_counter
    data = _load_json_file(TASK_QUEUE_FILE) or []
    if spawn_key:
        existing = next((t for t in data if t.get("spawn_key") == spawn_key), None)
        if existing:
            return existing
    max_id = max((t.get("id", 0) for t in data), default=0)
    _task_queue_id_counter = max(max_id, _task_queue_id_counter) + 1
    task = {
        "id": _task_queue_id_counter,
        "symbol": symbol.strip().upper(),
        "direction": direction,
        "target_pct": target_pct,
        "action": action,
        "note": note,
        "sibling_id": sibling_id,
        "sell_origin": sell_origin,
        "section_id": section_id,
        "quote_budget": _dec_str(quote_budget) if quote_budget is not None else None,
        "spawn_key": spawn_key or "",
    }
    data.append(task)
    _save_json_file(TASK_QUEUE_FILE, data)
    return task


def create_buy_task_reserving_quote(section_id: int, symbol: str, direction: str,
                                    target_pct: float, note: str,
                                    sell_origin: str = "",
                                    spawn_key: str | None = None,
                                    sibling_id: int | None = None
                                    ) -> dict[str, Any] | None:
    if _use_db():
        from .db import create_buy_task_reserving_quote as _create
        return _create(section_id, symbol, direction, target_pct, note,
                       sell_origin, spawn_key, sibling_id)
    if target_pct < -98 or target_pct > 98:
        return None
    global _task_queue_id_counter
    queue = _load_json_file(TASK_QUEUE_FILE) or []
    if spawn_key:
        existing = next((t for t in queue if t.get("spawn_key") == spawn_key), None)
        if existing:
            return existing
    sections = _load_json_file(SECTIONS_FILE) or []
    section = next((s for s in sections if s.get("id") == section_id), None)
    if section is None:
        return None
    budget = _dec(section.get("quote_balance"))
    if budget <= 0:
        return None
    section["quote_balance"] = "0"
    _save_json_file(SECTIONS_FILE, sections)
    max_id = max((t.get("id", 0) for t in queue), default=0)
    _task_queue_id_counter = max(max_id, _task_queue_id_counter) + 1
    task = {
        "id": _task_queue_id_counter,
        "symbol": symbol.strip().upper(),
        "direction": direction,
        "target_pct": target_pct,
        "action": "BUY",
        "note": note,
        "sibling_id": sibling_id,
        "sell_origin": sell_origin,
        "section_id": section_id,
        "quote_budget": _dec_str(budget),
        "spawn_key": spawn_key or "",
    }
    queue.append(task)
    _save_json_file(TASK_QUEUE_FILE, queue)
    return task


def add_passed_task_for_section(section_id: int, symbol: str, direction: str,
                                 action: str, target_pct: float, hit_pct: float,
                                 hit_price: float, note: str,
                                 task_id: int | None = None,
                                 sell_origin: str = "") -> None:
    if _use_db():
        from .db import add_passed_task_for_section as _add
        return _add(section_id, symbol, direction, action, target_pct, hit_pct, hit_price, note, task_id, sell_origin)
    data = _load_json_file(TASK_PASSED_FILE) or []
    if task_id is not None and any(
        t.get("section_id") == section_id and t.get("task_id") == task_id
        for t in data
    ):
        return
    data.insert(0, {
        "symbol": symbol.strip().upper(),
        "task_id": task_id,
        "direction": direction,
        "action": action,
        "target_pct": target_pct,
        "hit_pct": hit_pct,
        "hit_price": hit_price,
        "note": note,
        "sell_origin": sell_origin,
        "section_id": section_id,
        "at": datetime.now(UTC7).strftime("%Y-%m-%d %H:%M:%S"),
    })
    data = data[:500]
    _save_json_file(TASK_PASSED_FILE, data)


def load_passed_tasks_by_section(section_id: int) -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_passed_tasks_by_section as _load
        return _load(section_id)
    data = _load_json_file(TASK_PASSED_FILE) or []
    return [t for t in data if t.get("section_id") == section_id][:200]


def add_closed_task_for_section(section_id: int, symbol: str, closed_task_id: int,
                                 sibling_triggered_id: int | None, direction: str,
                                 action: str, target_pct: float, at_pct: float,
                                 at_price: float, reason: str, note: str) -> None:
    if _use_db():
        from .db import add_closed_task_for_section as _add
        return _add(section_id, symbol, closed_task_id, sibling_triggered_id,
                    direction, action, target_pct, at_pct, at_price, reason, note)
    data = _load_json_file(TASK_CLOSED_FILE) or []
    if any(t.get("section_id") == section_id and t.get("closed_task_id") == closed_task_id
           for t in data):
        return
    data.insert(0, {
        "symbol": symbol.strip().upper(),
        "closed_task_id": closed_task_id,
        "sibling_triggered_id": sibling_triggered_id,
        "direction": direction,
        "action": action,
        "target_pct": target_pct,
        "at_pct": at_pct,
        "at_price": at_price,
        "reason": reason,
        "note": note,
        "section_id": section_id,
        "at": datetime.now(UTC7).strftime("%Y-%m-%d %H:%M:%S"),
    })
    data = data[:500]
    _save_json_file(TASK_CLOSED_FILE, data)


def load_closed_tasks_by_section(section_id: int) -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_closed_tasks_by_section as _load
        return _load(section_id)
    data = _load_json_file(TASK_CLOSED_FILE) or []
    return [t for t in data if t.get("section_id") == section_id][:200]


def load_section_info_batched(section_id: int) -> dict[str, Any]:
    if _use_db():
        from .db import load_section_info_batched as _load
        return _load(section_id)
    section = load_section(section_id)
    tasks = load_task_queue_by_section(section_id)
    passed = load_passed_tasks_by_section(section_id)
    closed = load_closed_tasks_by_section(section_id)
    return {"section": section, "tasks": tasks, "passed": passed, "closed": closed}


# --------------- Price History ---------------

PRICE_HISTORY_FILE = DATA_DIR / "price_history.json"
DAILY_REPORTS_FILE = DATA_DIR / "daily_reports.json"


def add_price_history(symbol: str, price: float) -> None:
    if _use_db():
        from .db import add_price_history as _add
        return _add(symbol, price)
    data = _load_json_file(PRICE_HISTORY_FILE) or []
    data.insert(0, {
        "symbol": symbol.strip().upper(),
        "price": price,
        "at": datetime.now(UTC7).strftime("%Y-%m-%d %H:%M:%S"),
    })
    data = data[:1000]
    _save_json_file(PRICE_HISTORY_FILE, data)


def load_price_history(symbol: str, limit: int = 20) -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_price_history as _load
        return _load(symbol, limit)
    data = _load_json_file(PRICE_HISTORY_FILE) or []
    sym = symbol.strip().upper()
    return [h for h in data if (h.get("symbol") or "").upper() == sym][:limit]


def delete_engine(symbol: str) -> None:
    if _use_db():
        from .db import delete_engine_db as _delete
        return _delete(symbol)
    sym = symbol.strip().upper()
    # sections
    secs = _load_json_file(SECTIONS_FILE) or []
    sec_ids = {s["id"] for s in secs if (s.get("symbol") or "").upper() == sym}
    secs = [s for s in secs if s.get("id") not in sec_ids]
    _save_json_file(SECTIONS_FILE, secs)
    # tasks, passed, closed - remove by symbol
    for fpath in (TASK_QUEUE_FILE, TASK_PASSED_FILE, TASK_CLOSED_FILE):
        items = _load_json_file(fpath) or []
        items = [t for t in items if (t.get("symbol") or "").upper() != sym]
        _save_json_file(fpath, items)
    # engine state
    states = _load_json_file(TASK_ENGINE_STATE_FILE) or {}
    states.pop(sym, None)
    _save_json_file(TASK_ENGINE_STATE_FILE, states)
    # settings
    settings = _load_json_file(SETTINGS_FILE) or {}
    settings.pop(sym, None)
    _save_json_file(SETTINGS_FILE, settings)
    # price history
    history = _load_json_file(PRICE_HISTORY_FILE) or []
    history = [h for h in history if (h.get("symbol") or "").upper() != sym]
    _save_json_file(PRICE_HISTORY_FILE, history)


# --------------- Section ledger / accounting ---------------

def adjust_section_balances(section_id: int, base_delta: Any,
                            quote_delta: Any) -> dict[str, Any]:
    if _use_db():
        from .db import adjust_section_balances as _adjust
        return _adjust(section_id, base_delta, quote_delta)
    sections = _load_json_file(SECTIONS_FILE) or []
    section = next((s for s in sections if s.get("id") == section_id), None)
    if section is None:
        return {"coin_qty": "0", "quote_balance": "0", "clamped": False}
    coin = _dec(section.get("coin_qty")) + _dec(base_delta)
    quote = _dec(section.get("quote_balance")) + _dec(quote_delta)
    clamped = coin < 0
    if clamped:
        logger.warning("section %s oversell clamped to 0", section_id)
        coin = Decimal(0)
    section["coin_qty"] = _dec_str(coin)
    section["quote_balance"] = _dec_str(quote)
    _save_json_file(SECTIONS_FILE, sections)
    return {
        "coin_qty": _dec_str(coin),
        "quote_balance": _dec_str(quote),
        "clamped": clamped,
    }


def reserve_section_quote(section_id: int, amount: Any) -> bool:
    if _use_db():
        from .db import reserve_section_quote as _reserve
        return _reserve(section_id, amount)
    sections = _load_json_file(SECTIONS_FILE) or []
    section = next((s for s in sections if s.get("id") == section_id), None)
    if section is None:
        return False
    balance = _dec(section.get("quote_balance"))
    wanted = _dec(amount)
    if balance < wanted:
        return False
    section["quote_balance"] = _dec_str(balance - wanted)
    _save_json_file(SECTIONS_FILE, sections)
    return True


def add_section_quote(section_id: int, amount: Any) -> None:
    if _use_db():
        from .db import add_section_quote as _add
        return _add(section_id, amount)
    sections = _load_json_file(SECTIONS_FILE) or []
    for section in sections:
        if section.get("id") == section_id:
            section["quote_balance"] = _dec_str(_dec(section.get("quote_balance")) + _dec(amount))
            break
    _save_json_file(SECTIONS_FILE, sections)


def apply_order_accounting(task_id: int, base_delta: Any, quote_delta: Any,
                           aggregates: dict[str, Any] | None = None) -> bool:
    if _use_db():
        from .db import apply_order_accounting as _apply
        return _apply(task_id, base_delta, quote_delta, aggregates)
    _recover_journal()
    aggregates = aggregates or {}
    orders = _load_json_file(TASK_ORDERS_FILE) or []
    order = next((o for o in orders if o.get("task_id") == task_id), None)
    if order is None or order.get("accounting_applied_at"):
        return False
    section_id = order.get("section_id")
    sections = _load_json_file(SECTIONS_FILE) or []
    section = next((s for s in sections if s.get("id") == section_id), None)
    if section is None:
        return False
    before = _section_snapshot(section)
    coin = _dec(section.get("coin_qty")) + _dec(base_delta)
    quote = _dec(section.get("quote_balance")) + _dec(quote_delta)
    clamped = coin < 0
    if clamped:
        logger.warning("section %s oversell clamped to 0", section_id)
        coin = Decimal(0)
    after = {"coin_qty": _dec_str(coin), "quote_balance": _dec_str(quote)}

    status = str(aggregates.get("accounting_status") or "OK")
    if clamped and status != "NEEDS_REVIEW":
        status = "NEEDS_REVIEW"
    order_fields: dict[str, Any] = {
        "fee_base": _dec_str(aggregates.get("fee_base") or 0),
        "fee_quote": _dec_str(aggregates.get("fee_quote") or 0),
        "fee_other": _dec_str(aggregates.get("fee_other") or 0),
        "fee_other_currency": aggregates.get("fee_other_currency") or "",
        "accounting_status": status,
        "accounting_applied_at": _ts(),
        "updated_at": _ts(),
    }
    for field in ("filled_base", "filled_quote", "quote_spent"):
        if aggregates.get(field) is not None:
            order_fields[field] = _dec_str(aggregates[field])
    _commit_journal({
        "kind": "accounting",
        "task_id": task_id,
        "section_id": section_id,
        "section_before": before,
        "section_after": after,
        "order_fields": order_fields,
    })
    return True


def settle_buy_order(task_id: int) -> dict[str, Any] | None:
    if _use_db():
        from .db import settle_buy_order as _settle
        return _settle(task_id)
    _recover_journal()
    orders = _load_json_file(TASK_ORDERS_FILE) or []
    order = next((o for o in orders if o.get("task_id") == task_id), None)
    if order is None or order.get("quote_settled_at"):
        return None
    section_id = order.get("section_id")
    sections = _load_json_file(SECTIONS_FILE) or []
    section = next((s for s in sections if s.get("id") == section_id), None)
    if section is None:
        return None
    budget = _dec(order.get("quote_budget"))
    spent = _dec(order.get("quote_spent"))
    leftover = budget - spent
    if leftover < 0:
        leftover = Decimal(0)
    before = _section_snapshot(section)
    quote_after = _dec(section.get("quote_balance")) + leftover
    order_fields: dict[str, Any] = {
        "quote_settled_at": _ts(),
        "updated_at": _ts(),
    }
    if spent > budget:
        order_fields["accounting_status"] = "NEEDS_REVIEW"
    _commit_journal({
        "kind": "settle",
        "task_id": task_id,
        "section_id": section_id,
        "section_before": before,
        "section_after": {"coin_qty": before.get("coin_qty"),
                          "quote_balance": _dec_str(quote_after)},
        "order_fields": order_fields,
        "queue_quote_budget": "0",
    })
    return {"task_id": task_id, "released": _dec_str(leftover)}


def release_buy_task_budget(task_id: int) -> str | None:
    if _use_db():
        from .db import release_buy_task_budget as _release
        return _release(task_id)
    _recover_journal()
    orders = _load_json_file(TASK_ORDERS_FILE) or []
    if any(o.get("task_id") == task_id for o in orders):
        return None
    queue = _load_json_file(TASK_QUEUE_FILE) or []
    task = next((t for t in queue if t.get("id") == task_id), None)
    if task is None or str(task.get("action") or "").upper() != "BUY":
        return None
    budget = _dec(task.get("quote_budget"))
    if budget <= 0:
        return None
    section_id = task.get("section_id")
    sections = _load_json_file(SECTIONS_FILE) or []
    section = next((s for s in sections if s.get("id") == section_id), None)
    if section is None:
        return None
    before = _section_snapshot(section)
    quote_after = _dec(section.get("quote_balance")) + budget
    _commit_journal({
        "kind": "release",
        "task_id": task_id,
        "section_id": section_id,
        "section_before": before,
        "section_after": {"coin_qty": before.get("coin_qty"),
                          "quote_balance": _dec_str(quote_after)},
        "queue_quote_budget": "0",
    })
    return _dec_str(budget)


# --------------- Task Orders ---------------

ORDER_FIELDS = (
    "task_id", "section_id", "symbol", "exchange_symbol", "client_order_id",
    "exchange_order_id", "side", "direction", "order_type", "amount", "price",
    "stop_price", "target_pct", "target_price", "sell_origin", "status",
    "filled_amount", "avg_fill_price", "filled_base", "filled_quote",
    "fee_base", "fee_quote", "fee_other", "fee_other_currency",
    "quote_budget", "quote_spent", "quote_settled_at",
    "trades_synced_filled", "accounting_status", "accounting_applied_at",
    "attempts", "next_retry_at", "last_error", "last_synced_at",
    "completed_at", "filled_at",
)
ACTIVE_ORDER_STATUSES = ("PENDING", "OPEN", "PARTIALLY_FILLED", "UNKNOWN")
FINAL_ORDER_STATUSES = ("FILLED", "CANCELED", "REJECTED", "NEEDS_REVIEW")

_NUMERIC_ORDER_FIELDS = (
    "amount", "price", "stop_price", "target_pct", "target_price",
    "filled_amount", "avg_fill_price",
)

_MONEY_ORDER_FIELDS = (
    "filled_base", "filled_quote", "fee_base", "fee_quote", "fee_other",
    "quote_budget", "quote_spent", "trades_synced_filled",
)


def _ts() -> str:
    return datetime.now(UTC7).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_order(data: dict[str, Any]) -> dict[str, Any]:
    rec = {field: data.get(field) for field in ORDER_FIELDS}
    rec["symbol"] = (rec.get("symbol") or "").strip().upper()
    rec["exchange_symbol"] = (rec.get("exchange_symbol") or "").strip().upper()
    rec["status"] = (rec.get("status") or "PENDING").strip().upper()
    rec["sell_origin"] = rec.get("sell_origin") or ""
    rec["last_error"] = rec.get("last_error") or ""
    for field in _NUMERIC_ORDER_FIELDS:
        value = rec.get(field)
        if value is None:
            continue
        try:
            rec[field] = float(value)
        except (TypeError, ValueError):
            rec[field] = None
    for field in _MONEY_ORDER_FIELDS:
        rec[field] = _dec_str(rec.get(field))
    if rec.get("fee_base") is None:
        rec["fee_base"] = "0"
    if rec.get("fee_quote") is None:
        rec["fee_quote"] = "0"
    if rec.get("fee_other") is None:
        rec["fee_other"] = "0"
    if rec.get("quote_spent") is None:
        rec["quote_spent"] = "0"
    if rec.get("trades_synced_filled") is None:
        rec["trades_synced_filled"] = "0"
    rec["fee_other_currency"] = rec.get("fee_other_currency") or ""
    rec["accounting_status"] = rec.get("accounting_status") or "PENDING"
    try:
        rec["attempts"] = int(rec.get("attempts") or 0)
    except (TypeError, ValueError):
        rec["attempts"] = 0
    if rec.get("filled_amount") is None:
        rec["filled_amount"] = 0.0
    rec["section_id"] = int(rec["section_id"]) if rec.get("section_id") is not None else None
    rec["task_id"] = int(rec["task_id"]) if rec.get("task_id") is not None else None
    for field in ("next_retry_at", "last_synced_at", "completed_at",
                  "filled_at", "quote_settled_at", "accounting_applied_at",
                  "created_at", "updated_at"):
        value = rec.get(field)
        if value is None:
            continue
        rec[field] = value.strftime("%Y-%m-%d %H:%M:%S") if hasattr(value, "strftime") else str(value)
    return rec


def create_order_record(data: dict[str, Any]) -> dict[str, Any]:
    if _use_db():
        from .db import create_order_record as _create
        return _create(data)
    recs = _load_json_file(TASK_ORDERS_FILE) or []
    existing = next((r for r in recs if r.get("task_id") == data.get("task_id")), None)
    if existing:
        return existing
    now = _ts()
    max_id = max((r.get("id", 0) for r in recs), default=0) + 1
    rec = _normalize_order({**data, "status": data.get("status") or "PENDING"})
    rec["id"] = max_id
    rec["created_at"] = now
    rec["updated_at"] = now
    recs.append(rec)
    _save_json_file(TASK_ORDERS_FILE, recs)
    return rec


def load_order_by_task(task_id: int) -> dict[str, Any] | None:
    if _use_db():
        from .db import load_order_by_task as _load
        return _load(task_id)
    recs = _load_json_file(TASK_ORDERS_FILE) or []
    row = next((r for r in recs if r.get("task_id") == task_id), None)
    return _normalize_order(row) if row else None


def load_order_by_client_id(client_order_id: str) -> dict[str, Any] | None:
    if _use_db():
        from .db import load_order_by_client_id as _load
        return _load(client_order_id)
    recs = _load_json_file(TASK_ORDERS_FILE) or []
    row = next((r for r in recs if r.get("client_order_id") == client_order_id), None)
    return _normalize_order(row) if row else None


def list_orders(symbol: str | None = None, section_id: int | None = None,
                status: str | None = None) -> list[dict[str, Any]]:
    if _use_db():
        from .db import list_orders as _list
        return _list(symbol=symbol, section_id=section_id, status=status)
    recs = _load_json_file(TASK_ORDERS_FILE) or []
    out = []
    for row in recs:
        rec = _normalize_order(row)
        if symbol and rec.get("symbol") != symbol.strip().upper():
            continue
        if section_id is not None and rec.get("section_id") != section_id:
            continue
        if status and rec.get("status") != status.strip().upper():
            continue
        out.append(rec)
    return out


def _order_event_time(rec: dict[str, Any], fields: tuple[str, ...]) -> datetime | None:
    for field in fields:
        value = rec.get(field)
        if not value:
            continue
        if isinstance(value, datetime):
            return value
        try:
            return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None


def list_filled_orders_between(start: datetime, end: datetime) -> list[dict[str, Any]]:
    if _use_db():
        from .db import list_filled_orders_between as _list
        return _list(start, end)
    fields = ("filled_at", "last_synced_at", "completed_at", "created_at")
    out = []
    for row in _load_json_file(TASK_ORDERS_FILE) or []:
        rec = _normalize_order(row)
        if rec.get("status") != "FILLED":
            continue
        ts = _order_event_time(rec, fields)
        if ts is None or not (start <= ts < end):
            continue
        out.append(rec)
    out.sort(key=lambda r: _order_event_time(r, fields) or start)
    return out


def list_partial_orders_between(start: datetime, end: datetime) -> list[dict[str, Any]]:
    if _use_db():
        from .db import list_partial_orders_between as _list
        return _list(start, end)
    fields = ("filled_at", "last_synced_at", "updated_at", "created_at")
    out = []
    for row in _load_json_file(TASK_ORDERS_FILE) or []:
        rec = _normalize_order(row)
        if rec.get("status") == "FILLED":
            continue
        try:
            filled = float(rec.get("filled_amount") or 0)
        except (TypeError, ValueError):
            filled = 0.0
        if filled <= 0:
            continue
        ts = _order_event_time(rec, fields)
        if ts is None or not (start <= ts < end):
            continue
        out.append(rec)
    out.sort(key=lambda r: _order_event_time(r, fields) or start)
    return out


def load_daily_report(report_date: Any) -> dict[str, Any] | None:
    if _use_db():
        from .db import load_daily_report as _load
        return _load(report_date)
    data = _load_json_file(DAILY_REPORTS_FILE) or {}
    return data.get(str(report_date))


def save_daily_report(report_date: Any, status: str = "SENT",
                      error: str = "") -> None:
    if _use_db():
        from .db import save_daily_report as _save
        return _save(report_date, status, error)
    data = _load_json_file(DAILY_REPORTS_FILE) or {}
    data[str(report_date)] = {
        "report_date": str(report_date),
        "status": status,
        "sent_at": _ts(),
        "error": error or "",
    }
    _atomic_save_json_file(DAILY_REPORTS_FILE, data)


def load_active_orders() -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_active_orders as _load
        return _load()
    return [r for r in list_orders() if r.get("status") in ACTIVE_ORDER_STATUSES]


def load_uncompleted_filled_orders() -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_uncompleted_filled_orders as _load
        return _load()
    return [
        r for r in list_orders(status="FILLED")
        if not r.get("completed_at")
    ]


def update_order(task_id: int, **fields: Any) -> dict[str, Any] | None:
    if _use_db():
        from .db import update_order as _update
        return _update(task_id, **fields)
    recs = _load_json_file(TASK_ORDERS_FILE) or []
    allowed = set(ORDER_FIELDS) | {"id", "created_at", "updated_at"}
    for index, row in enumerate(recs):
        if row.get("task_id") != task_id:
            continue
        for key, value in fields.items():
            if key in allowed:
                row[key] = value
        row["updated_at"] = _ts()
        row = _normalize_order(row)
        recs[index] = row
        _save_json_file(TASK_ORDERS_FILE, recs)
        return row
    return None


def delete_order(task_id: int) -> None:
    if _use_db():
        from .db import delete_order as _delete
        return _delete(task_id)
    recs = _load_json_file(TASK_ORDERS_FILE) or []
    recs = [r for r in recs if r.get("task_id") != task_id]
    _save_json_file(TASK_ORDERS_FILE, recs)


# --------------- Command Queue ---------------

def enqueue_command(command_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    if _use_db():
        from .db import enqueue_command as _enqueue
        return _enqueue(command_type, payload)
    commands = _load_json_file(COMMANDS_FILE) or []
    now = _ts()
    max_id = max((c.get("id", 0) for c in commands), default=0) + 1
    rec = {
        "id": max_id,
        "command_type": command_type,
        "payload": payload,
        "status": "PENDING",
        "result": None,
        "error": "",
        "attempts": 0,
        "locked_by": None,
        "locked_at": None,
        "created_at": now,
        "processed_at": None,
    }
    commands.append(rec)
    _save_json_file(COMMANDS_FILE, commands)
    return rec


def get_command(command_id: int) -> dict[str, Any] | None:
    if _use_db():
        from .db import get_command as _get
        return _get(command_id)
    commands = _load_json_file(COMMANDS_FILE) or []
    return next((c for c in commands if c.get("id") == command_id), None)


def claim_next_command(worker_id: str) -> dict[str, Any] | None:
    if _use_db():
        from .db import claim_next_command as _claim
        return _claim(worker_id)
    commands = _load_json_file(COMMANDS_FILE) or []
    for cmd in sorted(commands, key=lambda c: c.get("id", 0)):
        if cmd.get("status") != "PENDING":
            continue
        cmd["status"] = "PROCESSING"
        cmd["locked_by"] = worker_id
        cmd["locked_at"] = _ts()
        cmd["attempts"] = int(cmd.get("attempts") or 0) + 1
        _save_json_file(COMMANDS_FILE, commands)
        return cmd
    return None


def complete_command(command_id: int, status: str,
                     result: dict[str, Any] | None = None,
                     error: str = "") -> None:
    if _use_db():
        from .db import complete_command as _complete
        return _complete(command_id, status, result=result, error=error)
    commands = _load_json_file(COMMANDS_FILE) or []
    for cmd in commands:
        if cmd.get("id") == command_id:
            cmd["status"] = status
            cmd["result"] = result
            cmd["error"] = error
            cmd["processed_at"] = _ts()
            break
    _save_json_file(COMMANDS_FILE, commands)


def recover_stale_commands(stale_sec: float) -> int:
    if _use_db():
        from .db import recover_stale_commands as _recover
        return _recover(stale_sec)
    commands = _load_json_file(COMMANDS_FILE) or []
    cutoff = datetime.now(UTC7).timestamp() - stale_sec
    changed = 0
    for cmd in commands:
        if cmd.get("status") != "PROCESSING":
            continue
        locked = cmd.get("locked_at")
        try:
            ts = datetime.strptime(str(locked), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC7).timestamp()
        except (TypeError, ValueError):
            ts = 0
        if ts < cutoff:
            cmd["status"] = "NEEDS_REVIEW"
            cmd["error"] = "processing timeout — worker restarted?"
            cmd["processed_at"] = _ts()
            changed += 1
    if changed:
        _save_json_file(COMMANDS_FILE, commands)
    return changed


# --------------- Worker Status ---------------

def save_worker_status(worker_id: str, mode: str, version: str = "",
                       last_error: str = "") -> None:
    if _use_db():
        from .db import save_worker_status as _save
        return _save(worker_id, mode, version, last_error)
    data = _load_json_file(WORKER_STATUS_FILE) or {}
    data[worker_id] = {
        "worker_id": worker_id,
        "last_seen": _ts(),
        "version": version,
        "mode": mode,
        "last_error": last_error,
    }
    _save_json_file(WORKER_STATUS_FILE, data)


def load_worker_statuses() -> list[dict[str, Any]]:
    if _use_db():
        from .db import load_worker_statuses as _load
        return _load()
    data = _load_json_file(WORKER_STATUS_FILE) or {}
    return sorted(data.values(), key=lambda r: r.get("worker_id", ""))


def load_worker_status(worker_id: str) -> dict[str, Any] | None:
    for row in load_worker_statuses():
        if row.get("worker_id") == worker_id:
            return row
    return None
