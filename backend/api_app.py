"""Thin public API for the VPS.

This module must never import the trading engine, order rules/executor,
alert checker or realtime price feed. Every trading mutation is queued in
crypto_commands and executed by the trusted worker.
"""
import hmac
import logging
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

if __name__ == "__main__" and __package__ is None:
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    import runpy
    runpy.run_module("backend.api_app", run_name="__main__")
    sys.exit()

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, make_response, request

from .command_types import (
    ALLOWED_COMMANDS,
    CREATE_SECTION,
    DELETE_ALL_BUY,
    DELETE_ENGINE,
    DELETE_SECTION,
    DELETE_TASK,
    INIT_ENGINE,
    ORDERS_CANCEL_ALL,
    ORDERS_SYNC,
    PROCESS_PRICE,
)
from .config import (
    ADMIN_TOKEN,
    FLASK_HOST,
    FLASK_PORT,
    UTC7,
    WORKER_HEARTBEAT_SEC,
)
from .fetcher import fetch_prices_dict
from .settings_schema import (
    FIELDS,
    normalize_settings,
    parse_coin_qty,
    validate_settings,
    validate_symbol,
)
from .store import (
    enqueue_command,
    get_command,
    list_commands,
    load_all_settings,
    load_live_prices,
    load_order_by_task,
    load_settings,
    load_worker_statuses,
    requeue_command,
    requeue_order_review,
    save_settings_config,
)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, X-Admin-Token",
    "Access-Control-Max-Age": "86400",
}


@app.after_request
def cors(resp):
    for key, value in _CORS_HEADERS.items():
        resp.headers[key] = value
    return resp


def _protected_request() -> bool:
    path = request.path
    if path in (
        "/api/task-engine/orders",
        "/api/task-engine/worker",
        "/api/task-engine/commands",
    ):
        return True
    if path.startswith(("/api/task-engine/orders/",
                        "/api/task-engine/commands/")):
        return True
    return request.method in ("POST", "PUT", "PATCH", "DELETE") and path.startswith("/api/")


@app.before_request
def before_request():
    if request.method == "OPTIONS":
        resp = make_response()
        for key, value in _CORS_HEADERS.items():
            resp.headers[key] = value
        return resp

    if ADMIN_TOKEN and _protected_request():
        supplied = request.headers.get("X-Admin-Token", "")
        if not hmac.compare_digest(supplied, ADMIN_TOKEN):
            return jsonify({"error": "Unauthorized"}), 401
    return None


def _enqueue(command_type: str, payload: dict):
    if command_type not in ALLOWED_COMMANDS:
        return jsonify({"error": f"Unknown command type {command_type}"}), 400
    try:
        record = enqueue_command(command_type, payload)
    except Exception as e:
        logging.exception("enqueue %s: %s", command_type, e)
        return jsonify({"error": "Failed to enqueue command"}), 500
    return jsonify({
        "ok": True,
        "command_id": record["id"],
        "status": record.get("status", "PENDING"),
    }), 202


def _float_value(data: dict, key: str):
    value = data.get(key)
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _worker_online(rows: list[dict]) -> tuple[bool, list[dict]]:
    threshold = max(WORKER_HEARTBEAT_SEC * 3, 60.0)
    now = datetime.now(UTC7).replace(tzinfo=None)
    decorated = []
    any_online = False
    for row in rows:
        last_seen = row.get("last_seen")
        try:
            seen = datetime.strptime(str(last_seen), "%Y-%m-%d %H:%M:%S")
            online = (now - seen).total_seconds() <= threshold
        except (TypeError, ValueError):
            online = False
        any_online = any_online or online
        decorated.append({**row, "online": online})
    return any_online, decorated


@app.route("/")
def index():
    return jsonify({
        "ok": True,
        "app": "crypto-currency-thin-api",
        "endpoints": [
            "/health",
            "/api/price",
            "/api/task-engine/symbols",
            "/api/task-engine/init",
            "/api/task-engine/price",
            "/api/task-engine/info",
            "/api/task-engine/live-prices",
            "/api/task-engine/settings",
            "/api/task-engine/summary",
            "/api/task-engine/sections",
            "/api/task-engine/sections/valid-x0",
            "/api/task-engine/tasks/<id>",
            "/api/task-engine/tasks/buy-all",
            "/api/task-engine/price-broadcast",
            "/api/task-engine/price-history",
            "/api/task-engine/commands",
            "/api/task-engine/commands/<id>",
            "/api/task-engine/commands/<id>/retry",
            "/api/task-engine/orders",
            "/api/task-engine/orders/<task_id>/resync",
            "/api/task-engine/worker",
        ],
    })


@app.route("/health")
def health():
    try:
        rows = load_worker_statuses()
    except Exception as e:
        logging.warning("health worker status: %s", e)
        rows = []
    online, workers = _worker_online(rows)
    return jsonify({"ok": True, "worker_online": online, "workers": workers})


@app.route("/api/price")
def api_price():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "Missing symbol"}), 400
    try:
        prices = fetch_prices_dict([symbol])
        if symbol not in prices:
            return jsonify({
                "error": f"Could not get price for {symbol}. Binance API failed. Try again later."
            }), 404
        return jsonify({"symbol": symbol, "price": prices[symbol]})
    except Exception as e:
        logging.exception("api/price: %s", e)
        return jsonify({"error": str(e)}), 500


# --------------- Read models ---------------

@app.route("/api/task-engine/symbols")
def api_task_engine_symbols():
    from .read_models import get_all_engine_symbols
    return jsonify({"symbols": get_all_engine_symbols()})


@app.route("/api/task-engine/info")
def api_task_engine_info():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "symbol is required"}), 400
    from .read_models import get_engine_info
    return jsonify(get_engine_info(symbol))


@app.route("/api/task-engine/live-prices")
def api_live_prices():
    try:
        return jsonify(load_live_prices() or {})
    except Exception as e:
        logging.warning("live-prices: %s", e)
        return jsonify({})


@app.route("/api/task-engine/sections/valid-x0")
def api_sections_valid_x0():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "symbol required"}), 400
    from .grid import get_valid_x0_for_symbol
    return jsonify(get_valid_x0_for_symbol(symbol))


@app.route("/api/task-engine/sections", methods=["GET"])
def api_sections_list():
    symbol = (request.args.get("symbol") or "").strip().upper() or None
    from .read_models import list_sections
    return jsonify({"sections": list_sections(symbol)})


@app.route("/api/task-engine/sections/<int:section_id>", methods=["GET"])
def api_section_info(section_id):
    from .read_models import get_section_info
    return jsonify(get_section_info(section_id))


@app.route("/api/task-engine/summary")
def api_task_engine_summary():
    from .read_models import build_summary
    return jsonify({"summary": build_summary()})


@app.route("/api/task-engine/price-history")
def api_price_history():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "symbol is required"}), 400
    try:
        limit = int(request.args.get("limit", 20))
    except (TypeError, ValueError):
        limit = 20
    from .read_models import get_price_history
    return jsonify({"history": get_price_history(symbol, limit)})


# --------------- Settings (direct, not trading logic) ---------------

@app.route("/api/task-engine/settings", methods=["GET"])
def api_task_engine_settings_get():
    return jsonify({"settings": load_all_settings()})


@app.route("/api/task-engine/settings", methods=["POST"])
def api_task_engine_settings_save():
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "symbol is required"}), 400
    symbol_error = validate_symbol(symbol)
    if symbol_error:
        return jsonify({"error": symbol_error}), 400

    updates = {field: data[field] for field in FIELDS
               if field in data and data[field] is not None}
    merged = normalize_settings(symbol, load_settings(symbol))
    merged.update(updates)
    error = validate_settings(merged, partial=False)
    if error:
        return jsonify({"error": error}), 400
    for field in FIELDS:
        if field in merged:
            updates[field] = merged[field]

    try:
        saved = save_settings_config(symbol, updates)
    except Exception as e:
        logging.exception("save settings: %s", e)
        return jsonify({"error": "Failed to save settings"}), 500
    return jsonify({"ok": True, "settings": normalize_settings(symbol, saved)})


# --------------- Command queue ---------------

@app.route("/api/task-engine/commands", methods=["POST"])
def api_enqueue_command():
    data = request.get_json(silent=True) or {}
    command_type = (data.get("type") or "").strip()
    payload = data.get("payload") or {}
    if not isinstance(payload, dict):
        return jsonify({"error": "payload must be an object"}), 400
    return _enqueue(command_type, payload)


@app.route("/api/task-engine/commands", methods=["GET"])
def api_list_commands():
    status = (request.args.get("status") or "").strip().upper() or None
    try:
        limit = int(request.args.get("limit") or 100)
    except (TypeError, ValueError):
        limit = 100
    try:
        rows = list_commands(status=status, limit=limit)
    except Exception as e:
        logging.exception("list commands: %s", e)
        return jsonify({"error": "Failed to list commands"}), 500
    return jsonify({"commands": rows})


@app.route("/api/task-engine/commands/<int:command_id>", methods=["GET"])
def api_get_command(command_id):
    command = get_command(command_id)
    if command is None:
        return jsonify({"error": "Command not found"}), 404
    return jsonify(command)


@app.route("/api/task-engine/commands/<int:command_id>/retry", methods=["POST"])
def api_retry_command(command_id):
    command = get_command(command_id)
    if command is None:
        return jsonify({"error": "Command not found"}), 404
    if str(command.get("status") or "").upper() not in ("NEEDS_REVIEW", "FAILED"):
        return jsonify({
            "error": f"Command status {command.get('status')} cannot be retried"
        }), 409
    updated = requeue_command(command_id)
    if updated is None:
        return jsonify({"error": "Failed to queue command"}), 409
    return jsonify({"ok": True, "command": updated})


@app.route("/api/task-engine/orders/<int:task_id>/resync", methods=["POST"])
def api_resync_order(task_id):
    order = load_order_by_task(task_id)
    if order is None:
        return jsonify({"error": "Order not found"}), 404
    status = str(order.get("status") or "").upper()
    accounting = str(order.get("accounting_status") or "").upper()
    if status != "NEEDS_REVIEW" and accounting != "NEEDS_REVIEW":
        return jsonify({"error": "Order does not need review"}), 409
    updated = requeue_order_review(task_id)
    if updated is None:
        return jsonify({"error": "Order not found"}), 404
    return jsonify({"ok": True, "order": updated})


@app.route("/api/task-engine/orders", methods=["GET"])
def api_orders():
    from .store import list_orders
    symbol = (request.args.get("symbol") or "").strip().upper() or None
    status = (request.args.get("status") or "").strip().upper() or None
    section_id = request.args.get("section_id")
    review_only = (request.args.get("review") or "").strip().lower() in (
        "1", "true", "yes", "on",
    )
    try:
        section_id = int(section_id) if section_id is not None else None
    except (TypeError, ValueError):
        section_id = None
    return jsonify({"orders": list_orders(
        symbol=symbol, section_id=section_id, status=status,
        review_only=review_only,
    )})


@app.route("/api/task-engine/worker", methods=["GET"])
def api_worker():
    try:
        rows = load_worker_statuses()
    except Exception as e:
        logging.warning("worker status: %s", e)
        rows = []
    online, workers = _worker_online(rows)
    return jsonify({"online": online, "workers": workers})


# --------------- Legacy mutation routes -> commands ---------------

@app.route("/api/task-engine/init", methods=["POST"])
def api_task_engine_init():
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    x0 = _float_value(data, "x0")
    coin_qty = _float_value(data, "coin_qty") or 0.0
    if not symbol or x0 is None:
        return jsonify({"error": "symbol and x0 are required"}), 400
    return _enqueue(INIT_ENGINE, {"symbol": symbol, "x0": x0, "coin_qty": coin_qty})


@app.route("/api/task-engine/price", methods=["POST"])
def api_task_engine_price():
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    price = _float_value(data, "price")
    if not symbol or price is None:
        return jsonify({"error": "symbol and price are required"}), 400
    return _enqueue(PROCESS_PRICE, {"symbol": symbol, "price": price})


@app.route("/api/task-engine/sections", methods=["POST"])
def api_sections_create():
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    name = (data.get("name") or "").strip()
    x0 = _float_value(data, "x0")
    if not symbol or not name or x0 is None:
        return jsonify({"error": "symbol, name, and x0 are required"}), 400
    symbol_error = validate_symbol(symbol)
    if symbol_error:
        return jsonify({"error": symbol_error}), 400
    coin_qty, qty_error = parse_coin_qty(data.get("coin_qty"))
    if qty_error:
        return jsonify({"error": qty_error}), 400
    return _enqueue(CREATE_SECTION, {
        "symbol": symbol,
        "name": name,
        "x0": x0,
        "coin_qty": str(coin_qty),
        "request_id": str(uuid4()),
    })


@app.route("/api/task-engine/sections/<int:section_id>", methods=["DELETE"])
def api_section_delete(section_id):
    return _enqueue(DELETE_SECTION, {"section_id": section_id})


@app.route("/api/task-engine/price-broadcast", methods=["POST"])
def api_price_broadcast():
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper()
    price = _float_value(data, "price")
    if not symbol or price is None:
        return jsonify({"error": "symbol and price are required"}), 400
    return _enqueue(PROCESS_PRICE, {"symbol": symbol, "price": price})


@app.route("/api/task-engine/engine/<symbol>", methods=["DELETE"])
def api_delete_engine(symbol):
    sym = symbol.strip().upper()
    if not sym:
        return jsonify({"error": "symbol is required"}), 400
    return _enqueue(DELETE_ENGINE, {"symbol": sym})


@app.route("/api/task-engine/tasks/<int:task_id>", methods=["DELETE"])
def api_delete_task(task_id):
    return _enqueue(DELETE_TASK, {"task_id": task_id})


@app.route("/api/task-engine/tasks/buy-all", methods=["DELETE"])
def api_delete_all_buy_tasks():
    symbol = (request.args.get("symbol") or "").strip().upper()
    if not symbol:
        return jsonify({"error": "symbol is required"}), 400
    return _enqueue(DELETE_ALL_BUY, {"symbol": symbol})


@app.route("/api/task-engine/orders/sync", methods=["POST"])
def api_orders_sync():
    return _enqueue(ORDERS_SYNC, {})


@app.route("/api/task-engine/orders/cancel-all", methods=["POST"])
def api_orders_cancel_all():
    data = request.get_json(silent=True) or {}
    symbol = (data.get("symbol") or "").strip().upper() or None
    return _enqueue(ORDERS_CANCEL_ALL, {"symbol": symbol})


@app.route("/api/check", methods=["GET", "POST"])
def api_run_check():
    return jsonify({
        "ok": False,
        "error": "Alert checker runs on the trusted worker",
    }), 410


if __name__ == "__main__":
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False)
