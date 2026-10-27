import atexit
import logging
import os
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from .config import UTC7

logger = logging.getLogger(__name__)


def _dec_str(value: Any) -> str | None:
    """Serialize a NUMERIC to a plain decimal string without exponent."""
    if value is None:
        return None
    try:
        text = format(Decimal(str(value)), "f")
    except (InvalidOperation, ValueError):
        return None
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

_pool = None
_pool_lock = threading.Lock()


def _get_dsn() -> str:
    url = DATABASE_URL
    if not url:
        raise ValueError("DATABASE_URL is not set")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if "sslmode" not in url and "postgresql" in url:
        url = url + ("&" if "?" in url else "?") + "sslmode=require"
    return url


def _get_pool():
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        from psycopg2.pool import ThreadedConnectionPool
        # Bound every connection so a killed/interrupted request cannot leave a
        # transaction holding DDL locks forever. Supabase/Supavisor may keep a
        # backend "idle in transaction" after a client is killed; the idle
        # timeout lets PostgreSQL clean it up.
        _pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=_get_dsn(),
            connect_timeout=10,
            options=(
                "-c statement_timeout=25000 "
                "-c lock_timeout=10000 "
                "-c idle_in_transaction_session_timeout=15000"
            ),
        )
        atexit.register(_shutdown_pool)
        logger.info("DB connection pool created (min=1, max=5)")
        return _pool


def _shutdown_pool():
    global _pool
    if _pool is not None:
        try:
            _pool.closeall()
        except Exception:
            pass
        _pool = None


@contextmanager
def _cursor():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        cur = conn.cursor()
        try:
            # Supavisor/Supabase may ignore libpq startup options, so enforce
            # the bounds inside every transaction as well.
            cur.execute(
                "SET LOCAL statement_timeout = '25s'; "
                "SET LOCAL lock_timeout = '10s'; "
                "SET LOCAL idle_in_transaction_session_timeout = '15s'"
            )
            yield cur
            conn.commit()
        except BaseException:
            # KeyboardInterrupt/SystemExit must not return an open transaction
            # to the pool (it can hold DDL locks indefinitely).
            conn.rollback()
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass
    finally:
        pool.putconn(conn)


_schema_initialized = False
_SCHEMA_LOCK_ID = 773311
# Bump this whenever the DDL below changes. Processes that see the current
# version skip all DDL, keeping /health and worker startup fast.
_SCHEMA_VERSION = "2026-10-27-1"


def init_schema() -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    with _cursor() as cur:
        # Serialize migrations across API/worker processes. Without this,
        # several processes running ALTER/CREATE INDEX can block each other
        # and leave stale locks if one is killed mid-migration.
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_LOCK_ID,))
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_schema_meta (
                id INTEGER PRIMARY KEY,
                version VARCHAR(64) NOT NULL DEFAULT ''
            );
        """)
        cur.execute("SELECT version FROM crypto_schema_meta WHERE id = 1")
        version_row = cur.fetchone()
        if version_row and version_row[0] == _SCHEMA_VERSION:
            _schema_initialized = True
            return
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_task_engine_state (
                symbol VARCHAR(20) PRIMARY KEY,
                x0 NUMERIC NOT NULL,
                current_x NUMERIC NOT NULL,
                current_pct NUMERIC NOT NULL DEFAULT 0,
                seeded BOOLEAN NOT NULL DEFAULT FALSE,
                coin_qty NUMERIC NOT NULL DEFAULT 0,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        try:
            cur.execute("ALTER TABLE crypto_task_engine_state ADD COLUMN IF NOT EXISTS coin_qty NUMERIC NOT NULL DEFAULT 0")
        except Exception:
            pass
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_task_queue (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                direction VARCHAR(4) NOT NULL,
                target_pct NUMERIC NOT NULL,
                action VARCHAR(10) NOT NULL DEFAULT 'BUY',
                note TEXT DEFAULT '',
                sibling_id INTEGER,
                sell_origin VARCHAR(20) DEFAULT '',
                quote_budget NUMERIC,
                spawn_key VARCHAR(100),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_queue_symbol ON crypto_task_queue(symbol);")
        try:
            cur.execute("ALTER TABLE crypto_task_queue ADD COLUMN IF NOT EXISTS sibling_id INTEGER")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE crypto_task_queue ADD COLUMN IF NOT EXISTS sell_origin VARCHAR(20) DEFAULT ''")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE crypto_task_queue ADD COLUMN IF NOT EXISTS quote_budget NUMERIC")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE crypto_task_queue ADD COLUMN IF NOT EXISTS spawn_key VARCHAR(100)")
        except Exception:
            pass
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_crypto_task_queue_spawn_key "
            "ON crypto_task_queue(spawn_key) WHERE spawn_key IS NOT NULL"
        )
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_task_passed (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                direction VARCHAR(4) NOT NULL,
                action VARCHAR(10) NOT NULL DEFAULT 'BUY',
                target_pct NUMERIC NOT NULL,
                hit_pct NUMERIC NOT NULL,
                hit_price NUMERIC NOT NULL,
                note TEXT DEFAULT '',
                task_id INTEGER,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_passed_symbol ON crypto_task_passed(symbol);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_passed_at ON crypto_task_passed(created_at DESC);")
        try:
            cur.execute("ALTER TABLE crypto_task_passed ADD COLUMN IF NOT EXISTS task_id INTEGER")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE crypto_task_passed ADD COLUMN IF NOT EXISTS sell_origin VARCHAR(20) DEFAULT ''")
        except Exception:
            pass

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_task_closed (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                closed_task_id INTEGER NOT NULL,
                sibling_triggered_id INTEGER,
                direction VARCHAR(4) NOT NULL,
                action VARCHAR(10) NOT NULL,
                target_pct NUMERIC NOT NULL,
                at_pct NUMERIC NOT NULL,
                at_price NUMERIC NOT NULL,
                reason TEXT DEFAULT '',
                note TEXT DEFAULT '',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_closed_symbol ON crypto_task_closed(symbol);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_closed_at ON crypto_task_closed(created_at DESC);")
        try:
            cur.execute("ALTER TABLE crypto_task_closed ALTER COLUMN sibling_triggered_id DROP NOT NULL")
        except Exception:
            pass

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_live_prices (
                symbol VARCHAR(20) PRIMARY KEY,
                price NUMERIC NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_settings (
                symbol VARCHAR(20) PRIMARY KEY,
                sell_down_pct NUMERIC NOT NULL DEFAULT 50,
                sell_up_pct NUMERIC NOT NULL DEFAULT 50,
                sell_down_offset_pct NUMERIC NOT NULL DEFAULT -2.0,
                sell_up_offset_pct NUMERIC NOT NULL DEFAULT 2.0,
                buy_after_sell_down_offset_pct NUMERIC NOT NULL DEFAULT -2.0,
                buy_after_sell_up_offset_pct NUMERIC NOT NULL DEFAULT -2.0,
                supplement_sell_up_offset_pct NUMERIC NOT NULL DEFAULT 2.0,
                grid_step_pct NUMERIC NOT NULL DEFAULT 3.0,
                buy_cancel_distance_pct NUMERIC NOT NULL DEFAULT 35.0
            );
        """)
        for column, default in (
            ("sell_down_offset_pct", "-2.0"),
            ("sell_up_offset_pct", "2.0"),
            ("buy_after_sell_down_offset_pct", "-2.0"),
            ("buy_after_sell_up_offset_pct", "-2.0"),
            ("supplement_sell_up_offset_pct", "2.0"),
            ("grid_step_pct", "3.0"),
            ("buy_cancel_distance_pct", "35.0"),
        ):
            try:
                cur.execute(
                    f"ALTER TABLE crypto_settings ADD COLUMN IF NOT EXISTS {column} NUMERIC NOT NULL DEFAULT {default}"
                )
            except Exception:
                pass

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_sections (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                x0 NUMERIC NOT NULL,
                coin_qty NUMERIC NOT NULL DEFAULT 0,
                quote_balance NUMERIC NOT NULL DEFAULT 0,
                current_x NUMERIC NOT NULL,
                current_pct NUMERIC NOT NULL DEFAULT 0,
                seeded BOOLEAN NOT NULL DEFAULT FALSE,
                request_id VARCHAR(64),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        try:
            cur.execute("ALTER TABLE crypto_sections ADD COLUMN IF NOT EXISTS request_id VARCHAR(64)")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE crypto_sections ADD COLUMN IF NOT EXISTS quote_balance NUMERIC NOT NULL DEFAULT 0")
        except Exception:
            pass
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_crypto_sections_request_id "
            "ON crypto_sections(request_id) WHERE request_id IS NOT NULL"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_sections_symbol ON crypto_sections(symbol);")

        try:
            cur.execute("ALTER TABLE crypto_task_queue ADD COLUMN IF NOT EXISTS section_id INTEGER")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE crypto_task_passed ADD COLUMN IF NOT EXISTS section_id INTEGER")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE crypto_task_closed ADD COLUMN IF NOT EXISTS section_id INTEGER")
        except Exception:
            pass

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_price_history (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                price NUMERIC NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_price_history_symbol ON crypto_price_history(symbol);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_price_history_at ON crypto_price_history(created_at DESC);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_daily_reports (
                report_date DATE PRIMARY KEY,
                status VARCHAR(20) NOT NULL DEFAULT 'SENT',
                sent_at TIMESTAMP,
                error TEXT DEFAULT ''
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_task_orders (
                id SERIAL PRIMARY KEY,
                task_id INTEGER NOT NULL UNIQUE,
                section_id INTEGER NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                exchange_symbol VARCHAR(20) NOT NULL,
                client_order_id VARCHAR(40) NOT NULL UNIQUE,
                exchange_order_id VARCHAR(64),
                side VARCHAR(4) NOT NULL,
                direction VARCHAR(4) NOT NULL,
                order_type VARCHAR(24) NOT NULL,
                amount NUMERIC NOT NULL,
                price NUMERIC NOT NULL,
                stop_price NUMERIC,
                target_pct NUMERIC NOT NULL,
                target_price NUMERIC NOT NULL,
                sell_origin VARCHAR(20) DEFAULT '',
                status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
                filled_amount NUMERIC NOT NULL DEFAULT 0,
                avg_fill_price NUMERIC,
                filled_base NUMERIC,
                filled_quote NUMERIC,
                fee_base NUMERIC NOT NULL DEFAULT 0,
                fee_quote NUMERIC NOT NULL DEFAULT 0,
                fee_other NUMERIC NOT NULL DEFAULT 0,
                fee_other_currency VARCHAR(20) DEFAULT '',
                quote_budget NUMERIC,
                quote_spent NUMERIC NOT NULL DEFAULT 0,
                quote_settled_at TIMESTAMP,
                trades_synced_filled NUMERIC NOT NULL DEFAULT 0,
                accounting_status VARCHAR(20) DEFAULT 'PENDING',
                accounting_applied_at TIMESTAMP,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_retry_at TIMESTAMP,
                last_error TEXT DEFAULT '',
                last_synced_at TIMESTAMP,
                completed_at TIMESTAMP,
                filled_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_orders_section ON crypto_task_orders(section_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_orders_symbol ON crypto_task_orders(symbol);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_orders_status ON crypto_task_orders(status);")
        for column, ddl in (
            ("filled_base", "NUMERIC"),
            ("filled_quote", "NUMERIC"),
            ("fee_base", "NUMERIC NOT NULL DEFAULT 0"),
            ("fee_quote", "NUMERIC NOT NULL DEFAULT 0"),
            ("fee_other", "NUMERIC NOT NULL DEFAULT 0"),
            ("fee_other_currency", "VARCHAR(20) DEFAULT ''"),
            ("quote_budget", "NUMERIC"),
            ("quote_spent", "NUMERIC NOT NULL DEFAULT 0"),
            ("quote_settled_at", "TIMESTAMP"),
            ("trades_synced_filled", "NUMERIC NOT NULL DEFAULT 0"),
            ("accounting_status", "VARCHAR(20) DEFAULT 'PENDING'"),
            ("accounting_applied_at", "TIMESTAMP"),
            ("filled_at", "TIMESTAMP"),
        ):
            try:
                cur.execute(
                    f"ALTER TABLE crypto_task_orders ADD COLUMN IF NOT EXISTS {column} {ddl}"
                )
            except Exception:
                pass

        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_orders_filled_at ON crypto_task_orders(filled_at);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_task_orders_updated_at ON crypto_task_orders(updated_at);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_commands (
                id SERIAL PRIMARY KEY,
                command_type VARCHAR(32) NOT NULL,
                payload JSONB NOT NULL,
                status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                result JSONB,
                error TEXT DEFAULT '',
                attempts INTEGER NOT NULL DEFAULT 0,
                locked_by VARCHAR(64),
                locked_at TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crypto_commands_status ON crypto_commands(status, id);")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_worker_status (
                worker_id VARCHAR(64) PRIMARY KEY,
                last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                version VARCHAR(32) DEFAULT '',
                mode VARCHAR(16) DEFAULT '',
                last_error TEXT DEFAULT ''
            );
        """)
        cur.execute(
            """INSERT INTO crypto_schema_meta (id, version) VALUES (1, %s)
               ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version""",
            (_SCHEMA_VERSION,),
        )
    _schema_initialized = True


# --------------- Task Engine State ---------------

def load_task_engine_state(symbol: str) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT symbol, x0, current_x, current_pct, seeded, coin_qty FROM crypto_task_engine_state WHERE UPPER(symbol) = UPPER(%s)",
                (symbol.strip(),),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {
                "symbol": row[0],
                "x0": float(row[1]),
                "current_x": float(row[2]),
                "current_pct": float(row[3]),
                "seeded": bool(row[4]),
                "coin_qty": float(row[5]) if row[5] is not None else 0.0,
            }
    except Exception as e:
        logger.warning("db load_task_engine_state: %s", e)
        return None


def save_task_engine_state(symbol: str, state: dict[str, Any]) -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                """INSERT INTO crypto_task_engine_state (symbol, x0, current_x, current_pct, seeded, coin_qty, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (symbol) DO UPDATE SET
                     x0 = EXCLUDED.x0,
                     current_x = EXCLUDED.current_x,
                     current_pct = EXCLUDED.current_pct,
                     seeded = EXCLUDED.seeded,
                     coin_qty = EXCLUDED.coin_qty,
                     updated_at = EXCLUDED.updated_at""",
                (
                    state["symbol"],
                    state["x0"],
                    state["current_x"],
                    state["current_pct"],
                    state["seeded"],
                    state.get("coin_qty", 0),
                    datetime.now(UTC7).replace(tzinfo=None),
                ),
            )
    except Exception as e:
        logger.warning("db save_task_engine_state: %s", e)


def load_all_task_engine_symbols() -> list[str]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute("SELECT symbol FROM crypto_task_engine_state ORDER BY symbol")
            out = [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.warning("db load_all_task_engine_symbols: %s", e)
    return out


# --------------- Task Queue ---------------

def load_task_queue(symbol: str) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT id, symbol, direction, target_pct, action, note, sibling_id, sell_origin, quote_budget, spawn_key FROM crypto_task_queue WHERE UPPER(symbol) = UPPER(%s)",
                (symbol.strip(),),
            )
            for row in cur.fetchall():
                out.append({
                    "id": row[0],
                    "symbol": row[1],
                    "direction": row[2],
                    "target_pct": float(row[3]),
                    "action": row[4],
                    "note": row[5] or "",
                    "sibling_id": row[6],
                    "sell_origin": row[7] or "",
                    "quote_budget": _dec_str(row[8]),
                    "spawn_key": row[9] or "",
                })
    except Exception as e:
        logger.warning("db load_task_queue: %s", e)
    return out


def add_task_to_queue(symbol: str, direction: str, target_pct: float,
                      action: str, note: str,
                      sibling_id: int | None = None,
                      sell_origin: str = "") -> dict[str, Any] | None:
    if target_pct < -98 or target_pct > 98:
        return None
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "INSERT INTO crypto_task_queue (symbol, direction, target_pct, action, note, sibling_id, sell_origin) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (symbol.strip().upper(), direction, target_pct, action, note, sibling_id, sell_origin),
            )
            row = cur.fetchone()
            return {
                "id": row[0],
                "symbol": symbol.strip().upper(),
                "direction": direction,
                "target_pct": target_pct,
                "action": action,
                "note": note,
                "sibling_id": sibling_id,
                "sell_origin": sell_origin,
            }
    except Exception as e:
        logger.warning("db add_task_to_queue: %s", e)
        return None


def update_task_sibling_id(task_id: int, sibling_id: int) -> None:
    try:
        with _cursor() as cur:
            cur.execute(
                "UPDATE crypto_task_queue SET sibling_id = %s WHERE id = %s",
                (sibling_id, task_id),
            )
    except Exception as e:
        logger.warning("db update_task_sibling_id: %s", e)


def load_task_by_id(task_id: int) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT id, symbol, direction, target_pct, action, note, sibling_id, sell_origin, section_id, quote_budget, spawn_key "
                "FROM crypto_task_queue WHERE id = %s",
                (task_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {
                "id": row[0],
                "symbol": row[1],
                "direction": row[2],
                "target_pct": float(row[3]),
                "action": row[4],
                "note": row[5] or "",
                "sibling_id": row[6],
                "sell_origin": row[7] or "",
                "section_id": row[8],
                "quote_budget": _dec_str(row[9]),
                "spawn_key": row[10] or "",
            }
    except Exception as e:
        logger.warning("db load_task_by_id: %s", e)
    return None


def remove_task_from_queue(task_id: int) -> None:
    try:
        with _cursor() as cur:
            cur.execute("DELETE FROM crypto_task_queue WHERE id = %s", (task_id,))
    except Exception as e:
        logger.warning("db remove_task_from_queue: %s", e)


def delete_all_buy_tasks_for_symbol(symbol: str) -> int:
    """Delete all BUY tasks for a symbol. Returns number of rows deleted."""
    try:
        with _cursor() as cur:
            cur.execute(
                "DELETE FROM crypto_task_queue WHERE symbol = %s AND action = 'BUY'",
                (symbol.strip().upper(),),
            )
            return cur.rowcount or 0
    except Exception as e:
        logger.warning("db delete_all_buy_tasks_for_symbol: %s", e)
    return 0


def clear_task_queue_for_symbol(symbol: str) -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute("DELETE FROM crypto_task_queue WHERE UPPER(symbol) = UPPER(%s)", (symbol.strip(),))
    except Exception as e:
        logger.warning("db clear_task_queue_for_symbol: %s", e)


# --------------- Passed Tasks ---------------

def add_passed_task(symbol: str, direction: str, action: str, target_pct: float,
                    hit_pct: float, hit_price: float, note: str,
                    task_id: int | None = None,
                    sell_origin: str = "") -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                """INSERT INTO crypto_task_passed (symbol, task_id, direction, action, target_pct, hit_pct, hit_price, note, sell_origin, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (symbol.strip().upper(), task_id, direction, action, target_pct, hit_pct, hit_price, note,
                 sell_origin, datetime.now(UTC7).replace(tzinfo=None)),
            )
    except Exception as e:
        logger.warning("db add_passed_task: %s", e)


def load_passed_tasks(symbol: str) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                """SELECT id, symbol, direction, action, target_pct, hit_pct, hit_price, note, created_at, task_id, sell_origin
                   FROM crypto_task_passed WHERE UPPER(symbol) = UPPER(%s) ORDER BY created_at DESC LIMIT 200""",
                (symbol.strip(),),
            )
            for row in cur.fetchall():
                out.append({
                    "id": row[0],
                    "symbol": row[1],
                    "direction": row[2],
                    "action": row[3],
                    "target_pct": float(row[4]),
                    "hit_pct": float(row[5]),
                    "hit_price": float(row[6]),
                    "note": row[7] or "",
                    "at": row[8].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[8], "strftime") else str(row[8]),
                    "task_id": row[9] if len(row) > 9 else None,
                    "sell_origin": row[10] or "" if len(row) > 10 else "",
                })
    except Exception as e:
        logger.warning("db load_passed_tasks: %s", e)
    return out


def clear_passed_tasks_for_symbol(symbol: str) -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute("DELETE FROM crypto_task_passed WHERE UPPER(symbol) = UPPER(%s)", (symbol.strip(),))
    except Exception as e:
        logger.warning("db clear_passed_tasks_for_symbol: %s", e)


# --------------- Closed Tasks (sibling cancelled) ---------------

def add_closed_task(symbol: str, closed_task_id: int,
                    sibling_triggered_id: int | None, direction: str, action: str,
                    target_pct: float, at_pct: float, at_price: float,
                    reason: str, note: str) -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                """INSERT INTO crypto_task_closed
                   (symbol, closed_task_id, sibling_triggered_id, direction, action,
                    target_pct, at_pct, at_price, reason, note, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (symbol.strip().upper(), closed_task_id, sibling_triggered_id,
                 direction, action, target_pct, at_pct, at_price, reason, note,
                 datetime.now(UTC7).replace(tzinfo=None)),
            )
    except Exception as e:
        logger.warning("db add_closed_task: %s", e)


def load_closed_tasks(symbol: str) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                """SELECT id, symbol, closed_task_id, sibling_triggered_id,
                          direction, action, target_pct, at_pct, at_price,
                          reason, note, created_at
                   FROM crypto_task_closed
                   WHERE UPPER(symbol) = UPPER(%s)
                   ORDER BY created_at DESC LIMIT 200""",
                (symbol.strip(),),
            )
            for row in cur.fetchall():
                out.append({
                    "id": row[0],
                    "symbol": row[1],
                    "closed_task_id": row[2],
                    "sibling_triggered_id": row[3],
                    "direction": row[4],
                    "action": row[5],
                    "target_pct": float(row[6]),
                    "at_pct": float(row[7]),
                    "at_price": float(row[8]),
                    "reason": row[9] or "",
                    "note": row[10] or "",
                    "at": row[11].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[11], "strftime") else str(row[11]),
                })
    except Exception as e:
        logger.warning("db load_closed_tasks: %s", e)
    return out


def clear_closed_tasks_for_symbol(symbol: str) -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute("DELETE FROM crypto_task_closed WHERE UPPER(symbol) = UPPER(%s)", (symbol.strip(),))
    except Exception as e:
        logger.warning("db clear_closed_tasks_for_symbol: %s", e)


# --------------- Live Prices ---------------

def save_live_prices(prices: dict[str, float]) -> None:
    if not prices:
        return
    try:
        init_schema()
        now = datetime.now(UTC7).replace(tzinfo=None)
        values = [(s.strip().upper(), p, now) for s, p in prices.items()]
        with _cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(
                cur,
                """INSERT INTO crypto_live_prices (symbol, price, updated_at)
                   VALUES %s
                   ON CONFLICT (symbol) DO UPDATE SET
                     price = EXCLUDED.price,
                     updated_at = EXCLUDED.updated_at""",
                values,
                template="(%s, %s, %s)",
            )
    except Exception as e:
        logger.warning("db save_live_prices: %s", e)


def load_live_prices() -> dict[str, float]:
    out = {}
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute("SELECT symbol, price FROM crypto_live_prices")
            for row in cur.fetchall():
                out[row[0]] = float(row[1])
    except Exception as e:
        logger.warning("db load_live_prices: %s", e)
    return out


# --------------- Batched info query (single connection) ---------------

def load_engine_info_batched(symbol: str) -> dict[str, Any]:
    """Load state + queue + passed + closed in a single DB connection."""
    result: dict[str, Any] = {
        "state": None, "tasks": [], "passed": [], "closed": [],
    }
    try:
        init_schema()
        sym = symbol.strip()
        with _cursor() as cur:
            cur.execute(
                "SELECT symbol, x0, current_x, current_pct, seeded, coin_qty FROM crypto_task_engine_state WHERE UPPER(symbol) = UPPER(%s)",
                (sym,),
            )
            row = cur.fetchone()
            if row:
                result["state"] = {
                    "symbol": row[0],
                    "x0": float(row[1]),
                    "current_x": float(row[2]),
                    "current_pct": float(row[3]),
                    "seeded": bool(row[4]),
                    "coin_qty": float(row[5]) if row[5] is not None else 0.0,
                }

            cur.execute(
                "SELECT id, symbol, direction, target_pct, action, note, sibling_id, sell_origin, quote_budget, spawn_key FROM crypto_task_queue WHERE UPPER(symbol) = UPPER(%s)",
                (sym,),
            )
            for row in cur.fetchall():
                result["tasks"].append({
                    "id": row[0], "symbol": row[1], "direction": row[2],
                    "target_pct": float(row[3]), "action": row[4],
                    "note": row[5] or "", "sibling_id": row[6],
                    "sell_origin": row[7] or "",
                    "quote_budget": _dec_str(row[8]),
                    "spawn_key": row[9] or "",
                })

            cur.execute(
                """SELECT id, symbol, direction, action, target_pct, hit_pct, hit_price, note, created_at, task_id, sell_origin
                   FROM crypto_task_passed WHERE UPPER(symbol) = UPPER(%s) ORDER BY created_at DESC LIMIT 200""",
                (sym,),
            )
            for row in cur.fetchall():
                result["passed"].append({
                    "id": row[0], "symbol": row[1], "direction": row[2],
                    "action": row[3], "target_pct": float(row[4]),
                    "hit_pct": float(row[5]), "hit_price": float(row[6]),
                    "note": row[7] or "",
                    "at": row[8].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[8], "strftime") else str(row[8]),
                    "task_id": row[9] if len(row) > 9 else None,
                    "sell_origin": row[10] or "" if len(row) > 10 else "",
                })

            cur.execute(
                """SELECT id, symbol, closed_task_id, sibling_triggered_id,
                          direction, action, target_pct, at_pct, at_price,
                          reason, note, created_at
                   FROM crypto_task_closed WHERE UPPER(symbol) = UPPER(%s)
                   ORDER BY created_at DESC LIMIT 200""",
                (sym,),
            )
            for row in cur.fetchall():
                result["closed"].append({
                    "id": row[0], "symbol": row[1],
                    "closed_task_id": row[2], "sibling_triggered_id": row[3],
                    "direction": row[4], "action": row[5],
                    "target_pct": float(row[6]), "at_pct": float(row[7]),
                    "at_price": float(row[8]),
                    "reason": row[9] or "", "note": row[10] or "",
                    "at": row[11].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[11], "strftime") else str(row[11]),
                })
    except Exception as e:
        logger.warning("db load_engine_info_batched: %s", e)
    return result


# --------------- Settings ---------------

SETTINGS_COLUMNS = (
    "sell_down_pct", "sell_up_pct", "sell_down_offset_pct",
    "sell_up_offset_pct", "buy_after_sell_down_offset_pct",
    "buy_after_sell_up_offset_pct", "supplement_sell_up_offset_pct",
    "grid_step_pct", "buy_cancel_distance_pct",
)


def _settings_row(row) -> dict[str, Any]:
    out = {"symbol": row[0]}
    for i, field in enumerate(SETTINGS_COLUMNS, start=1):
        out[field] = float(row[i])
    return out


def load_settings(symbol: str) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"SELECT symbol, {', '.join(SETTINGS_COLUMNS)} FROM crypto_settings WHERE UPPER(symbol) = UPPER(%s)",
                (symbol.strip(),),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return _settings_row(row)
    except Exception as e:
        logger.warning("db load_settings: %s", e)
        return None


def save_settings_config(symbol: str, values: dict[str, Any]) -> None:
    try:
        init_schema()
        columns = ", ".join(SETTINGS_COLUMNS)
        placeholders = ", ".join(["%s"] * len(SETTINGS_COLUMNS))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in SETTINGS_COLUMNS)
        params = [symbol.strip().upper()] + [values[c] for c in SETTINGS_COLUMNS]
        with _cursor() as cur:
            cur.execute(
                f"""INSERT INTO crypto_settings (symbol, {columns})
                    VALUES (%s, {placeholders})
                    ON CONFLICT (symbol) DO UPDATE SET {updates}""",
                params,
            )
    except Exception as e:
        logger.warning("db save_settings_config: %s", e)


def save_settings(symbol: str, sell_down_pct: float, sell_up_pct: float) -> None:
    """Backward-compatible 2-column save; other settings are preserved."""
    existing = load_settings(symbol)
    if existing is None:
        from .settings_schema import normalize_settings
        existing = normalize_settings(symbol, None)
    existing["sell_down_pct"] = sell_down_pct
    existing["sell_up_pct"] = sell_up_pct
    save_settings_config(symbol, existing)


def load_all_settings() -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"SELECT symbol, {', '.join(SETTINGS_COLUMNS)} FROM crypto_settings ORDER BY symbol"
            )
            for row in cur.fetchall():
                out.append(_settings_row(row))
    except Exception as e:
        logger.warning("db load_all_settings: %s", e)
    return out


def ensure_settings(symbol: str) -> dict[str, Any]:
    """Create settings with full defaults if none exist for the symbol."""
    existing = load_settings(symbol)
    if existing:
        return existing
    from .settings_schema import normalize_settings
    full = normalize_settings(symbol, None)
    save_settings_config(symbol, full)
    return full


# --------------- Sections ---------------

def create_section(symbol: str, name: str, x0: float, coin_qty: float,
                   request_id: str | None = None) -> dict[str, Any] | None:
    try:
        init_schema()
        now = datetime.now(UTC7).replace(tzinfo=None)
        with _cursor() as cur:
            cur.execute(
                """INSERT INTO crypto_sections (name, symbol, x0, coin_qty, current_x, current_pct, seeded, request_id, created_at)
                   VALUES (%s, %s, %s, %s, %s, 0, TRUE, %s, %s) RETURNING id""",
                (name, symbol.strip().upper(), x0, coin_qty, x0, request_id, now),
            )
            row = cur.fetchone()
            return {
                "id": row[0], "name": name, "symbol": symbol.strip().upper(),
                "x0": x0, "coin_qty": _dec_str(coin_qty) or "0",
                "quote_balance": "0", "current_x": x0,
                "current_pct": 0.0, "seeded": True,
                "request_id": request_id,
            }
    except Exception as e:
        logger.warning("db create_section: %s", e)
        if request_id:
            return load_section_by_request_id(request_id)
        return None


def load_section_by_request_id(request_id: str) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT id, name, symbol, x0, coin_qty, quote_balance, current_x, current_pct, seeded "
                "FROM crypto_sections WHERE request_id = %s",
                (request_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {
                "id": row[0], "name": row[1], "symbol": row[2],
                "x0": float(row[3]), "coin_qty": _dec_str(row[4]) or "0",
                "quote_balance": _dec_str(row[5]) or "0",
                "current_x": float(row[6]), "current_pct": float(row[7]),
                "seeded": bool(row[8]),
            }
    except Exception as e:
        logger.warning("db load_section_by_request_id: %s", e)
        return None


def load_sections(symbol: str | None = None) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            if symbol:
                cur.execute(
                    "SELECT id, name, symbol, x0, coin_qty, quote_balance, current_x, current_pct, seeded FROM crypto_sections WHERE UPPER(symbol) = UPPER(%s) ORDER BY id",
                    (symbol.strip(),),
                )
            else:
                cur.execute("SELECT id, name, symbol, x0, coin_qty, quote_balance, current_x, current_pct, seeded FROM crypto_sections ORDER BY symbol, id")
            for row in cur.fetchall():
                out.append({
                    "id": row[0], "name": row[1], "symbol": row[2],
                    "x0": float(row[3]), "coin_qty": _dec_str(row[4]) or "0",
                    "quote_balance": _dec_str(row[5]) or "0",
                    "current_x": float(row[6]), "current_pct": float(row[7]),
                    "seeded": bool(row[8]),
                })
    except Exception as e:
        logger.warning("db load_sections: %s", e)
    return out


def load_section(section_id: int) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT id, name, symbol, x0, coin_qty, quote_balance, current_x, current_pct, seeded FROM crypto_sections WHERE id = %s",
                (section_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {
                "id": row[0], "name": row[1], "symbol": row[2],
                "x0": float(row[3]), "coin_qty": _dec_str(row[4]) or "0",
                "quote_balance": _dec_str(row[5]) or "0",
                "current_x": float(row[6]), "current_pct": float(row[7]),
                "seeded": bool(row[8]),
            }
    except Exception as e:
        logger.warning("db load_section: %s", e)
        return None


def save_section_state(section_id: int, current_x: float, current_pct: float) -> None:
    try:
        with _cursor() as cur:
            cur.execute(
                "UPDATE crypto_sections SET current_x = %s, current_pct = %s WHERE id = %s",
                (current_x, current_pct, section_id),
            )
    except Exception as e:
        logger.warning("db save_section_state: %s", e)


def delete_section_db(section_id: int) -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute("DELETE FROM crypto_task_queue WHERE section_id = %s", (section_id,))
            cur.execute("DELETE FROM crypto_task_passed WHERE section_id = %s", (section_id,))
            cur.execute("DELETE FROM crypto_task_closed WHERE section_id = %s", (section_id,))
            cur.execute("DELETE FROM crypto_sections WHERE id = %s", (section_id,))
    except Exception as e:
        logger.warning("db delete_section_db: %s", e)


def load_task_queue_by_section(section_id: int) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"{_TASK_SELECT} WHERE section_id = %s",
                (section_id,),
            )
            for row in cur.fetchall():
                out.append(_task_row_to_dict(row))
    except Exception as e:
        logger.warning("db load_task_queue_by_section: %s", e)
    return out


def _task_row_to_dict(row) -> dict[str, Any]:
    return {
        "id": row[0], "symbol": row[1], "direction": row[2],
        "target_pct": float(row[3]), "action": row[4],
        "note": row[5] or "", "sibling_id": row[6],
        "sell_origin": row[7] or "", "section_id": row[8],
        "quote_budget": _dec_str(row[9]), "spawn_key": row[10] or "",
    }


_TASK_SELECT = (
    "SELECT id, symbol, direction, target_pct, action, note, sibling_id, "
    "sell_origin, section_id, quote_budget, spawn_key FROM crypto_task_queue"
)


def add_task_to_queue_for_section(section_id: int, symbol: str, direction: str,
                                   target_pct: float, action: str, note: str,
                                   sibling_id: int | None = None,
                                   sell_origin: str = "",
                                   quote_budget: Any = None,
                                   spawn_key: str | None = None) -> dict[str, Any] | None:
    if target_pct < -98 or target_pct > 98:
        return None
    try:
        init_schema()
        with _cursor() as cur:
            if spawn_key:
                cur.execute(f"{_TASK_SELECT} WHERE spawn_key = %s", (spawn_key,))
                existing = cur.fetchone()
                if existing is not None:
                    return _task_row_to_dict(existing)
            cur.execute(
                """INSERT INTO crypto_task_queue
                   (symbol, direction, target_pct, action, note, sibling_id,
                    sell_origin, section_id, quote_budget, spawn_key)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING
                   RETURNING id, symbol, direction, target_pct, action, note,
                             sibling_id, sell_origin, section_id, quote_budget, spawn_key""",
                (symbol.strip().upper(), direction, target_pct, action, note,
                 sibling_id, sell_origin, section_id,
                 _dec_str(quote_budget) if quote_budget is not None else None,
                 spawn_key or None),
            )
            row = cur.fetchone()
            if row is None and spawn_key:
                cur.execute(f"{_TASK_SELECT} WHERE spawn_key = %s", (spawn_key,))
                row = cur.fetchone()
            if row is None:
                return None
            return _task_row_to_dict(row)
    except Exception as e:
        logger.warning("db add_task_to_queue_for_section: %s", e)
        return None


def add_passed_task_for_section(section_id: int, symbol: str, direction: str,
                                 action: str, target_pct: float, hit_pct: float,
                                 hit_price: float, note: str,
                                 task_id: int | None = None,
                                 sell_origin: str = "") -> None:
    try:
        init_schema()
        with _cursor() as cur:
            if task_id is not None:
                cur.execute(
                    "SELECT 1 FROM crypto_task_passed WHERE section_id = %s AND task_id = %s LIMIT 1",
                    (section_id, task_id),
                )
                if cur.fetchone() is not None:
                    return
            cur.execute(
                """INSERT INTO crypto_task_passed (symbol, task_id, direction, action, target_pct, hit_pct, hit_price, note, sell_origin, section_id, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (symbol.strip().upper(), task_id, direction, action, target_pct,
                 hit_pct, hit_price, note, sell_origin, section_id,
                 datetime.now(UTC7).replace(tzinfo=None)),
            )
    except Exception as e:
        logger.warning("db add_passed_task_for_section: %s", e)


def load_passed_tasks_by_section(section_id: int) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                """SELECT id, symbol, direction, action, target_pct, hit_pct, hit_price, note, created_at, task_id, sell_origin, section_id
                   FROM crypto_task_passed WHERE section_id = %s ORDER BY created_at DESC LIMIT 200""",
                (section_id,),
            )
            for row in cur.fetchall():
                out.append({
                    "id": row[0], "symbol": row[1], "direction": row[2],
                    "action": row[3], "target_pct": float(row[4]),
                    "hit_pct": float(row[5]), "hit_price": float(row[6]),
                    "note": row[7] or "",
                    "at": row[8].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[8], "strftime") else str(row[8]),
                    "task_id": row[9], "sell_origin": row[10] or "",
                    "section_id": row[11],
                })
    except Exception as e:
        logger.warning("db load_passed_tasks_by_section: %s", e)
    return out


def add_closed_task_for_section(section_id: int, symbol: str, closed_task_id: int,
                                 sibling_triggered_id: int | None, direction: str,
                                 action: str, target_pct: float, at_pct: float,
                                 at_price: float, reason: str, note: str) -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT 1 FROM crypto_task_closed WHERE section_id = %s AND closed_task_id = %s LIMIT 1",
                (section_id, closed_task_id),
            )
            if cur.fetchone() is not None:
                return
            cur.execute(
                """INSERT INTO crypto_task_closed
                   (symbol, closed_task_id, sibling_triggered_id, direction, action,
                    target_pct, at_pct, at_price, reason, note, section_id, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (symbol.strip().upper(), closed_task_id, sibling_triggered_id,
                 direction, action, target_pct, at_pct, at_price, reason, note,
                 section_id, datetime.now(UTC7).replace(tzinfo=None)),
            )
    except Exception as e:
        logger.warning("db add_closed_task_for_section: %s", e)


def load_closed_tasks_by_section(section_id: int) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                """SELECT id, symbol, closed_task_id, sibling_triggered_id,
                          direction, action, target_pct, at_pct, at_price,
                          reason, note, created_at, section_id
                   FROM crypto_task_closed WHERE section_id = %s
                   ORDER BY created_at DESC LIMIT 200""",
                (section_id,),
            )
            for row in cur.fetchall():
                out.append({
                    "id": row[0], "symbol": row[1],
                    "closed_task_id": row[2], "sibling_triggered_id": row[3],
                    "direction": row[4], "action": row[5],
                    "target_pct": float(row[6]), "at_pct": float(row[7]),
                    "at_price": float(row[8]),
                    "reason": row[9] or "", "note": row[10] or "",
                    "at": row[11].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[11], "strftime") else str(row[11]),
                    "section_id": row[12],
                })
    except Exception as e:
        logger.warning("db load_closed_tasks_by_section: %s", e)
    return out


def load_section_info_batched(section_id: int) -> dict[str, Any]:
    result: dict[str, Any] = {"section": None, "tasks": [], "passed": [], "closed": []}
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT id, name, symbol, x0, coin_qty, quote_balance, current_x, current_pct, seeded FROM crypto_sections WHERE id = %s",
                (section_id,),
            )
            row = cur.fetchone()
            if row:
                result["section"] = {
                    "id": row[0], "name": row[1], "symbol": row[2],
                    "x0": float(row[3]), "coin_qty": _dec_str(row[4]) or "0",
                    "quote_balance": _dec_str(row[5]) or "0",
                    "current_x": float(row[6]), "current_pct": float(row[7]),
                    "seeded": bool(row[8]),
                }

            cur.execute(
                "SELECT id, symbol, direction, target_pct, action, note, sibling_id, sell_origin, section_id, quote_budget, spawn_key FROM crypto_task_queue WHERE section_id = %s",
                (section_id,),
            )
            for row in cur.fetchall():
                result["tasks"].append(_task_row_to_dict(row))

            cur.execute(
                """SELECT id, symbol, direction, action, target_pct, hit_pct, hit_price, note, created_at, task_id, sell_origin, section_id
                   FROM crypto_task_passed WHERE section_id = %s ORDER BY created_at DESC LIMIT 200""",
                (section_id,),
            )
            for row in cur.fetchall():
                result["passed"].append({
                    "id": row[0], "symbol": row[1], "direction": row[2],
                    "action": row[3], "target_pct": float(row[4]),
                    "hit_pct": float(row[5]), "hit_price": float(row[6]),
                    "note": row[7] or "",
                    "at": row[8].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[8], "strftime") else str(row[8]),
                    "task_id": row[9], "sell_origin": row[10] or "",
                    "section_id": row[11],
                })

            cur.execute(
                """SELECT id, symbol, closed_task_id, sibling_triggered_id,
                          direction, action, target_pct, at_pct, at_price,
                          reason, note, created_at, section_id
                   FROM crypto_task_closed WHERE section_id = %s
                   ORDER BY created_at DESC LIMIT 200""",
                (section_id,),
            )
            for row in cur.fetchall():
                result["closed"].append({
                    "id": row[0], "symbol": row[1],
                    "closed_task_id": row[2], "sibling_triggered_id": row[3],
                    "direction": row[4], "action": row[5],
                    "target_pct": float(row[6]), "at_pct": float(row[7]),
                    "at_price": float(row[8]),
                    "reason": row[9] or "", "note": row[10] or "",
                    "at": row[11].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[11], "strftime") else str(row[11]),
                    "section_id": row[12],
                })
    except Exception as e:
        logger.warning("db load_section_info_batched: %s", e)
    return result


# --------------- Price History ---------------

def add_price_history(symbol: str, price: float) -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "INSERT INTO crypto_price_history (symbol, price, created_at) VALUES (%s, %s, %s)",
                (symbol.strip().upper(), price, datetime.now(UTC7).replace(tzinfo=None)),
            )
    except Exception as e:
        logger.warning("db add_price_history: %s", e)


def load_price_history(symbol: str, limit: int = 20) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT id, symbol, price, created_at FROM crypto_price_history WHERE UPPER(symbol) = UPPER(%s) ORDER BY created_at DESC LIMIT %s",
                (symbol.strip(), limit),
            )
            for row in cur.fetchall():
                out.append({
                    "id": row[0], "symbol": row[1], "price": float(row[2]),
                    "at": row[3].strftime("%Y-%m-%d %H:%M:%S") if hasattr(row[3], "strftime") else str(row[3]),
                })
    except Exception as e:
        logger.warning("db load_price_history: %s", e)
    return out


def delete_engine_db(symbol: str) -> None:
    """Delete all data for a symbol: sections, tasks, passed, closed, engine state, settings."""
    try:
        init_schema()
        sym = symbol.strip().upper()
        with _cursor() as cur:
            cur.execute("DELETE FROM crypto_task_queue WHERE UPPER(symbol) = UPPER(%s)", (sym,))
            cur.execute("DELETE FROM crypto_task_passed WHERE UPPER(symbol) = UPPER(%s)", (sym,))
            cur.execute("DELETE FROM crypto_task_closed WHERE UPPER(symbol) = UPPER(%s)", (sym,))
            cur.execute("DELETE FROM crypto_sections WHERE UPPER(symbol) = UPPER(%s)", (sym,))
            cur.execute("DELETE FROM crypto_task_engine_state WHERE UPPER(symbol) = UPPER(%s)", (sym,))
            cur.execute("DELETE FROM crypto_settings WHERE UPPER(symbol) = UPPER(%s)", (sym,))
            cur.execute("DELETE FROM crypto_price_history WHERE UPPER(symbol) = UPPER(%s)", (sym,))
    except Exception as e:
        logger.warning("db delete_engine_db: %s", e)


# --------------- Task Orders ---------------

ORDER_COLUMNS = (
    "id", "task_id", "section_id", "symbol", "exchange_symbol",
    "client_order_id", "exchange_order_id", "side", "direction", "order_type",
    "amount", "price", "stop_price", "target_pct", "target_price",
    "sell_origin", "status", "filled_amount", "avg_fill_price",
    "filled_base", "filled_quote", "fee_base", "fee_quote", "fee_other",
    "fee_other_currency", "quote_budget", "quote_spent", "quote_settled_at",
    "trades_synced_filled", "accounting_status", "accounting_applied_at",
    "attempts", "next_retry_at", "last_error", "last_synced_at",
    "completed_at", "filled_at", "created_at", "updated_at",
)

_MONEY_ORDER_FIELDS = (
    "filled_base", "filled_quote", "fee_base", "fee_quote", "fee_other",
    "quote_budget", "quote_spent", "trades_synced_filled",
)

_ORDER_TIMESTAMP_FIELDS = (
    "next_retry_at", "last_synced_at", "completed_at", "filled_at",
    "quote_settled_at", "accounting_applied_at", "created_at", "updated_at",
)

_NUMERIC_ORDER_FIELDS = (
    "amount", "price", "stop_price", "target_pct", "target_price",
    "filled_amount", "avg_fill_price",
)


def _fmt_ts(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _order_row(row) -> dict[str, Any]:
    out = dict(zip(ORDER_COLUMNS, row))
    for field in _NUMERIC_ORDER_FIELDS:
        out[field] = float(out[field]) if out.get(field) is not None else None
    for field in _MONEY_ORDER_FIELDS:
        out[field] = _dec_str(out.get(field))
    out["attempts"] = int(out.get("attempts") or 0)
    out["sell_origin"] = out.get("sell_origin") or ""
    out["last_error"] = out.get("last_error") or ""
    out["fee_other_currency"] = out.get("fee_other_currency") or ""
    out["accounting_status"] = out.get("accounting_status") or "PENDING"
    for field in _ORDER_TIMESTAMP_FIELDS:
        out[field] = _fmt_ts(out.get(field))
    return out


def create_order_record(data: dict[str, Any]) -> dict[str, Any]:
    columns = [c for c in ORDER_COLUMNS if c not in ("id", "created_at", "updated_at")]
    now = datetime.now(UTC7).replace(tzinfo=None)
    defaults = {
        "status": "PENDING", "filled_amount": 0, "attempts": 0,
        "sell_origin": "", "last_error": "", "fee_base": 0, "fee_quote": 0,
        "fee_other": 0, "fee_other_currency": "", "quote_spent": 0,
        "trades_synced_filled": 0, "accounting_status": "PENDING",
    }
    values = []
    for column in columns:
        value = data.get(column)
        if value is None and column in defaults:
            value = defaults[column]
        values.append(value)
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"""INSERT INTO crypto_task_orders ({', '.join(columns)}, created_at, updated_at)
                    VALUES ({', '.join(['%s'] * len(columns))}, %s, %s)
                    ON CONFLICT (task_id) DO NOTHING""",
                values + [now, now],
            )
    except Exception as e:
        logger.warning("db create_order_record: %s", e)
    return load_order_by_task(int(data["task_id"])) or {}


def load_order_by_task(task_id: int) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(ORDER_COLUMNS)} FROM crypto_task_orders WHERE task_id = %s",
                (task_id,),
            )
            row = cur.fetchone()
            return _order_row(row) if row else None
    except Exception as e:
        logger.warning("db load_order_by_task: %s", e)
        return None


def load_order_by_client_id(client_order_id: str) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(ORDER_COLUMNS)} FROM crypto_task_orders WHERE client_order_id = %s",
                (client_order_id,),
            )
            row = cur.fetchone()
            return _order_row(row) if row else None
    except Exception as e:
        logger.warning("db load_order_by_client_id: %s", e)
        return None


def list_orders(symbol: str | None = None, section_id: int | None = None,
                status: str | None = None) -> list[dict[str, Any]]:
    out = []
    clauses = []
    params: list[Any] = []
    if symbol:
        clauses.append("UPPER(symbol) = UPPER(%s)")
        params.append(symbol.strip())
    if section_id is not None:
        clauses.append("section_id = %s")
        params.append(section_id)
    if status:
        clauses.append("status = %s")
        params.append(status.strip().upper())
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(ORDER_COLUMNS)} FROM crypto_task_orders{where} ORDER BY id",
                params,
            )
            out = [_order_row(row) for row in cur.fetchall()]
    except Exception as e:
        logger.warning("db list_orders: %s", e)
    return out


def list_filled_orders_between(start: Any, end: Any) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"""SELECT {', '.join(ORDER_COLUMNS)} FROM crypto_task_orders
                    WHERE status = 'FILLED'
                      AND COALESCE(filled_at, last_synced_at, completed_at, created_at) >= %s
                      AND COALESCE(filled_at, last_synced_at, completed_at, created_at) < %s
                    ORDER BY COALESCE(filled_at, last_synced_at, completed_at, created_at), id""",
                (start, end),
            )
            out = [_order_row(row) for row in cur.fetchall()]
    except Exception as e:
        logger.warning("db list_filled_orders_between: %s", e)
    return out


def list_partial_orders_between(start: Any, end: Any) -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"""SELECT {', '.join(ORDER_COLUMNS)} FROM crypto_task_orders
                    WHERE status <> 'FILLED'
                      AND filled_amount > 0
                      AND COALESCE(filled_at, last_synced_at, updated_at, created_at) >= %s
                      AND COALESCE(filled_at, last_synced_at, updated_at, created_at) < %s
                    ORDER BY COALESCE(filled_at, last_synced_at, updated_at, created_at), id""",
                (start, end),
            )
            out = [_order_row(row) for row in cur.fetchall()]
    except Exception as e:
        logger.warning("db list_partial_orders_between: %s", e)
    return out


def load_daily_report(report_date: Any) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT report_date, status, sent_at, error FROM crypto_daily_reports WHERE report_date = %s",
                (str(report_date),),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {
                "report_date": str(row[0]),
                "status": row[1] or "",
                "sent_at": _fmt_ts(row[2]),
                "error": row[3] or "",
            }
    except Exception as e:
        logger.warning("db load_daily_report: %s", e)
        return None


def save_daily_report(report_date: Any, status: str = "SENT",
                      error: str = "") -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                """INSERT INTO crypto_daily_reports (report_date, status, sent_at, error)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (report_date) DO UPDATE SET
                     status = EXCLUDED.status,
                     sent_at = EXCLUDED.sent_at,
                     error = EXCLUDED.error""",
                (str(report_date), status, datetime.now(UTC7).replace(tzinfo=None),
                 error or ""),
            )
    except Exception as e:
        logger.warning("db save_daily_report: %s", e)


def load_active_orders() -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"""SELECT {', '.join(ORDER_COLUMNS)} FROM crypto_task_orders
                    WHERE status IN ('PENDING','OPEN','PARTIALLY_FILLED','UNKNOWN')
                    ORDER BY id"""
            )
            out = [_order_row(row) for row in cur.fetchall()]
    except Exception as e:
        logger.warning("db load_active_orders: %s", e)
    return out


def load_uncompleted_filled_orders() -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"""SELECT {', '.join(ORDER_COLUMNS)} FROM crypto_task_orders
                    WHERE status = 'FILLED' AND completed_at IS NULL
                    ORDER BY COALESCE(last_synced_at, updated_at), id"""
            )
            out = [_order_row(row) for row in cur.fetchall()]
    except Exception as e:
        logger.warning("db load_uncompleted_filled_orders: %s", e)
    return out


def update_order(task_id: int, **fields: Any) -> dict[str, Any] | None:
    allowed = set(ORDER_COLUMNS) - {"id", "task_id", "created_at", "updated_at"}
    updates = [(k, v) for k, v in fields.items() if k in allowed]
    if not updates:
        return load_order_by_task(task_id)
    assignments = ", ".join(f"{k} = %s" for k, _ in updates)
    params = [v for _, v in updates] + [datetime.now(UTC7).replace(tzinfo=None), task_id]
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"UPDATE crypto_task_orders SET {assignments}, updated_at = %s WHERE task_id = %s",
                params,
            )
    except Exception as e:
        logger.warning("db update_order: %s", e)
    return load_order_by_task(task_id)


def delete_order(task_id: int) -> None:
    try:
        with _cursor() as cur:
            cur.execute("DELETE FROM crypto_task_orders WHERE task_id = %s", (task_id,))
    except Exception as e:
        logger.warning("db delete_order: %s", e)


# --------------- Section ledger / accounting ---------------

def adjust_section_balances(section_id: int, base_delta: Any,
                            quote_delta: Any) -> dict[str, Any]:
    """Apply base/quote deltas to a section ledger. Clamps coin_qty at 0."""
    result = {"coin_qty": "0", "quote_balance": "0", "clamped": False}
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT coin_qty, quote_balance FROM crypto_sections WHERE id = %s FOR UPDATE",
                (section_id,),
            )
            row = cur.fetchone()
            if row is None:
                return result
            coin = Decimal(str(row[0] or 0)) + Decimal(str(base_delta or 0))
            quote = Decimal(str(row[1] or 0)) + Decimal(str(quote_delta or 0))
            clamped = coin < 0
            if clamped:
                logger.warning("section %s oversell clamped to 0", section_id)
                coin = Decimal(0)
            cur.execute(
                "UPDATE crypto_sections SET coin_qty = %s, quote_balance = %s WHERE id = %s",
                (_dec_str(coin), _dec_str(quote), section_id),
            )
            result = {
                "coin_qty": _dec_str(coin),
                "quote_balance": _dec_str(quote),
                "clamped": clamped,
            }
    except Exception as e:
        logger.warning("db adjust_section_balances: %s", e)
    return result


def reserve_section_quote(section_id: int, amount: Any) -> bool:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "UPDATE crypto_sections SET quote_balance = quote_balance - %s "
                "WHERE id = %s AND quote_balance >= %s RETURNING id",
                (_dec_str(amount), section_id, _dec_str(amount)),
            )
            return cur.fetchone() is not None
    except Exception as e:
        logger.warning("db reserve_section_quote: %s", e)
        return False


def add_section_quote(section_id: int, amount: Any) -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "UPDATE crypto_sections SET quote_balance = quote_balance + %s WHERE id = %s",
                (_dec_str(amount), section_id),
            )
    except Exception as e:
        logger.warning("db add_section_quote: %s", e)


def create_buy_task_reserving_quote(section_id: int, symbol: str, direction: str,
                                    target_pct: float, note: str,
                                    sell_origin: str = "",
                                    spawn_key: str | None = None,
                                    sibling_id: int | None = None
                                    ) -> dict[str, Any] | None:
    """Reserve the section's full free quote balance into a BUY task."""
    if target_pct < -98 or target_pct > 98:
        return None
    try:
        init_schema()
        with _cursor() as cur:
            if spawn_key:
                cur.execute(f"{_TASK_SELECT} WHERE spawn_key = %s", (spawn_key,))
                existing = cur.fetchone()
                if existing is not None:
                    return _task_row_to_dict(existing)
            cur.execute(
                "SELECT quote_balance FROM crypto_sections WHERE id = %s FOR UPDATE",
                (section_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            budget = Decimal(str(row[0] or 0))
            if budget <= 0:
                return None
            cur.execute(
                "UPDATE crypto_sections SET quote_balance = 0 WHERE id = %s",
                (section_id,),
            )
            cur.execute(
                """INSERT INTO crypto_task_queue
                   (symbol, direction, target_pct, action, note, sibling_id,
                    sell_origin, section_id, quote_budget, spawn_key)
                   VALUES (%s, %s, %s, 'BUY', %s, %s, %s, %s, %s, %s)
                   RETURNING id, symbol, direction, target_pct, action, note,
                             sibling_id, sell_origin, section_id, quote_budget, spawn_key""",
                (symbol.strip().upper(), direction, target_pct, note, sibling_id,
                 sell_origin, section_id, _dec_str(budget), spawn_key or None),
            )
            inserted = cur.fetchone()
            if inserted is None and spawn_key:
                cur.execute(f"{_TASK_SELECT} WHERE spawn_key = %s", (spawn_key,))
                inserted = cur.fetchone()
            if inserted is None:
                return None
            return _task_row_to_dict(inserted)
    except Exception as e:
        logger.warning("db create_buy_task_reserving_quote: %s", e)
        return None


def apply_order_accounting(task_id: int, base_delta: Any, quote_delta: Any,
                           aggregates: dict[str, Any] | None = None) -> bool:
    """Apply an order's ledger deltas exactly once. Returns True if applied."""
    aggregates = aggregates or {}
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT section_id, accounting_applied_at FROM crypto_task_orders "
                "WHERE task_id = %s FOR UPDATE",
                (task_id,),
            )
            row = cur.fetchone()
            if row is None:
                return False
            if row[1] is not None:
                return False
            section_id = row[0]
            cur.execute(
                "SELECT coin_qty, quote_balance FROM crypto_sections WHERE id = %s FOR UPDATE",
                (section_id,),
            )
            sec = cur.fetchone()
            if sec is None:
                return False
            coin = Decimal(str(sec[0] or 0)) + Decimal(str(base_delta or 0))
            quote = Decimal(str(sec[1] or 0)) + Decimal(str(quote_delta or 0))
            clamped = coin < 0
            if clamped:
                logger.warning("section %s oversell clamped to 0", section_id)
                coin = Decimal(0)
            cur.execute(
                "UPDATE crypto_sections SET coin_qty = %s, quote_balance = %s WHERE id = %s",
                (_dec_str(coin), _dec_str(quote), section_id),
            )
            status = str(aggregates.get("accounting_status") or "OK")
            if clamped and status != "NEEDS_REVIEW":
                status = "NEEDS_REVIEW"
            cur.execute(
                """UPDATE crypto_task_orders SET
                     filled_base = COALESCE(%s, filled_base),
                     filled_quote = COALESCE(%s, filled_quote),
                     fee_base = %s,
                     fee_quote = %s,
                     fee_other = %s,
                     fee_other_currency = %s,
                     quote_spent = COALESCE(%s, quote_spent),
                     accounting_status = %s,
                     accounting_applied_at = %s,
                     updated_at = %s
                   WHERE task_id = %s""",
                (
                    _dec_str(aggregates.get("filled_base")),
                    _dec_str(aggregates.get("filled_quote")),
                    _dec_str(aggregates.get("fee_base") or 0),
                    _dec_str(aggregates.get("fee_quote") or 0),
                    _dec_str(aggregates.get("fee_other") or 0),
                    aggregates.get("fee_other_currency") or "",
                    _dec_str(aggregates.get("quote_spent")),
                    status,
                    datetime.now(UTC7).replace(tzinfo=None),
                    datetime.now(UTC7).replace(tzinfo=None),
                    task_id,
                ),
            )
            return True
    except Exception:
        logger.exception("db apply_order_accounting failed")
        raise


def settle_buy_order(task_id: int) -> dict[str, Any] | None:
    """Return leftover reserved quote to the section. Idempotent."""
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT section_id, quote_budget, quote_spent, quote_settled_at "
                "FROM crypto_task_orders WHERE task_id = %s FOR UPDATE",
                (task_id,),
            )
            row = cur.fetchone()
            if row is None or row[3] is not None:
                return None
            section_id = row[0]
            budget = Decimal(str(row[1] or 0))
            spent = Decimal(str(row[2] or 0))
            leftover = budget - spent
            if leftover < 0:
                leftover = Decimal(0)
            if leftover > 0:
                cur.execute(
                    "UPDATE crypto_sections SET quote_balance = quote_balance + %s WHERE id = %s",
                    (_dec_str(leftover), section_id),
                )
            now = datetime.now(UTC7).replace(tzinfo=None)
            if spent > budget:
                cur.execute(
                    "UPDATE crypto_task_orders SET quote_settled_at = %s, "
                    "accounting_status = 'NEEDS_REVIEW', updated_at = %s WHERE task_id = %s",
                    (now, now, task_id),
                )
            else:
                cur.execute(
                    "UPDATE crypto_task_orders SET quote_settled_at = %s, updated_at = %s "
                    "WHERE task_id = %s",
                    (now, now, task_id),
                )
            cur.execute(
                "UPDATE crypto_task_queue SET quote_budget = 0 WHERE id = %s",
                (task_id,),
            )
            return {"task_id": task_id, "released": _dec_str(leftover)}
    except Exception:
        logger.exception("db settle_buy_order failed")
        raise


def release_buy_task_budget(task_id: int) -> str | None:
    """Release a BUY task's reserved quote when no order row exists."""
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT 1 FROM crypto_task_orders WHERE task_id = %s LIMIT 1",
                (task_id,),
            )
            if cur.fetchone() is not None:
                return None
            cur.execute(
                "SELECT section_id, action, quote_budget FROM crypto_task_queue "
                "WHERE id = %s FOR UPDATE",
                (task_id,),
            )
            row = cur.fetchone()
            if row is None or str(row[1] or "").upper() != "BUY":
                return None
            budget = Decimal(str(row[2] or 0))
            if budget <= 0:
                return None
            cur.execute(
                "UPDATE crypto_sections SET quote_balance = quote_balance + %s WHERE id = %s",
                (_dec_str(budget), row[0]),
            )
            cur.execute(
                "UPDATE crypto_task_queue SET quote_budget = 0 WHERE id = %s",
                (task_id,),
            )
            return _dec_str(budget)
    except Exception as e:
        logger.warning("db release_buy_task_budget: %s", e)
        return None


# --------------- Command Queue ---------------

COMMAND_COLUMNS = (
    "id", "command_type", "payload", "status", "result", "error", "attempts",
    "locked_by", "locked_at", "created_at", "processed_at",
)


def _command_row(row) -> dict[str, Any]:
    out = dict(zip(COMMAND_COLUMNS, row))
    out["payload"] = out.get("payload") or {}
    out["result"] = out.get("result")
    out["error"] = out.get("error") or ""
    out["attempts"] = int(out.get("attempts") or 0)
    for field in ("locked_at", "created_at", "processed_at"):
        out[field] = _fmt_ts(out.get(field))
    return out


def enqueue_command(command_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    from psycopg2.extras import Json
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "INSERT INTO crypto_commands (command_type, payload) VALUES (%s, %s) RETURNING id, status",
                (command_type, Json(payload)),
            )
            row = cur.fetchone()
            return {"id": row[0], "command_type": command_type,
                    "payload": payload, "status": row[1]}
    except Exception as e:
        logger.warning("db enqueue_command: %s", e)
        raise


def get_command(command_id: int) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(COMMAND_COLUMNS)} FROM crypto_commands WHERE id = %s",
                (command_id,),
            )
            row = cur.fetchone()
            return _command_row(row) if row else None
    except Exception as e:
        logger.warning("db get_command: %s", e)
        return None


def claim_next_command(worker_id: str) -> dict[str, Any] | None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT id FROM crypto_commands WHERE status = 'PENDING' "
                "ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                return None
            command_id = row[0]
            now = datetime.now(UTC7).replace(tzinfo=None)
            cur.execute(
                "UPDATE crypto_commands SET status = 'PROCESSING', locked_by = %s, "
                "locked_at = %s, attempts = attempts + 1 WHERE id = %s",
                (worker_id, now, command_id),
            )
            cur.execute(
                f"SELECT {', '.join(COMMAND_COLUMNS)} FROM crypto_commands WHERE id = %s",
                (command_id,),
            )
            return _command_row(cur.fetchone())
    except Exception as e:
        logger.warning("db claim_next_command: %s", e)
        return None


def complete_command(command_id: int, status: str,
                     result: dict[str, Any] | None = None,
                     error: str = "") -> None:
    from psycopg2.extras import Json
    try:
        with _cursor() as cur:
            cur.execute(
                "UPDATE crypto_commands SET status = %s, result = %s, error = %s, "
                "processed_at = %s WHERE id = %s",
                (status, Json(result) if result is not None else None, error,
                 datetime.now(UTC7).replace(tzinfo=None), command_id),
            )
    except Exception as e:
        logger.warning("db complete_command: %s", e)


def recover_stale_commands(stale_sec: float) -> int:
    cutoff = datetime.now(UTC7).replace(tzinfo=None) - timedelta(seconds=stale_sec)
    try:
        with _cursor() as cur:
            cur.execute(
                "UPDATE crypto_commands SET status = 'NEEDS_REVIEW', "
                "error = 'processing timeout — worker restarted?', processed_at = %s "
                "WHERE status = 'PROCESSING' AND locked_at < %s",
                (datetime.now(UTC7).replace(tzinfo=None), cutoff),
            )
            return cur.rowcount or 0
    except Exception as e:
        logger.warning("db recover_stale_commands: %s", e)
        return 0


# --------------- Worker Status ---------------

def save_worker_status(worker_id: str, mode: str, version: str = "",
                       last_error: str = "") -> None:
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                """INSERT INTO crypto_worker_status (worker_id, last_seen, version, mode, last_error)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (worker_id) DO UPDATE SET
                     last_seen = EXCLUDED.last_seen,
                     version = EXCLUDED.version,
                     mode = EXCLUDED.mode,
                     last_error = EXCLUDED.last_error""",
                (worker_id, datetime.now(UTC7).replace(tzinfo=None), version, mode, last_error),
            )
    except Exception as e:
        logger.warning("db save_worker_status: %s", e)


def load_worker_statuses() -> list[dict[str, Any]]:
    out = []
    try:
        init_schema()
        with _cursor() as cur:
            cur.execute(
                "SELECT worker_id, last_seen, version, mode, last_error "
                "FROM crypto_worker_status ORDER BY worker_id"
            )
            for row in cur.fetchall():
                out.append({
                    "worker_id": row[0],
                    "last_seen": _fmt_ts(row[1]),
                    "version": row[2] or "",
                    "mode": row[3] or "",
                    "last_error": row[4] or "",
                })
    except Exception as e:
        logger.warning("db load_worker_statuses: %s", e)
    return out
