"""Shared/API configuration for the thin API.

Worker-only settings (Binance private keys, Telegram, order execution,
daily-report scheduling and alert tuning) live in the local-only
``worker/config_worker.py``.
"""
import os
from datetime import timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DEFAULT_SYMBOLS = "BTCUSDT, ETHUSDT"
SYMBOLS_STR = os.getenv("CRYPTO_SYMBOLS", DEFAULT_SYMBOLS)
SYMBOLS = [s.strip().upper() for s in SYMBOLS_STR.split(",") if s.strip()]

BINANCE_BASE_URL = os.getenv(
    "BINANCE_BASE_URL",
    "https://api.binance.com",
).strip().rstrip("/") or "https://api.binance.com"

# Binance publishes additional Spot API domains (api1..api4) for performance.
# Note: HTTP 451 from Binance usually means geo/legal restriction on the server IP.
_BINANCE_DEFAULT_BASE_URLS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]

BINANCE_BASE_URLS_STR = os.getenv("BINANCE_BASE_URLS", "").strip()
if BINANCE_BASE_URLS_STR:
    BINANCE_BASE_URLS = [
        u.strip().rstrip("/")
        for u in BINANCE_BASE_URLS_STR.split(",")
        if u.strip()
    ]
else:
    others = [u for u in _BINANCE_DEFAULT_BASE_URLS if u.rstrip("/") != BINANCE_BASE_URL]
    BINANCE_BASE_URLS = [BINANCE_BASE_URL] + others

SAMPLE_PRICES = os.getenv("SAMPLE_PRICES", "").strip().lower() in ("1", "true", "yes")

FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0").strip() or "0.0.0.0"
FLASK_PORT = int(os.getenv("FLASK_PORT", "5004").strip() or "5004")

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "8").strip() or "8")

LOCAL_DATA_DIR_NAME = os.getenv("LOCAL_DATA_DIR", "local-data").strip() or "local-data"
_data_dir_path = Path(LOCAL_DATA_DIR_NAME)
DATA_DIR = (
    _data_dir_path
    if _data_dir_path.is_absolute()
    else (Path.cwd() / _data_dir_path).resolve()
)

UTC_OFFSET_HOURS = int(os.getenv("UTC_OFFSET_HOURS", "7").strip() or "7")
UTC7 = timezone(timedelta(hours=UTC_OFFSET_HOURS))

SAMPLE_BTC_MIN = int(os.getenv("SAMPLE_BTC_MIN", "95000").strip() or "95000")
SAMPLE_BTC_MAX = int(os.getenv("SAMPLE_BTC_MAX", "100000").strip() or "100000")
SAMPLE_ETH_MIN = int(os.getenv("SAMPLE_ETH_MIN", "1800").strip() or "1800")
SAMPLE_ETH_MAX = int(os.getenv("SAMPLE_ETH_MAX", "2000").strip() or "2000")

SAMPLE_PRICES_ROTATE_MINUTES = int(os.getenv("SAMPLE_PRICES_ROTATE_MINUTES", "1").strip() or "1")


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
WORKER_HEARTBEAT_SEC = float(os.getenv("WORKER_HEARTBEAT_SEC", "10").strip() or "10")
