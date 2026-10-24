import os
from datetime import timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

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
    # Prefer configured BINANCE_BASE_URL first (backwards compatible)
    others = [u for u in _BINANCE_DEFAULT_BASE_URLS if u.rstrip("/") != BINANCE_BASE_URL]
    BINANCE_BASE_URLS = [BINANCE_BASE_URL] + others

BINANCE_WS_URL = os.getenv(
    "BINANCE_WS_URL",
    "wss://stream.binance.com:9443",
).strip().rstrip("/") or "wss://stream.binance.com:9443"

SAMPLE_PRICES = os.getenv("SAMPLE_PRICES", "").strip().lower() in ("1", "true", "yes")

FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0").strip() or "0.0.0.0"
FLASK_PORT = int(os.getenv("FLASK_PORT", "5004").strip() or "5004")

TELEGRAM_API_BASE = os.getenv("TELEGRAM_API_BASE", "https://api.telegram.org").strip().rstrip("/") or "https://api.telegram.org"

CHECK_INTERVAL_SEC = int(os.getenv("CHECK_INTERVAL_SEC", "30").strip() or "30")
PRICE_BAND_PCT = float(os.getenv("PRICE_BAND_PCT", "0.001").strip() or "0.001")
ALERT_DEBOUNCE_SEC = float(os.getenv("ALERT_DEBOUNCE_SEC", "1").strip() or "1")
ALERT_CACHE_TTL_SEC = float(os.getenv("ALERT_CACHE_TTL_SEC", "5").strip() or "5")
EQUAL_TOLERANCE_PCT = float(os.getenv("EQUAL_TOLERANCE_PCT", "0.0001").strip() or "0.0001")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "8").strip() or "8")
MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", "4096").strip() or "4096")

LOCAL_DATA_DIR_NAME = os.getenv("LOCAL_DATA_DIR", "local-data").strip() or "local-data"


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


DATA_DIR = _project_root() / LOCAL_DATA_DIR_NAME

UTC_OFFSET_HOURS = int(os.getenv("UTC_OFFSET_HOURS", "7").strip() or "7")
UTC7 = timezone(timedelta(hours=UTC_OFFSET_HOURS))

SAMPLE_BTC_MIN = int(os.getenv("SAMPLE_BTC_MIN", "95000").strip() or "95000")
SAMPLE_BTC_MAX = int(os.getenv("SAMPLE_BTC_MAX", "100000").strip() or "100000")
SAMPLE_ETH_MIN = int(os.getenv("SAMPLE_ETH_MIN", "1800").strip() or "1800")
SAMPLE_ETH_MAX = int(os.getenv("SAMPLE_ETH_MAX", "2000").strip() or "2000")

SAMPLE_PRICES_ROTATE_MINUTES = int(os.getenv("SAMPLE_PRICES_ROTATE_MINUTES", "1").strip() or "1")


# --------------- Auto-trade / worker split ---------------

def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


AUTO_TRADE_ENABLED = _env_bool("AUTO_TRADE_ENABLED", "0")
BINANCE_SANDBOX = _env_bool("BINANCE_SANDBOX", "1")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_SECRET = os.getenv("BINANCE_SECRET", "").strip()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()

ORDER_SYNC_INTERVAL_SEC = float(os.getenv("ORDER_SYNC_INTERVAL_SEC", "10").strip() or "10")
ORDER_MAX_OPEN = int(os.getenv("ORDER_MAX_OPEN", "20").strip() or "20")
AUTO_TRADE_SYMBOLS_STR = os.getenv("AUTO_TRADE_SYMBOLS", "").strip()
AUTO_TRADE_SYMBOLS = [
    s.strip().upper() for s in AUTO_TRADE_SYMBOLS_STR.split(",") if s.strip()
]
ORDER_RETRY_MAX = int(os.getenv("ORDER_RETRY_MAX", "3").strip() or "3")
ORDER_RETRY_BACKOFF_SEC = float(os.getenv("ORDER_RETRY_BACKOFF_SEC", "15").strip() or "15")

WORKER_ID = os.getenv("WORKER_ID", "worker-1").strip() or "worker-1"
COMMAND_POLL_INTERVAL_SEC = float(os.getenv("COMMAND_POLL_INTERVAL_SEC", "2").strip() or "2")
WORKER_HEARTBEAT_SEC = float(os.getenv("WORKER_HEARTBEAT_SEC", "10").strip() or "10")
COMMAND_STALE_SEC = float(os.getenv("COMMAND_STALE_SEC", "120").strip() or "120")
WORKER_VERSION = os.getenv("WORKER_VERSION", "1.0").strip() or "1.0"
