"""
Fetch crypto prices from Binance public API.
Endpoint: GET /api/v3/ticker/price?symbol=BTCUSDT
Response: {"symbol":"BTCUSDT","price":"96816.99000000"}
"""
import logging
import random
import threading
import time
from typing import Optional

import requests

from .config import (
    BINANCE_BASE_URLS,
    REQUEST_TIMEOUT,
    SAMPLE_BTC_MAX,
    SAMPLE_BTC_MIN,
    SAMPLE_ETH_MAX,
    SAMPLE_ETH_MIN,
    SAMPLE_PRICES,
    SAMPLE_PRICES_ROTATE_MINUTES,
)

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CryptoBot/1.0)", "Accept": "application/json"}


def _binance_single_price(symbol: str) -> Optional[float]:
    last_err: Exception | None = None
    for base in BINANCE_BASE_URLS:
        url = f"{base}/api/v3/ticker/price"
        try:
            r = requests.get(url, params={"symbol": symbol}, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            # 451 usually means the server IP is geo/legally restricted.
            if r.status_code == 451:
                last_err = Exception(f"451 from {base}")
                continue
            r.raise_for_status()
            data = r.json()
            return float(data["price"])
        except Exception as e:
            last_err = e
            continue
    if last_err:
        logger.debug("Binance %s failed via all bases: %s", symbol, last_err)
    return None


def fetch_prices(symbols: list[str]) -> str:
    lines = []
    for sym in symbols:
        sym = sym.strip().upper()
        price = _binance_single_price(sym)
        if price is not None:
            lines.append(f"📈 {sym}: {price:,.2f}")
    if not lines:
        return "⚠️ Could not fetch prices. Check network and symbols (e.g. BTCUSDT, ETHUSDT)."
    return "\n".join(lines)


def parse_prices_text(text: str) -> dict[str, float]:
    result = {}
    for line in text.split("\n"):
        line = line.strip()
        if not line or "📈" not in line:
            continue
        rest = line.replace("📈", "").strip()
        if ":" not in rest:
            continue
        symbol, value = rest.split(":", 1)
        symbol = symbol.strip()
        value = value.strip().split("(")[0].strip().replace(",", "")
        try:
            result[symbol] = float(value)
        except ValueError:
            pass
    return result


_sample_prices: dict[str, float] = {}
_sample_lock = threading.Lock()
_sample_thread_started = False


def _sample_price_loop() -> None:
    while True:
        time.sleep(SAMPLE_PRICES_ROTATE_MINUTES * 60)
        with _sample_lock:
            _sample_prices["BTCUSDT"] = float(random.randint(SAMPLE_BTC_MIN, SAMPLE_BTC_MAX))
            _sample_prices["ETHUSDT"] = float(random.randint(SAMPLE_ETH_MIN, SAMPLE_ETH_MAX))
        logger.info("Sample prices: BTC=%s ETH=%s", _sample_prices.get("BTCUSDT"), _sample_prices.get("ETHUSDT"))


def fetch_prices_dict(symbols: list[str]) -> dict[str, float]:
    if SAMPLE_PRICES:
        global _sample_thread_started
        with _sample_lock:
            if not _sample_thread_started:
                _sample_thread_started = True
                _sample_prices["BTCUSDT"] = float(random.randint(SAMPLE_BTC_MIN, SAMPLE_BTC_MAX))
                _sample_prices["ETHUSDT"] = float(random.randint(SAMPLE_ETH_MIN, SAMPLE_ETH_MAX))
                t = threading.Thread(target=_sample_price_loop, daemon=True)
                t.start()
                logger.info("Sample price mode: BTC %s-%s, ETH %s-%s, rotate every %s min",
                            SAMPLE_BTC_MIN, SAMPLE_BTC_MAX, SAMPLE_ETH_MIN, SAMPLE_ETH_MAX,
                            SAMPLE_PRICES_ROTATE_MINUTES)
            result = {}
            for s in symbols:
                s = s.strip().upper()
                if s in _sample_prices:
                    result[s] = _sample_prices[s]
        return result

    text = fetch_prices(symbols)
    if not text or text.startswith("⚠️"):
        return {}
    return parse_prices_text(text)
