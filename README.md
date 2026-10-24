# ct-crc-api

Thin public API for the crypto-currency project. This repository intentionally
contains only the API/read-model/command-queue code. Trading logic and Binance
credentials live on the trusted worker (`worker_main.py` in the private/full
repository).

## Koyeb

- Builder: Dockerfile
- Port: `8000`
- Health check: `/health`
- Environment:
  - `DATABASE_URL`
  - `ADMIN_TOKEN`

Do not add `BINANCE_API_KEY`, `BINANCE_SECRET`, `TELEGRAM_*` or
`AUTO_TRADE_ENABLED` to this service.
