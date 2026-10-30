FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY backend ./backend

EXPOSE 8000

CMD ["sh", "-c", "gunicorn --workers 1 --worker-class gthread --threads 4 --timeout 60 --access-logfile - --error-logfile - --bind 0.0.0.0:${PORT:-8000} backend.api_app:app"]
