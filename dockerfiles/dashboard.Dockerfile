FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    dash \
    dash-bootstrap-components \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    python-dotenv \
    plotly \
    gunicorn

EXPOSE 8050

CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "2", "--timeout", "120", "app:server"]