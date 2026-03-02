FROM python:3.9-slim

WORKDIR /app

RUN pip install pandas psycopg2-binary

COPY scripts/csv_to_db.py .

CMD ["python", "csv_to_db.py"]
