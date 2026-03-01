# Dockerfile.producer
FROM python:3.9-slim

WORKDIR /app

RUN pip install pandas confluent-kafka

COPY scripts/producer.py .

CMD ["python", "producer.py"]