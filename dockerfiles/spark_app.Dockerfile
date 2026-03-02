FROM apache/spark:3.5.1

USER root
WORKDIR /app


RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.2.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9


COPY scripts/spark_app.py .

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD [ \
    "--master", "local[*]", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2", \
    "/app/spark_app.py" \
]
