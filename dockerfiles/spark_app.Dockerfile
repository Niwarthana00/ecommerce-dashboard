# Dockerfile.spark_app
FROM apache/spark:3.5.1

USER root
WORKDIR /app

RUN curl -o /opt/spark/jars/postgresql-42.7.2.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

RUN pip install pyspark
RUN pip install psycopg2-binary

COPY scripts/spark_app.py .

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "spark_app.py"]