FROM apache/spark:3.5.1

USER root
WORKDIR /app

# ── System dependencies ──
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ── PostgreSQL JDBC Driver ──
# spark-submit --packages වලින් download කරනවා නිසා මෙතනත් දාන්න ඕනෑ නෑ
# හැබැයි pre-download කරලා දැම්මොත් startup faster
RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.2.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.2.jar

# ── Python dependencies ──
# pyspark දාන්න එපා — spark image එකේ දැනටමත් තියෙනවා
# දැම්මොත් version conflict එකක් එනවා
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9

# ── App copy ──
COPY scripts/spark_app.py .

# ── Entrypoint ──
# CMD එක docker-compose.yml එකෙන් override කරනවා නිසා
# මෙතන දාන්න ඕනෑ නෑ — හැබැයි fallback එකක් විදිහට තියමු
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD [ \
    "--master", "local[*]", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2", \
    "/app/spark_app.py" \
]
