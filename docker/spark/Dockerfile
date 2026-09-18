FROM python:3.12-slim-bookworm

ARG SPARK_VERSION=4.2.0
ARG MSSQL_JDBC_VERSION=12.8.1.jre11

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    SPARK_CONF_DIR=/opt/spark/conf

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip install pyspark==${SPARK_VERSION} numpy==2.5.3

ADD --chmod=644 https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}/mssql-jdbc-${MSSQL_JDBC_VERSION}.jar \
    ${SPARK_HOME}/jars/

RUN useradd --create-home --uid 1000 app \
    && mkdir -p /app/work /app/logs ${SPARK_CONF_DIR} \
    && chown -R app:app /app

COPY --chmod=755 entrypoint.sh /opt/entrypoint.sh

WORKDIR /app
USER app

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]
