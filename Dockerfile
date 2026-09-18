FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/default-java

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home app

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN mkdir -p data models reports logs && chown -R app:app /app

USER app

COPY --chown=app:app src ./src

ENTRYPOINT ["python", "src/main.py"]
CMD ["train"]
