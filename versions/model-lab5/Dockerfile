FROM mlops/spark-py:4.2.0

COPY --chown=app:app src /app/src

WORKDIR /app/work

CMD ["python", "/app/src/main.py", "train"]
