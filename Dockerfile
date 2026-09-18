FROM mlops/spark-py:4.2.0

COPY --chown=app:app src /app/src

WORKDIR /app/work

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh", "python", "/app/src/main.py"]
CMD ["train"]
