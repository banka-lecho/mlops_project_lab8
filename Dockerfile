# Сервис модели (версия лабораторной №7): PySpark KMeans, данные — только через витрину.
# Spark, Java и Python — из базового образа docker/spark, тех же версий, что у executor'ов в k8s.
FROM mlops/spark-py:4.2.0

COPY --chown=app:app src /app/src

# Относительные пути конфига (models/, data/) — внутри рабочего каталога (в k8s — общий том)
WORKDIR /app/work

# Команда модели передаётся аргументом: train | predict (как в docker compose run app train)
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh", "python", "/app/src/main.py"]
CMD ["train"]
