# Сервис модели версии лабораторной №6: данные и журнал запусков — в MS SQL.
# Spark, Java, Python и JDBC-драйвер MS SQL берутся из базового образа — те же, что у executor'ов.
FROM mlops/spark-py:4.2.0

# pymssql — короткие запросы драйвера к журналу запусков ml.model_runs (JDBC Spark не возвращает run_id)
RUN pip install --user pymssql==2.3.8

COPY --chown=app:app src /app/src

# Относительные пути конфига (data/, models/, reports/) — внутри общего тома
WORKDIR /app/work

CMD ["python", "/app/src/main.py", "train"]
