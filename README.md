# Lab 7: витрина данных на Scala между моделью и MS SQL Server

Кластеризация продуктов Open Food Facts по пищевой ценности (на 100г) с помощью Spark ML KMeans.
Модель не обращается к базе напрямую: между ними стоит витрина данных на Scala
([`datamart/`](datamart)). Витрина готовит данные (чтение сырого CSV, очистка, выборка),
хранит их в MS SQL Server и общается с моделью по HTTP в едином JSON-формате.

## Установка

Секреты для базы — в `.env` в корне проекта:

```bash
cp .env.example .env
```

Положите сырой датасет в `data/en.openfoodfacts.org.products.csv`

### Через Docker

```bash
docker compose up -d mssql mssql-init datamart
```

Витрина собирается из [`Dockerfile.datamart`](Dockerfile.datamart) (sbt внутри образа) и слушает порт 8090:

```bash
curl localhost:8090/health
```

`mssql-init` прогоняет [`docker/mssql/init/schema.sql`](docker/mssql/init/schema.sql). Данные базы лежат в volume `mssql-data` и переживают `docker compose down`.

### Локально

Нужна Java и поднятые контейнеры `mssql` и `datamart`:

```bash
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

set -a; source .env; set +a
```

## Конфигурация

Все настройки — в [`src/config.json`](src/config.json): пути к данным/артефактам (`data`), какие колонки брать (`features`),
параметры модели (`model`, включая диапазон `k`), адрес витрины (`datamart`).
Ресурсы машины (ядра, RAM) не задаются вручную — определяются в рантайме (`src/spark_session.py`), конфиг лишь ограничивает,
сколько от них брать. Адрес витрины переопределяется переменной `DATAMART_URL`.

Настройки витрины — в [`datamart/src/main/resources/config.json`](datamart/src/main/resources/config.json):
HTTP-сервер (`server`), Spark (`spark`), пути к данным (`data`), размер выборки (`sampling`), подключение к базе (`datasource`).
Переменные окружения важнее конфига: `MSSQL_HOST` / `MSSQL_PORT` переопределяют `datasource.host` / `datasource.port`,
`MSSQL_USER` / `MSSQL_PASSWORD` обязательны и берутся только из окружения. Память витрины задаётся через `JAVA_OPTS`
(по умолчанию `-Xmx2g`, для предобработки полного CSV: `DATAMART_JAVA_OPTS=-Xmx4g docker compose up -d datamart`).

## Запуск

```bash
# Предобработка на стороне витрины
curl -X POST localhost:8090/v1/preprocess -d '{}'

# Обучение: подбор k по silhouette, модель на диск, предсказания и метрики через витрину
docker compose run --rm app train

# Инференс последней успешной модели
docker compose run --rm app predict --output data/processed/predictions.parquet
```

Локально те же команды модели: `python src/main.py train|predict`.

## Протокол взаимодействия: модель — витрина — источник данных

**Кто участвует.** 
- Модель на PySpark
- Витрина данных на Scala
- Источник данных — MS SQL Server.

Модель всегда начинает первой и обращается только к витрине. В базу ходит исключительно витрина:
логин и пароль есть только у неё.

**Как общаются.** 
- HTTP на порт 8090, тело запроса и ответа — JSON.
- Клиент со стороны модели — [`src/datamart_client.py`](src/datamart_client.py),
- Сервер со стороны витрины — [`datamart/src/main/scala/datamart/DataMartApp.scala`](datamart/src/main/scala/datamart/DataMartApp.scala).

**Единый формат ответа.** Успех:

```json
{"status": "ok", "data": {}}
```

Ошибка (код 400, 404, 409 или 500):

```json
{"status": "error", "error": "Описание ошибки"}
```

Служебные поля называются в camelCase (`runId`, `rowCount`), поля строк данных — как колонки в базе
(`code`, `cluster_id`, `energy_kcal_100g`). Формат описан в
[`Protocol.scala`](datamart/src/main/scala/datamart/Protocol.scala), там же задан список признаков.

### Эндпоинты витрины

| Запрос | Тело | `data` в ответе |
|---|---|---|
| `POST /v1/preprocess` | `{}` или `{"rawPath": "...", "maxRows": 100000}` | отчёт об очистке и выборке |
| `POST /v1/features` | `{}` или `{"limit": 1000}` | `featureCols`, `rowCount`, `rows` |
| `POST /v1/runs/start` | `{"command": "train", "scalerPath": "...", "params": {}}` | `runId` |
| `POST /v1/runs/finish` | `{"runId": 1, "status": "SUCCESS", ...}` | `runId` |
| `POST /v1/runs/train` | `{}` или `{"runId": 1}` | `runId`, `modelPath`, `scalerPath` |
| `POST /v1/predictions` | `{"runId": 1, "rows": [{"code": "...", "cluster_id": 2}]}` | `runId`, `saved` |
| `POST /v1/predictions/get` | `{"runId": 1}` | `featureCols`, `rowCount`, `rows` |
| `GET /health` | — | `sparkVersion`, `database` |

### Предобработка (`/v1/preprocess`)

Выполняется целиком на стороне витрины.Статистика по шагам возвращается в ответе и пишется в `reports/preprocess_report.json`.

### Обучение (`train`)

1. **Регистрация.** Модель просит витрину открыть запуск. Витрина вставляет строку в `ml.model_runs` со статусом `RUNNING` и возвращает `run_id`.
2. **Данные.** Модель запрашивает признаки. Витрина читает `raw.processed_data` и отдаёт строки JSON вместе со списком колонок.
3. **Обучение.** Модель подбирает число кластеров и обучается. Модель и скейлер сохраняются на диск в `models/`.
4. **Результаты.** Модель отправляет витрине кластер каждого товара, витрина пишет их в `ml.predictions`.
5. **Завершение.** Модель просит закрыть запуск: статус `SUCCESS`, число кластеров и оценка качества.

Если на любом шаге произошла ошибка, модель закрывает запуск статусом `FAILED`, и текст ошибки сохраняется в базе.

### Предсказание (`predict`)

Порядок тот же, но вместо обучения:
- модель спрашивает у витрины, какое обучение было последним успешным и где лежит его модель;
- загружает модель с диска и применяет её к данным от витрины;
- после отправки результатов запрашивает их обратно и сохраняет в parquet-файл.


## Формат хранения данных

Все таблицы создаёт скрипт [`docker/mssql/init/schema.sql`](docker/mssql/init/schema.sql).
Таблицы разделены на две группы: `raw` — данные, которые модель получает, `ml` — то, что модель выдаёт.

### `raw.processed_data` — данные для модели

Одна строка — один товар.

| Колонка | Что хранит |
|---|---|
| `product_id` | порядковый номер строки, проставляет база |
| `code` | штрихкод товара |
| `energy_kcal_100g`, `fat_100g`, `saturated_fat_100g`, `carbohydrates_100g`, `sugars_100g`, `proteins_100g`, `salt_100g` | 7 признаков на 100 г: калории, жиры, насыщенные жиры, углеводы, сахар, белки, соль |

### `ml.model_runs` — журнал запусков

Одна строка — один запуск `train` или `predict`.

| Колонка | Что хранит |
|---|---|
| `run_id` | номер запуска, проставляет база |
| `command` | `train` или `predict` |
| `status` | `RUNNING`, `SUCCESS` или `FAILED` |
| `started_at`, `finished_at` | время начала и окончания |
| `rows_in` | сколько строк модель получила из базы |
| `best_k`, `best_silhouette` | число кластеров и оценка качества кластеризации |
| `params` | остальные параметры в формате JSON, например путь к модели |
| `scaler_path` | где на диске лежит скейлер |
| `error_message` | текст ошибки, если запуск упал |

### `ml.predictions` — результаты модели

Одна строка — один товар в одном запуске.

| Колонка | Что хранит |
|---|---|
| `run_id` | в каком запуске получен результат |
| `code` | штрихкод товара |
| `cluster_id` | номер кластера |

В одном запуске у товара ровно один кластер: пара `run_id` + `code` не может повторяться.
