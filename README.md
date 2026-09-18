# Lab 8: модель, витрина и источник данных в Kubernetes

Кластеризация продуктов Open Food Facts по пищевой ценности (на 100г) с помощью Spark ML KMeans.
Модель не обращается к базе напрямую: между ними стоит витрина данных на Scala
([`datamart/`](datamart)). Витрина готовит данные (чтение сырого CSV, очистка, выборка),
хранит их в MS SQL Server и общается с моделью по HTTP в едином JSON-формате.

Все три сервиса перенесены в Kubernetes, вычисления Spark распределены: драйвер модели и драйвер витрины
сами просят у Kubernetes поды executor'ов (2 реплики), а после работы удаляют их.

## Как устроено в кластере

Все объекты лежат в namespace `mlops`.

| Сервис | Объекты | Манифест |
|---|---|---|
| Инфраструктура Spark | ServiceAccount `spark` с правами на поды, общий диск `mlops-work` (папка `storage/` проекта), ConfigMap `spark-defaults` — число и размер executor'ов | [`k8s/spark-rbac.yaml`](k8s/spark-rbac.yaml), [`k8s/storage.yaml`](k8s/storage.yaml), [`k8s/spark-defaults.yaml`](k8s/spark-defaults.yaml) |
| Источник данных MS SQL | StatefulSet `mssql` со своим диском, Service `mssql:1433`, Job `mssql-init` (таблицы), Secret `mssql-credentials` | [`k8s/mssql/`](k8s/mssql) |
| Витрина данных | Deployment `datamart` — постоянно работающий драйвер Spark с динамическим выделением executor'ов (0–2), Service `datamart:8090` | [`k8s/datamart/datamart.yaml`](k8s/datamart/datamart.yaml) |
| Модель | Job'ы `model-preprocess` (вызов витрины), `model-train`, `model-predict` | [`k8s/model/`](k8s/model) |
| Проверка инфраструктуры | Job `spark-smoke-test`: 2 executor'а, вычисление, запись на общий диск | [`k8s/spark-smoke-test.yaml`](k8s/spark-smoke-test.yaml) |

Образы:

| Образ | Из чего | Кто запускается |
|---|---|---|
| `mlops/spark-py:4.2.0` | [`docker/spark/Dockerfile`](docker/spark/Dockerfile) | executor'ы всех приложений; основа образа модели |
| `mlops/model:lab7` | [`Dockerfile`](Dockerfile) | драйвер модели |
| `mlops/datamart:lab7` | [`Dockerfile.datamart`](Dockerfile.datamart) | витрина |

Пароли базы есть только у самой базы, у `mssql-init` и у витрины: у Job'ов модели нет доступа к Secret.

## Подготовка

Секреты для базы — в `.env` в корне проекта:

```bash
cp .env.example .env
```

Положите сырой датасет в `data/en.openfoodfacts.org.products.csv`.

## Запуск в Kubernetes

Нужны Kubernetes (проверялось на Docker Desktop, способ развёртывания kubeadm, ~12 ГБ памяти) и `kubectl`.
В [`k8s/storage.yaml`](k8s/storage.yaml) указан абсолютный путь к папке `storage/` проекта — на другом компьютере его нужно поменять.

### 0. Чистый лист (если что-то уже развёрнуто)

```bash
kubectl delete namespace mlops
kubectl delete pv mlops-work
docker compose down -v
rm -rf storage
```

Не удаляйте контейнеры командой `docker rm -f $(docker ps -aq)`: в Docker Desktop в том же Docker работают
системные контейнеры самого Kubernetes (`k8s_…`). Полный сброс кластера — Docker Desktop → Settings → Kubernetes → Reset Kubernetes Cluster.

### 1. Инфраструктура Spark

```bash
docker build -t mlops/spark-py:4.2.0 docker/spark
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/spark-rbac.yaml -f k8s/storage.yaml -f k8s/spark-defaults.yaml
kubectl -n mlops get pvc
```

Проверка: драйвер поднимает 2 executor'а, считает сумму 0..10M и пишет/читает parquet на общем диске.
Во втором терминале удобно смотреть, как появляются поды: `kubectl -n mlops get pods -w`.

```bash
kubectl apply -f k8s/spark-smoke-test.yaml && kubectl -n mlops wait --for=condition=complete job/spark-smoke-test --timeout=300s
kubectl -n mlops logs job/spark-smoke-test
```

Ожидается: `Задачи выполнили 2 executor'а`, `sum(0..10M) = 49999995000000`, `100000 строк`.

### 2. Данные для кластера

Для проверки в кластере берутся первые 100 тыс. строк CSV, они кладутся на общий диск:

```bash
mkdir -p storage/data/raw
head -n 100001 data/en.openfoodfacts.org.products.csv > storage/data/raw/products_100k.csv
```

### 3. Источник данных MS SQL

Пароли из `.env` попадают в k8s Secret, схема — в ConfigMap из того же `schema.sql`, что использует docker compose:

```bash
kubectl -n mlops create secret generic mssql-credentials --from-env-file=.env
kubectl -n mlops create configmap mssql-schema --from-file=docker/mssql/init/schema.sql
kubectl apply -f k8s/mssql/mssql.yaml && kubectl -n mlops rollout status statefulset/mssql --timeout=400s
kubectl apply -f k8s/mssql/job-init.yaml && kubectl -n mlops wait --for=condition=complete job/mssql-init --timeout=300s
kubectl -n mlops logs job/mssql-init
```

Ожидается `Схема применена`.

### 4. Витрина и модель

```bash
docker build -t mlops/datamart:lab7 -f Dockerfile.datamart .
docker build -t mlops/model:lab7 .
kubectl apply -f k8s/datamart/datamart.yaml && kubectl -n mlops rollout status deployment/datamart --timeout=300s
```

Модель запускается строго по очереди — каждый шаг берёт результат предыдущего:

```bash
kubectl apply -f k8s/model/job-preprocess.yaml && kubectl -n mlops wait --for=condition=complete job/model-preprocess --timeout=600s
kubectl apply -f k8s/model/job-train.yaml && kubectl -n mlops wait --for=condition=complete job/model-train --timeout=900s
kubectl apply -f k8s/model/job-predict.yaml && kubectl -n mlops wait --for=condition=complete job/model-predict --timeout=600s
```

Если Job упал, `kubectl wait` этого не заметит и будет ждать до таймаута — проверяйте `kubectl -n mlops get jobs`.
Job нельзя перезапустить с тем же именем: перед повтором `kubectl -n mlops delete job <имя>`.

Результаты — в логах и в базе, модели и предсказания — в `storage/models` и `storage/data/processed`:

```bash
kubectl -n mlops logs job/model-train | grep -E "INFO|Silhouette"
kubectl -n mlops exec mssql-0 -- bash -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -d OpenFoodDB -W -Q "SELECT run_id, command, status, rows_in, best_k, best_silhouette FROM ml.model_runs"'
```

### 5. Утилизация ресурсов

Ресурсы подобраны по замерам `kubectl top` (нужен metrics-server, см. [инструменты](#инструменты-для-демонстрации-в-kubernetes)):

| Под | Резерв (requests) | Лимит | Пик при работе |
|---|---|---|---|
| `datamart` | 250m CPU, 1 ГБ | 1,5 ГБ | 476m, 760 МБ |
| драйвер модели | 500m CPU, 1 ГБ | 1,5 ГБ | 880m, 800 МБ |
| executor | 500m CPU, 896 МБ | 1 CPU, 896 МБ | 590m, 550–680 МБ |
| `mssql-0` | 250m CPU, 1,5 ГБ | 2 ГБ (минимум для MS SQL) | 60m, ~1,1–1,4 ГБ |

- Витрина держит executor'ов только пока считает: после 60 с простоя они удаляются (динамическое выделение).
  В простое в `mlops` работают только `datamart` и `mssql-0`: резерв 500m CPU и 2,5 ГБ вместо 2000m и 6,4 ГБ.
- Модель сама определяет память драйвера по лимиту пода (`/sys/fs/cgroup/memory.max`), а не по памяти всей ноды.
- `maxExecutors` витрины в [`datamart.yaml`](k8s/datamart/datamart.yaml) должен быть не меньше `spark.executor.instances`
  в [`spark-defaults.yaml`](k8s/spark-defaults.yaml): Spark стартует с `instances` executor'ов, иначе витрина не запустится.

```bash
kubectl top pods -n mlops
kubectl describe node docker-desktop | grep -A 8 "Allocated resources"
```

### Демонстрации

Обновление витрины без остановки: новый под поднимается и становится готовым раньше, чем гасится старый
(во втором терминале `kubectl -n mlops get pods -w`):

```bash
kubectl -n mlops rollout restart deployment/datamart
```

Самовосстановление: Kubernetes сам пересоздаёт под базы, данные остаются на её диске:

```bash
kubectl -n mlops delete pod mssql-0
```

Spark UI обучения: `SPARK_UI_HOLD_SEC` держит драйвер открытым после обучения. Порт пробрасывается, когда Spark стартовал:

```bash
kubectl -n mlops delete job model-train --ignore-not-found
sed 's/value: "0"/value: "300"/' k8s/model/job-train.yaml | kubectl apply -f -
until kubectl -n mlops logs job/model-train 2>/dev/null | grep -q "Spark 4.2.0"; do sleep 1; done
kubectl -n mlops port-forward $(kubectl -n mlops get pod -l job-name=model-train -o name) 4040:4040
```

Затем http://localhost:4040/executors/. Spark UI витрины доступен всё время:
`kubectl -n mlops port-forward deploy/datamart 4041:4040` → http://localhost:4041.

Общий обзор развёрнутого:

```bash
kubectl -n mlops get all,pvc,configmap,secret,sa
```

## Запуск без Kubernetes (docker compose, как в лабораторной №7)

Образ модели собирается из базового образа Spark, его нужно собрать заранее:

```bash
docker build -t mlops/spark-py:4.2.0 docker/spark
docker compose up -d mssql mssql-init datamart
```

Витрина собирается из [`Dockerfile.datamart`](Dockerfile.datamart) (sbt внутри образа) и слушает порт 8090:

```bash
curl localhost:8090/health
```

`mssql-init` прогоняет [`docker/mssql/init/schema.sql`](docker/mssql/init/schema.sql). Данные базы лежат в volume `mssql-data` и переживают `docker compose down`.

```bash
# Предобработка на стороне витрины
curl -X POST localhost:8090/v1/preprocess -d '{}'

# Обучение: подбор k по silhouette, модель на диск, предсказания и метрики через витрину
docker compose run --rm app train

# Инференс последней успешной модели
docker compose run --rm app predict --output data/processed/predictions.parquet
```

### Локально

Нужна Java и поднятые контейнеры `mssql` и `datamart`:

```bash
brew install openjdk@17
export JAVA_HOME=/opt/homebrew/opt/openjdk@17

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

set -a; source .env; set +a
python src/main.py train
```

## Конфигурация

Настройки модели — в [`src/config.json`](src/config.json): Spark (`spark`), параметры модели (`model`, включая диапазон `k`),
адрес витрины (`datamart`). Ресурсы (ядра, RAM) не задаются вручную — определяются в рантайме (`src/spark_session.py`),
в контейнере с лимитом памяти — по этому лимиту; конфиг лишь ограничивает, сколько от них брать.

Настройки витрины — в [`datamart/src/main/resources/config.json`](datamart/src/main/resources/config.json):
HTTP-сервер (`server`), Spark (`spark`), пути к данным (`data`), размер выборки (`sampling`), подключение к базе (`datasource`).
Память витрины задаётся через `JAVA_OPTS` (в compose по умолчанию `-Xmx2g`, для предобработки полного CSV:
`DATAMART_JAVA_OPTS=-Xmx4g docker compose up -d datamart`).

Переменные окружения важнее конфигов — через них кластер меняет только то, что отличается от локального запуска:

| Переменная | Кому | Что задаёт |
|---|---|---|
| `SPARK_MASTER` | модель, витрина | `k8s://…` — executor'ы в кластере; без неё Spark работает локально (`local[...]`) |
| `DATAMART_URL` | модель | адрес витрины (`http://datamart:8090` в кластере) |
| `MSSQL_HOST`, `MSSQL_PORT` | витрина | адрес базы |
| `MSSQL_USER`, `MSSQL_PASSWORD` | витрина | логин и пароль, только из окружения (в кластере — из Secret) |
| `SPARK_CONF_DIR` | витрина | папка с `spark-defaults.conf`: витрина запускается без `spark-submit` и читает его сама |
| `SPARK_UI_HOLD_SEC` | модель | сколько секунд держать Spark UI после обучения (для показа) |

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

Выполняется целиком на стороне витрины. Статистика по шагам возвращается в ответе и пишется в `reports/preprocess_report.json`.

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

## Инструменты для демонстрации в Kubernetes

**metrics-server** — источник цифр для `kubectl top` и графиков нагрузки. В Docker Desktop ему нужен флаг `--kubelet-insecure-tls`:

```bash
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm upgrade --install metrics-server metrics-server/metrics-server -n kube-system --set 'args={--kubelet-insecure-tls}'
kubectl top pods -n mlops
```

**Kubernetes Dashboard** — веб-интерфейс кластера. Для входа создаётся учётная запись только для чтения
(встроенная роль `view`: поды, Job'ы, логи — без права что-либо менять и без доступа к Secret'ам):

```bash
helm repo add kubernetes-dashboard https://kubernetes-retired.github.io/dashboard/
helm upgrade --install kubernetes-dashboard kubernetes-dashboard/kubernetes-dashboard -n kubernetes-dashboard --create-namespace
kubectl -n kubernetes-dashboard create serviceaccount dashboard-viewer
kubectl create clusterrolebinding dashboard-viewer --clusterrole=view --serviceaccount=kubernetes-dashboard:dashboard-viewer
kubectl -n kubernetes-dashboard port-forward svc/kubernetes-dashboard-kong-proxy 8443:443
```

Токен для входа на https://localhost:8443:

```bash
kubectl -n kubernetes-dashboard create token dashboard-viewer --duration=24h
```
