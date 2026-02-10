# NASA Kafka Pet Project

Pet-проект на Python с Kafka, Postgres и observability стеком (Sentry + ELK).

## Что делает проект

- **Producer** (Python):
  - выбирает случайную тему (moon/mars/earth/…)
  - получает случайный `nasa_id` из NASA Images API
  - получает `image_url`
  - отправляет событие в Kafka topic `images`
  - при недоступности Kafka **не теряет событие**: ждёт и ретраит отправку (backoff)

- **Consumer** (Python):
  - читает события из Kafka topic `images`
  - скачивает картинку по `image_url`
  - сохраняет в Postgres (`images`), дедупликация по `nasa_id` (`UNIQUE` + `ON CONFLICT DO NOTHING`)
  - устойчив к сбоям (retry/backoff на сетевых и DB ошибках), manual commit offsets

- **Observability**:
  - ошибки приложения отправляются в **Sentry**
  - контейнерные логи собираются **Filebeat** → **Elasticsearch**
  - просмотр логов в **Kibana**
  - Kafka UI для просмотра топиков/сообщений

## Стек

- Python (kafka-python, requests, psycopg2)
- Kafka + Zookeeper
- Postgres
- Sentry
- Elasticsearch + Kibana + Filebeat
- Kafka UI
- Docker Compose

## Быстрый старт

1) Перейти в папку docker:
```bash
cd docker
