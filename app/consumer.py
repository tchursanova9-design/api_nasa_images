# consumer.py
import time
import sentry_sdk
import psycopg2

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from config import KAFKA_BOOTSTRAP
from insert_bd import ensure_table, insert_image
from observability import init_sentry

from events import parse_image_event, PermanentError
from http_client import download_image_bytes, TransientHttpError, PermanentHttpError

TOPIC = "images"


def create_consumer() -> KafkaConsumer:
    attempts = 10
    for i in range(1, attempts + 1):
        try:
            print(f"[Kafka] consumer connecting to {KAFKA_BOOTSTRAP} (attempt {i})")

            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="images-writer",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                # ВАЖНО: bytes, чтобы не падать на не-JSON до try/except
                value_deserializer=lambda v: v,
            )

            print("[Kafka] consumer connected")
            return consumer

        except NoBrokersAvailable:
            time.sleep(2)

    raise RuntimeError("Kafka consumer not available after retries")


def main() -> None:
    init_sentry("consumer")
    ensure_table()

    consumer = create_consumer()
    print("[consumer] started. Waiting for messages...")

    for msg in consumer:
        raw_bytes: bytes = msg.value

        try:
            ev = parse_image_event(raw_bytes)
            print(f"[consumer] received nasa_id={ev.nasa_id}")

            image_bytes = download_image_bytes(ev.image_url)

            inserted = insert_image(
                nasa_id=ev.nasa_id,
                image_url=ev.image_url,
                image_bytes=image_bytes,
            )


            consumer.commit()

            if inserted:
                print(f"[consumer] saved to DB: {ev.nasa_id}")
            else:
                print(f"[consumer] duplicate skipped: {ev.nasa_id}")

        # --- Битые сообщения / плохие URL: пропускаем и НЕ падаем ---
        except (PermanentError, PermanentHttpError) as e:
            sentry_sdk.capture_exception(e)
            print("[consumer] PERMANENT error, skipping:", e)
            consumer.commit()  # важно: иначе вечный цикл

        # --- Интернет отвалился / 5xx: не коммитим, подождём и попробуем позже ---
        except TransientHttpError as e:
            sentry_sdk.capture_exception(e)
            print("[consumer] TRANSIENT network error, will retry later:", e)
            time.sleep(5)

        # --- Временная проблема с Postgres (сеть/перезапуск) ---
        except psycopg2.OperationalError as e:
            sentry_sdk.capture_exception(e)
            print("[consumer] TRANSIENT DB error, will retry later:", e)
            time.sleep(5)

        # --- Остальное: считаем временным, чтобы не терять данные ---
        except Exception as e:
            sentry_sdk.capture_exception(e)
            print("[consumer] UNKNOWN error, will retry later:", e)
            time.sleep(3)


if __name__ == "__main__":
    main()
