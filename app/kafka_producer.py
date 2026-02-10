# kafka_producer.py
import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError
from config import KAFKA_BOOTSTRAP

TOPIC = "images"
_producer: KafkaProducer | None = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is not None:
        return _producer

    attempts = 10
    for i in range(1, attempts + 1):
        try:
            print(f"[Kafka] connecting to {KAFKA_BOOTSTRAP} (attempt {i})")
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=10,
                linger_ms=20,
                request_timeout_ms=15000,
                max_block_ms=15000,
            )
            print("[Kafka] producer connected")
            return _producer
        except NoBrokersAvailable:
            time.sleep(2)

    raise RuntimeError("Kafka producer not available after retries")


def _reset_producer() -> None:
    global _producer
    try:
        if _producer is not None:
            _producer.close(timeout=5)
    except Exception:
        pass
    _producer = None


def send_image_event(nasa_id: str, image_url: str) -> None:
    """
    Гарантированная отправка: пока Kafka недоступна — ждём и ретраим тот же event.
    """
    event = {
        "event_type": "image_saved",
        "nasa_id": nasa_id,
        "image_url": image_url,
    }

    backoff = 0
    while True:
        try:
            producer = get_producer()
            future = producer.send(TOPIC, value=event)
            future.get(timeout=10)      # ждём ack
            producer.flush(timeout=10)  # добиваем буфер
            print("[Kafka] sent:", nasa_id)
            return

        except (KafkaTimeoutError, NoBrokersAvailable, KafkaError) as e:
            # Kafka down/подвисла — сбрасываем продюсер и ждём с backoff
            _reset_producer()
            backoff = min(backoff + 1, 8)           # ограничим рост
            sleep_s = min(2 ** backoff, 60) + random.uniform(0, 1.0)  # jitter
            print(f"[Kafka] unavailable, will retry same event in {sleep_s:.1f}s: {e}")
            time.sleep(sleep_s)
