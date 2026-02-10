import time
import random
import sentry_sdk
import requests
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable, KafkaError

from observability import init_sentry
from config import QUERY, SEARCH_URL, INTERVAL_SECONDS
from nasa_api import NasaImagesClient
from kafka_producer import send_image_event

QUERIES = [
    "moon", "mars", "earth", "nebula", "galaxy", "saturn", "jupiter", "apollo",
]

def run_once(client: NasaImagesClient) -> None:
    query = QUERY or random.choice(QUERIES)
    nasa_id = client.fetch_random_nasa_id(query)
    image_url = client.fetch_image_url(nasa_id)

    print(f"[producer] query={query} nasa_id={nasa_id} image_url={image_url}")
    send_image_event(nasa_id=nasa_id, image_url=image_url)
    print(f"[producer] sent to kafka nasa_id={nasa_id}")

def main() -> None:
    init_sentry("producer")
    client = NasaImagesClient(search_url=SEARCH_URL)

    print(f"[producer] started; interval={INTERVAL_SECONDS}s")

    while True:
        try:
            run_once(client)

        except KeyboardInterrupt:
            print("[producer] stopped by user")
            break

        # ✅ 1) Интернет/DNS отвалился — ожидаемо: не спамим Sentry
        except (requests.ConnectionError, requests.Timeout) as e:
            print("[producer] network issue:", e)
            time.sleep(5)

        # ✅ 2) Kafka выключена/недоступна — ожидаемо: не спамим Sentry
        except (KafkaTimeoutError, NoBrokersAvailable, KafkaError) as e:
            print("[producer] Kafka unavailable:", e)
            time.sleep(5)  # можно заменить на backoff

        # ✅ 3) Всё остальное — это уже “настоящие” ошибки/баги → в Sentry
        except Exception as e:
            sentry_sdk.capture_exception(e)
            print("[producer] ERROR:", e)
            time.sleep(5)

        # Небольшой jitter, чтобы запросы не шли ровно по таймеру
        time.sleep(INTERVAL_SECONDS + random.uniform(0, 1.0))

if __name__ == "__main__":
    main()
