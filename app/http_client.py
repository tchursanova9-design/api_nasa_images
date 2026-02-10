# http_client.py
import time
import requests


class TransientHttpError(Exception):
    """Временная HTTP/сеть: можно ретраить."""
    pass


class PermanentHttpError(Exception):
    """Плохой URL/4xx: ретраить обычно бессмысленно."""
    pass


def download_image_bytes(url: str, timeout: int = 30, retries: int = 5) -> bytes:
    last = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)

            if 400 <= resp.status_code < 500:
                raise PermanentHttpError(f"HTTP {resp.status_code} for {url}")
            if resp.status_code >= 500:
                raise TransientHttpError(f"HTTP {resp.status_code} for {url}")

            resp.raise_for_status()
            return resp.content

        except PermanentHttpError:
            raise
        except (requests.Timeout, requests.ConnectionError, TransientHttpError) as e:
            last = e
            time.sleep(min(2 ** attempt, 15))

    raise TransientHttpError(f"Failed after retries: {last}")
