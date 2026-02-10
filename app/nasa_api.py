# nasa_api.py
import random
import requests


class NasaImagesClient:
    def __init__(self, search_url: str, timeout: int = 30):
        self.search_url = search_url
        self.timeout = timeout

    def fetch_first_nasa_id(self, query: str) -> str:
        params = {"q": query, "media_type": "image"}
        resp = requests.get(self.search_url, params=params, timeout=self.timeout)
        resp.raise_for_status()

        items = resp.json()["collection"]["items"]
        return items[0]["data"][0]["nasa_id"]

    def fetch_random_nasa_id(self, query: str) -> str:
        """
        Возвращает случайный nasa_id из выдачи по query.
        """
        params = {"q": query, "media_type": "image"}
        resp = requests.get(self.search_url, params=params, timeout=self.timeout)
        resp.raise_for_status()

        items = resp.json()["collection"]["items"]
        if not items:
            raise RuntimeError(f"No images found for query={query!r}")

        item = random.choice(items)
        return item["data"][0]["nasa_id"]

    def fetch_image_url(self, nasa_id: str) -> str:
        asset_url = f"https://images-api.nasa.gov/asset/{nasa_id}"
        resp = requests.get(asset_url, timeout=self.timeout)
        resp.raise_for_status()

        items = resp.json()["collection"]["items"]

        for it in items:
            href = it.get("href", "")
            if href.lower().endswith(".jpg"):
                return href

        return items[0]["href"]
