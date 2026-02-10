# events.py
import json
from dataclasses import dataclass
from typing import Any, Dict


class PermanentError(Exception):
    """Битые данные/схема: ретраи бессмысленны."""
    pass


@dataclass(frozen=True)
class ImageEvent:
    nasa_id: str
    image_url: str
    raw: Dict[str, Any]


def parse_image_event(raw_bytes: bytes) -> ImageEvent:
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
    except Exception as e:
        raise PermanentError(f"Bad JSON: {e}")

    if not isinstance(data, dict):
        raise PermanentError("Event must be a JSON object")

    nasa_id = data.get("nasa_id")
    image_url = data.get("image_url")

    if not isinstance(nasa_id, str) or not nasa_id.strip():
        raise PermanentError("Missing/invalid nasa_id")
    if not isinstance(image_url, str) or not image_url.startswith("http"):
        raise PermanentError("Missing/invalid image_url")

    return ImageEvent(nasa_id=nasa_id, image_url=image_url, raw=data)
