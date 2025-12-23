import re
from urllib.parse import urlparse


def normalize_name(name: str) -> str:
    return (
        name.lower()
            .strip()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("__", "_")
    )


def slug_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/").split("/")[-1]
    return normalize_name(re.sub(r"\.html$", "", path))
