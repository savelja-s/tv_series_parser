from pathlib import Path

SITE = "https://uaserials.my/"
BASE_DIR = Path("serials")
PRIORITY_TRANSLATIONS = ["HDrezka Studio"]
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": SITE,
}
TIMEOUT = 20

# Паралельність
MAX_WORKERS = 6  # кількість потоків/процесів для паралельної підготовки/скачування
SEGMENT_DOWNLOAD_RETRIES = 3  # кількість повторів при неуспішному завантаженні сегмента
