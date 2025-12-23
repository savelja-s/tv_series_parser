import json
import re
from bs4 import BeautifulSoup
from pathlib import Path
from fetcher import fetch


class PlayerJSParser:
    def __init__(self, logger):
        self.logger = logger

    def extract_player_data(self, serial_url: str, serial_dir: Path) -> list:
        player_json_path = serial_dir / "player.json"

        if player_json_path.exists():
            self.logger.info("player.json cache hit")
            return json.loads(player_json_path.read_text(encoding="utf-8"))

        self.logger.info("fetch serial page")
        page_html = fetch(serial_url)
        soup = BeautifulSoup(page_html, "html.parser")
        iframe = soup.select_one("iframe[data-src]")
        if not iframe:
            raise RuntimeError("iframe[data-src] не знайдено")

        embed_url = iframe["data-src"]
        self.logger.info(f"fetch embed: {embed_url}")

        embed_html = fetch(embed_url)
        player_data = self._extract_player_json(embed_html)

        player_json_path.write_text(
            json.dumps(player_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return player_data

    @staticmethod
    def _extract_player_json(html: str) -> list:
        match = re.search(r"new\s+Playerjs\s*\(\s*\{([\s\S]*?)\}\s*\)", html)
        if not match:
            raise RuntimeError("PlayerJS не знайдено")

        body = match.group(1)
        file_match = re.search(r"file\s*:\s*(['\"])([\s\S]*?)\1\s*,", body)
        if not file_match:
            raise RuntimeError("Поле file не знайдено")

        return json.loads(file_match.group(2))
