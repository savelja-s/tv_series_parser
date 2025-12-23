from pathlib import Path
import json

from parser import m3u8_parser
from utils import normalize_name
from config import PRIORITY_TRANSLATIONS, MAX_WORKERS
from fetcher import fetch, safe_run
from concurrent.futures import ThreadPoolExecutor, as_completed


# на рівні модуля
def fetch_and_parse_variant(m3u8_url: str, variants_path: Path, logger, m3u8_parser):
    if variants_path.exists():
        return json.loads(variants_path.read_text(encoding="utf-8"))
    logger.info(f"fetch m3u8: {m3u8_url}")
    playlist_text = fetch(m3u8_url)
    parsed = m3u8_parser.parse_master_m3u8(playlist_text, m3u8_url)
    variants_path.write_text(
        json.dumps(parsed, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return parsed


class ContentBuilder:
    def __init__(self, logger, m3u8_parser, max_workers=4):
        self.logger = logger
        self.m3u8_parser = m3u8_parser
        self.max_workers = max_workers

    @staticmethod
    def choose_translation(folders):
        for name in PRIORITY_TRANSLATIONS:
            for f in folders:
                if f.get("title") == name:
                    return f
        return None

    def build_content_tasks(self, player_data, serial_dir: Path):
        """Формує список задач для підготовки variants.json всіх серій"""
        tasks = []
        task_info = []  # щоб знати, яка серія до якого файлу
        seasons_dir = serial_dir / "seasons"
        seasons_dir.mkdir(exist_ok=True)

        for season in player_data:
            season_title = season.get("title", "season")
            season_path = seasons_dir / normalize_name(season_title)
            season_path.mkdir(parents=True, exist_ok=True)

            translation = self.choose_translation(season.get("folder", []))
            if not translation:
                self.logger.warning(f"no priority translation for {season_title}")
                continue

            tr_path = season_path / normalize_name(translation["title"])
            tr_path.mkdir(parents=True, exist_ok=True)

            for idx, episode in enumerate(translation.get("folder", []), start=1):
                if "file" not in episode:
                    continue

                m3u8_url = episode["file"]
                variants_path = tr_path / f"{idx:02d}_variants.json"

                tasks.append(fetch_and_parse_variant(m3u8_url, variants_path, self.logger, self.m3u8_parser))
                task_info.append({
                    "season": season_title,
                    "episode": episode.get("title"),
                    "translation": translation["title"],
                    "variants_file": variants_path,
                    "source_url": m3u8_url
                })
        return tasks, task_info

    def build(self, player_data, serial_dir: Path, from_season: int = 1):
        filtered_player_data = player_data[from_season - 1:]
        """Запускає підготовку variants.json паралельно і формує content.json"""
        tasks, task_info = self.build_content_tasks(filtered_player_data, serial_dir)
        manifest = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_and_parse_variant, ep["source_url"], Path(ep["variants_file"]), self.logger,
                                m3u8_parser): ep for ep in task_info}
            for f in as_completed(futures):
                ep_info = futures[f]
                try:
                    parsed = f.result()
                    if not parsed.get("selected"):
                        msg = f"SOUND({ep_info['translation']}) - {ep_info['episode']}({ep_info['season']})"
                        self.logger.warning(f"skip episode {msg} — no playable variant")
                        continue
                    # Конвертуємо шлях у рядок
                    ep_info["variants_file"] = str(ep_info["variants_file"])
                    ep_info["selected_resolution"] = parsed["selected"]["resolution"]
                    manifest.append(ep_info)
                except Exception as e:
                    self.logger.error(f"error processing {ep_info['episode']}: {e}")

        from collections import defaultdict

        seasons = defaultdict(list)
        for ep in manifest:
            seasons[ep["season"]].append(ep)

        (serial_dir / "content.json").write_text(
            json.dumps(seasons, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return manifest
