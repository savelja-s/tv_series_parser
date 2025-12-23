#!/usr/bin/env python3
import sys
from pathlib import Path

from logger import setup_logger
from parser.playerjs_parser import PlayerJSParser
from parser.m3u8_parser import M3U8Parser
from parser.content_builder import ContentBuilder
from fetcher import download_with_fallback
from utils import slug_from_url, normalize_name
from config import MAX_WORKERS, BASE_DIR


def main(serial_url: str):
    serial_slug = slug_from_url(serial_url)
    serial_dir = Path("serials") / serial_slug
    serial_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logger(serial_slug, BASE_DIR)
    logger.info("=== START ===")

    player_parser = PlayerJSParser(logger)
    player_data = player_parser.extract_player_data(
        serial_url,
        serial_dir,
    )

    m3u8_parser = M3U8Parser(logger)

    builder = ContentBuilder(
        logger,
        m3u8_parser,
        max_workers=MAX_WORKERS,
    )

    manifest = builder.build(player_data, serial_dir)

    logger.info(
        f"episodes to download: {len(manifest)}"
    )

    # ─── ПОСЛІДОВНО ПО СЕРІЯХ ─────────────────────────────
    for ep in manifest:
        ep["season_slug"] = normalize_name(ep["season"])
        ep["episode_slug"] = slug_from_url(ep["episode"])

        try:
            download_with_fallback(ep, serial_dir)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(
                f"[{ep['season']} | {ep['episode']}] fatal error → {e}"
            )

    logger.info("=== DONE ===")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <serial_url>")
        sys.exit(1)

    main(sys.argv[1])
