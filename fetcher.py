import json
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed

import aiohttp
import logging
from pathlib import Path
from typing import List, Callable

from config import (
    HEADERS,
    TIMEOUT,
    SEGMENT_DOWNLOAD_RETRIES,
)

logger = logging.getLogger("fetcher")

SEGMENT_WORKERS = 5
SEGMENT_DELAY = 0.15
RESOLUTIONS_FALLBACK = ["1080", "720", "480"]


def safe_run(tasks: List[Callable], workers: int):
    """
    Safe concurrent execution for IO-bound tasks.
    NO lambdas, NO process pool.
    """
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(task) for task in tasks]
        for f in as_completed(futures):
            f.result()


async def _fetch_async(url: str) -> str:
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    async with aiohttp.ClientSession(
            headers=HEADERS,
            timeout=timeout,
    ) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.text()


def fetch(url: str) -> str:
    """
    Backward-compatible fetch used by parsers.
    """
    return asyncio.run(_fetch_async(url))


def split_segments(segments: List[dict], workers: int):
    chunks = [[] for _ in range(workers)]
    for i, seg in enumerate(segments):
        chunks[i % workers].append(seg)
    return chunks


async def download_segment_batch(
        session: aiohttp.ClientSession,
        batch: List[dict],
        temp_dir: Path,
        progress,
        label: str,
):
    for seg in batch:
        path = temp_dir / f"{seg['index']:05d}.ts"

        for attempt in range(SEGMENT_DOWNLOAD_RETRIES):
            try:
                async with session.get(seg["uri"]) as resp:
                    resp.raise_for_status()
                    path.write_bytes(await resp.read())
                    progress.update(1)
                    break
            except Exception as e:
                if attempt == SEGMENT_DOWNLOAD_RETRIES - 1:
                    raise RuntimeError(
                        f"[{label}] segment {seg['index']} failed"
                    ) from e
                await asyncio.sleep(0.3)

        await asyncio.sleep(SEGMENT_DELAY)


async def _download_episode_async(
        segments: List[dict],
        temp_dir: Path,
        label: str,
):
    temp_dir.mkdir(parents=True, exist_ok=True)

    batches = split_segments(segments, SEGMENT_WORKERS)

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(
            headers=HEADERS,
            timeout=timeout,
    ) as session:
        from tqdm import tqdm

        with tqdm(
                total=len(segments),
                desc=label,
                unit="seg",
                leave=True,
        ) as progress:
            tasks = [
                download_segment_batch(
                    session,
                    batch,
                    temp_dir,
                    progress,
                    label,
                )
                for batch in batches
                if batch
            ]
            await asyncio.gather(*tasks)


def download_episode_segments(
        segments: List[dict],
        dest_file: Path,
        label: str,
):
    temp_dir = dest_file.parent / f"{dest_file.stem}_tmp"

    asyncio.run(
        _download_episode_async(
            segments,
            temp_dir,
            label,
        )
    )

    dest_file.parent.mkdir(parents=True, exist_ok=True)

    with dest_file.open("wb") as out:
        for f in sorted(temp_dir.iterdir(), key=lambda p: int(p.stem)):
            out.write(f.read_bytes())
            f.unlink()

    temp_dir.rmdir()
    logger.info(f"[{label}] saved → {dest_file.name}")


def download_with_fallback(ep: dict, serial_dir: Path):
    label = f"{ep['season']} | {ep['episode']}"

    variants_path = Path(ep["variants_file"])
    data = json.loads(variants_path.read_text(encoding="utf-8"))

    for res in RESOLUTIONS_FALLBACK:
        segments = [
            s for s in data["selected"]["segments"]
            if f"/{res}/" in s["uri"]
        ]

        if not segments:
            continue

        dest_file = (
                serial_dir
                / "seasons"
                / ep["season_slug"]
                / f"{ep['episode_slug']}.ts"
        )

        logger.info(
            f"[{label}] try {res}p ({len(segments)} segments)"
        )

        try:
            download_episode_segments(
                segments,
                dest_file,
                label,
            )
            return
        except Exception as e:
            logger.warning(
                f"[{label}] {res}p failed → {e}"
            )

    logger.error(
        f"[{label}] skipped — all resolutions failed"
    )
