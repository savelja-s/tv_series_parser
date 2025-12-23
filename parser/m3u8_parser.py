from urllib.parse import urljoin
import requests
import re


class M3U8Parser:
    def __init__(self, logger, timeout=20):
        self.logger = logger
        self.timeout = timeout

    def parse_media_m3u8(self, media_text: str, base_url: str) -> list:
        segments = []
        lines = [l.strip() for l in media_text.splitlines() if l.strip()]
        index = 1
        i = 0
        while i < len(lines):
            if lines[i].startswith("#EXTINF"):
                duration = float(lines[i].split(":", 1)[1].rstrip(","))
                uri = lines[i + 1]
                segments.append({
                    "index": index,
                    "duration": duration,
                    "uri": urljoin(base_url, uri)
                })
                index += 1
                i += 2
            else:
                i += 1
        return segments

    def parse_master_m3u8(self, master_text: str, master_url: str) -> dict:
        variants = []
        lines = [l.strip() for l in master_text.splitlines() if l.strip()]
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith("#EXT-X-STREAM-INF"):
                attrs = dict(
                    item.split("=", 1)
                    for item in re.findall(r'([A-Z\-]+=[^,]+)', line)
                )
                resolution = attrs.get("RESOLUTION")
                if not resolution:
                    i += 1
                    continue
                width, height = map(int, resolution.split("x"))
                bandwidth = int(attrs.get("BANDWIDTH", 0))
                url = lines[i + 1]
                variants.append({
                    "resolution": resolution,
                    "width": width,
                    "height": height,
                    "bandwidth": bandwidth,
                    "url": urljoin(master_url, url),
                    "segments": [],
                })
                i += 2
                continue
            i += 1

        if not variants:
            self.logger.warning("no variants found in master m3u8")
            return {"variants": []}

        # ─── fallback: від більшої якості до меншої ──────────────────────────────
        variants.sort(key=lambda v: v["width"] * v["height"], reverse=True)
        playable_variant = None
        for variant in variants:
            url = variant["url"]
            self.logger.info(f"try variant {variant['resolution']} → {url}")
            try:
                resp = requests.get(url, timeout=self.timeout)
                resp.raise_for_status()
            except requests.RequestException as e:
                self.logger.warning(f"variant unavailable {variant['resolution']}: {e}")
                continue

            variant["segments"] = self.parse_media_m3u8(resp.text, url)
            playable_variant = variant
            break

        return {"variants": variants, "selected": playable_variant}
