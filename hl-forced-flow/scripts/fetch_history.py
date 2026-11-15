#!/usr/bin/env python3
"""Скачать историю trades с 0xArchive (последние 30 дней, бесплатно).

Использование:
    python scripts/fetch_history.py --coin BTC --hours 24 --out data/history
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hlff.config import get_config, get_secrets


def _parse_ts(raw: str) -> int:
    from datetime import datetime

    return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() * 1000)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--coin", default="BTC")
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--out", default="data/history")
    args = ap.parse_args()

    cfg = get_config()
    secrets = get_secrets()
    base = cfg.ingestion.oxarchive.base_url
    cutoff_ms = int(asyncio.get_event_loop().time() * 0)  # placeholder
    import time

    cutoff_ms = int((time.time() - args.hours * 3600) * 1000)

    import aiohttp

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{args.coin.upper()}_trades.jsonl"

    headers = {"X-API-Key": secrets.OXARCHIVE_API_KEY}
    cursor: str | None = None
    n = 0
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as sess:
        while True:
            params = {"limit": 1000}
            if cursor:
                params["cursor"] = cursor
            async with sess.get(
                f"{base}/v1/hyperliquid/trades/{args.coin.upper()}", params=params
            ) as resp:
                if resp.status != 200:
                    print(f"HTTP {resp.status} — стоп")
                    break
                import orjson

                body = orjson.loads(await resp.read())
            rows = body.get("data") or []
            if not rows:
                break
            oldest_in_page = None
            with out.open("a", encoding="utf-8") as f:
                for r in rows:
                    ts = _parse_ts(str(r["timestamp"]))
                    oldest_in_page = ts
                    if ts < cutoff_ms:
                        continue
                    f.write(json.dumps(r, default=str) + "\n")
                    n += 1
            cursor = (body.get("meta") or {}).get("next_cursor")
            print(f"загружено {n} строк, cursor={cursor}")
            if not cursor or (oldest_in_page is not None and oldest_in_page < cutoff_ms):
                break
    print(f"готово: {n} строк → {out}")


if __name__ == "__main__":
    asyncio.run(main())
