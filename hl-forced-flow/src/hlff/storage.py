"""Опциональный sink событий в JSONL (включается env HLFF_STORAGE__PARQUET_DIR).

По умолчанию выключен: hot state живёт в памяти, snapshot — в state.json.
Если директория задана, каждое событие дублируется строкой JSONL по монетам.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from hlff.events import Event
from hlff.utils import get_logger

log = get_logger("storage.sink")


class JsonlSink:
    """Асинхронно выгребает bus.queue и пишет JSONL: <dir>/<date>/<coin>.jsonl."""

    def __init__(self, out_dir: str) -> None:
        self.dir = Path(out_dir)
        self._handles: dict[str, object] = {}

    async def run(self, queue: asyncio.Queue) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        while True:
            ev = await queue.get()
            self._write(ev)

    def _write(self, ev: Event) -> None:
        import json as _json

        path = self.dir / f"{ev.coin}.jsonl"
        try:
            with path.open("a", encoding="utf-8") as f:
                f.write(_json.dumps(ev.model_dump(), default=str) + "\n")
        except Exception:
            log.exception("sink_write_failed", coin=ev.coin)
