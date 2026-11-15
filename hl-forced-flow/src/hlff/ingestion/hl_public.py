"""Публичный Hyperliquid: WS (trades, l2Book) + REST /info (allMids fallback).

Без ключа. Публикует:
  * Fill (user="", crossed=True) — анонимный тейкер-поток для pressure/regime;
  * BookSnapshot — L2-книга для mid-price и pressure surface;
  * MarkPrice — из allMids, если WS-канал не поднялся.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import Any

import aiohttp
import websockets

from hlff.config import HlConfig
from hlff.enums import Side
from hlff.events import BookLevel, BookSnapshot, Fill, MarkPrice
from hlff.ingestion.base import EventBus, EventSource
from hlff.utils import get_logger

log = get_logger("ingestion.hl_public")


def _now_ms() -> int:
    return int(time.time() * 1000)


class HlPublicSource(EventSource):
    name = "hl_public"

    def __init__(self, bus: EventBus, cfg: HlConfig, coins: list[str], book_levels: int = 20) -> None:
        super().__init__(bus)
        self.cfg = cfg
        self.coins = [c.upper() for c in coins]
        self.book_levels = book_levels
        if not self.coins:
            raise ValueError("HlPublicSource требует явный список монет (markets.coins)")

    async def run(self) -> None:
        await self._backoff_loop(
            self._session,
            min_delay=self.cfg.reconnect_min_sec,
            max_delay=self.cfg.reconnect_max_sec,
        )

    async def _session(self) -> AsyncIterator[None]:
        ws_task = asyncio.create_task(self._ws_loop())
        mids_task = asyncio.create_task(self._mids_loop())
        try:
            done, _ = await asyncio.wait({ws_task, mids_task}, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                exc = t.exception()
                if exc:
                    raise exc
            yield  # pragma: no cover — сессия завершилась, reconnect через backoff
        finally:
            ws_task.cancel()
            mids_task.cancel()

    # -- websocket -------------------------------------------------------------

    async def _ws_loop(self) -> None:
        async with websockets.connect(
            self.cfg.ws_url, ping_interval=20, ping_timeout=20, max_queue=4096
        ) as ws:
            for coin in self.coins:
                for ch in ("trades", "l2Book"):
                    await ws.send(
                        json.dumps({"method": "subscribe", "subscription": {"type": ch, "coin": coin}})
                    )
            log.info("hl_ws_connected", coins=self.coins)
            self.bus.set_source_health(self.name, True)
            async for raw in ws:
                try:
                    import orjson

                    msg: dict[str, Any] = orjson.loads(raw)
                except Exception:  # noqa: BLE001
                    log.debug("hl_ws_bad_frame")
                    continue
                channel = msg.get("channel")
                data = msg.get("data")
                if channel == "trades":
                    for t in data or []:
                        ev = self._parse_trade(t)
                        if ev:
                            self.bus.publish(ev)
                elif channel == "l2Book":
                    ev = self._parse_book(data)
                    if ev:
                        self.bus.publish(ev)
                elif channel == "subscriptionResponse":
                    err = (data or {}).get("data") if isinstance(data, dict) else None
                    if err:
                        log.warning("hl_ws_sub_response", data=data)

    def _parse_trade(self, t: dict[str, Any]) -> Fill | None:
        try:
            coin = str(t["coin"]).upper()
            if coin not in self.coins:
                return None
            return Fill(
                coin=coin,
                ts_exchange_ms=int(t.get("time") or _now_ms()),
                venue="hyperliquid",
                user="",
                side=Side.from_str(str(t["side"])),
                px=float(t["px"]),
                sz=float(t["sz"]),
                tid=int(t.get("tid") or 0) or None,
                crossed=True,
            )
        except (KeyError, TypeError, ValueError):
            return None

    def _parse_book(self, data: dict[str, Any]) -> BookSnapshot | None:
        try:
            coin = str(data["coin"]).upper()
            if coin not in self.coins:
                return None
            levels = data.get("levels") or [[], []]

            def _lvl(entry: Any) -> BookLevel | None:
                # формат: {'px': '...', 'sz': '...', 'n': ...} или [px, sz]
                try:
                    if isinstance(entry, dict):
                        return BookLevel(px=float(entry["px"]), sz=float(entry["sz"]))
                    return BookLevel(px=float(entry[0]), sz=float(entry[1]))
                except (KeyError, TypeError, ValueError, IndexError):
                    return None

            bids = [b for b in (_lvl(e) for e in levels[0]) if b is not None][: self.book_levels]
            asks = [a for a in (_lvl(e) for e in levels[1]) if a is not None][: self.book_levels]
            return BookSnapshot(
                coin=coin,
                ts_exchange_ms=int(data.get("time") or _now_ms()),
                venue="hyperliquid",
                bids=bids,
                asks=asks,
            )
        except (KeyError, TypeError, ValueError):
            return None

    # -- REST fallback: allMids -------------------------------------------------

    async def _mids_loop(self) -> None:
        """Раз в mids_poll_sec дёргаем allMids; живёт, пока жив WS."""
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            while True:
                await asyncio.sleep(self.cfg.mids_poll_sec)
                try:
                    async with sess.post(
                        self.cfg.info_url,
                        json={"type": "allMids"},
                        headers={"Content-Type": "application/json"},
                    ) as resp:
                        if resp.status != 200:
                            continue
                        import orjson

                        body = orjson.loads(await resp.read())
                    for coin, px in (body.get("mids") or {}).items():
                        coin = str(coin).upper()
                        if coin in self.coins:
                            self.bus.publish(
                                MarkPrice(coin=coin, ts_exchange_ms=_now_ms(), px=float(px))
                            )
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    log.debug("hl_mids_poll_failed")
