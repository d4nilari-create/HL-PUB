"""0xArchive Free: история trades (30 дней) + дополнительные срезы.

Base: https://api.0xarchive.io (header X-API-Key).
Используемые endpoint-ы:
  GET /v1/hyperliquid/trades/{symbol}?limit=&cursor=  — страницы trades
      (data[].crossed=True — тейкерская сторона с user_address)
  GET /v1/hyperliquid/liquidations/{symbol}/levels     — карта ликвидаций
  GET /v1/hyperliquid/orders/{symbol}/trigger-levels   — стоп/триггер уровни

Live-поллинг: каждые trades_poll_sec забираем последние trades без курсора
и дедуплицируем по (trade_id, user_address). История по курсору — опционально
через scripts/fetch_history.py.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import UTC
from typing import Any

import aiohttp

from hlff.config import OxarchiveConfig
from hlff.enums import Side
from hlff.events import (
    Fill,
    LiqLevel,
    LiqLevelsSnapshot,
    StopCluster,
    StopClustersSnapshot,
)
from hlff.ingestion.base import EventBus, EventSource
from hlff.utils import get_logger

log = get_logger("ingestion.oxarchive")


def _now_ms() -> int:
    return int(time.time() * 1000)


class OxarchiveSource(EventSource):
    name = "oxarchive"

    def __init__(self, bus: EventBus, cfg: OxarchiveConfig, api_key: str, coins: list[str]) -> None:
        super().__init__(bus)
        self.cfg = cfg
        self.coins = [c.upper() for c in coins]
        self._headers = {"X-API-Key": api_key}
        self._seen_tids: set[str] = set()
        self._seen_cap = 200_000
        if not self.coins:
            raise ValueError("OxarchiveSource требует явный список монет (markets.coins)")

    async def run(self) -> None:
        await self._backoff_loop(self._session, min_delay=3.0, max_delay=120.0)

    async def _session(self) -> AsyncIterator[None]:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout, headers=self._headers) as sess:
            last_levels = 0.0
            while True:
                await self._poll_trades(sess)
                now = time.monotonic()
                if now - last_levels >= self.cfg.levels_poll_sec:
                    await self._poll_levels(sess)
                    last_levels = now
                await asyncio.sleep(self.cfg.trades_poll_sec)
                yield

    # -- trades -----------------------------------------------------------------

    async def _poll_trades(self, sess: aiohttp.ClientSession) -> None:
        any_ok = False
        for coin in self.coins:
            url = f"{self.cfg.base_url}/v1/hyperliquid/trades/{coin}"
            try:
                async with sess.get(url, params={"limit": self.cfg.trades_limit}) as resp:
                    if resp.status == 401:
                        log.error("oxarchive_auth_failed")
                        self.bus.set_source_health(self.name, False)
                        return
                    if resp.status != 200:
                        log.warning("oxarchive_trades_http", coin=coin, status=resp.status)
                        continue
                    import orjson

                    body = orjson.loads(await resp.read())
                any_ok = True
                rows: list[dict[str, Any]] = body.get("data") or []
                # API отдаёт от новых к старым — публикуем в хронологии
                for row in reversed(rows):
                    ev = self._parse_fill(coin, row)
                    if ev is not None:
                        self.bus.publish(ev)
            except Exception:
                self.bus.set_source_health(self.name, False)
                log.exception("oxarchive_trades_failed", coin=coin)
        if any_ok:
            self.bus.set_source_health(self.name, True)

    def _parse_fill(self, coin: str, row: dict[str, Any]) -> Fill | None:
        tid = row.get("trade_id")
        user = str(row.get("user_address") or "")
        if tid is None:
            return None
        key = f"{tid}:{user}:{row.get('side')}"
        if key in self._seen_tids:
            return None
        self._seen_tids.add(key)
        if len(self._seen_tids) > self._seen_cap:
            self._seen_tids = set(list(self._seen_tids)[self._seen_cap // 2 :])

        try:
            px = float(row["price"])
            sz = float(row["size"])
            ts = _parse_ts(str(row["timestamp"]))
            side = Side.from_str(str(row["side"]))
        except (KeyError, TypeError, ValueError):
            return None
        return Fill(
            coin=coin,
            ts_exchange_ms=ts,
            venue="oxarchive",
            user=user,
            side=side,
            px=px,
            sz=sz,
            tid=int(tid),
            crossed=bool(row.get("crossed", True)),
        )

    # -- levels -------------------------------------------------------------------

    async def _poll_levels(self, sess: aiohttp.ClientSession) -> None:
        for coin in self.coins:
            try:
                await self._poll_liq_levels(sess, coin)
                await self._poll_trigger_levels(sess, coin)
            except Exception:
                log.exception("oxarchive_levels_failed", coin=coin)

    async def _poll_liq_levels(self, sess: aiohttp.ClientSession, coin: str) -> None:
        url = f"{self.cfg.base_url}/v1/hyperliquid/liquidations/{coin}/levels"
        async with sess.get(url) as resp:
            if resp.status != 200:
                return
            import orjson

            body = orjson.loads(await resp.read())
        data = body.get("data") or {}
        levels = [
            LiqLevel(
                px=float(r["price"]),
                long_notional=float(r.get("long_notional") or 0.0),
                short_notional=float(r.get("short_notional") or 0.0),
            )
            for r in (data.get("levels") or [])
            if r.get("price") is not None
        ]
        if levels:
            self.bus.publish(
                LiqLevelsSnapshot(
                    coin=coin,
                    ts_exchange_ms=_now_ms(),
                    source="oxarchive",
                    mid=data.get("mid_price"),
                    levels=levels,
                )
            )

    async def _poll_trigger_levels(self, sess: aiohttp.ClientSession, coin: str) -> None:
        url = f"{self.cfg.base_url}/v1/hyperliquid/orders/{coin}/trigger-levels"
        async with sess.get(url) as resp:
            if resp.status != 200:
                return
            import orjson

            body = orjson.loads(await resp.read())
        data = body.get("data") or {}
        mid = data.get("mid_price")
        clusters = [
            StopCluster(
                px=float(r["price_bucket"]),
                sell_notional=float(r.get("ask_size") or 0.0) * (mid or 0.0),
                buy_notional=float(r.get("bid_size") or 0.0) * (mid or 0.0),
                n_orders=int(r.get("ask_count") or 0) + int(r.get("bid_count") or 0),
            )
            for r in (data.get("levels") or [])
            if r.get("price_bucket") is not None
        ]
        if clusters:
            self.bus.publish(
                StopClustersSnapshot(
                    coin=coin,
                    ts_exchange_ms=_now_ms(),
                    source="oxarchive",
                    mid=mid,
                    clusters=sorted(clusters, key=lambda c: c.px),
                )
            )


def _parse_ts(raw: str) -> int:
    """ISO-8601 '2026-11-14T02:33:41.339Z' → epoch ms (UTC)."""
    from datetime import datetime

    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)
