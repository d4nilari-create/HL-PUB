"""LiqFlow: реальные liquidation levels + stop-order clusters с ноды HL.

Base: https://node.liqflow.app (Bearer LIQFLOW_API_KEY).
Используемые endpoint-ы:
  GET /api/coin/{coin}/liq-levels   — уровни ликвидации всех кошельков
  GET /api/coin/{coin}/stop-levels  — неисполненные стоп-ордера (limit+offset)
  GET /api/prices                   — mid-prices (fallback-справка)

Нода обновляет данные примерно раз в 5 минут — поллинг по refresh-интервалу.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import aiohttp

from hlff.config import LiqflowConfig
from hlff.events import (
    LiqLevel,
    LiqLevelsSnapshot,
    MarkPrice,
    StopCluster,
    StopClustersSnapshot,
)
from hlff.ingestion.base import EventBus, EventSource
from hlff.utils import get_logger

log = get_logger("ingestion.liqflow")


def _now_ms() -> int:
    return int(time.time() * 1000)


class LiqflowSource(EventSource):
    name = "liqflow"

    def __init__(self, bus: EventBus, cfg: LiqflowConfig, api_key: str, coins: list[str]) -> None:
        super().__init__(bus)
        self.cfg = cfg
        self.coins = [c.upper() for c in coins]
        self._headers = {"Authorization": f"Bearer {api_key}"}
        self._prices: dict[str, float] = {}
        self._prices_ts: float = 0.0
        if not self.coins:
            raise ValueError("LiqflowSource требует явный список монет (markets.coins)")

    async def run(self) -> None:
        await self._backoff_loop(self._session, min_delay=5.0, max_delay=120.0)

    async def _session(self) -> AsyncIterator[None]:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout, headers=self._headers) as sess:
            await self._poll_all(sess)  # сразу, не ждём первого интервала
            while True:
                await asyncio.sleep(self.cfg.levels_refresh_sec)
                await self._poll_all(sess)
                yield

    async def _poll_all(self, sess: aiohttp.ClientSession) -> None:
        await self._poll_prices(sess)
        any_ok = False
        for coin in self.coins:
            try:
                ok = await self._poll_coin(sess, coin)
                any_ok = any_ok or ok
            except Exception:
                self.bus.set_source_health(self.name, False)
                log.exception("liqflow_coin_failed", coin=coin)
        if any_ok:
            self.bus.set_source_health(self.name, True)

    async def _poll_prices(self, sess: aiohttp.ClientSession) -> None:
        if time.monotonic() - self._prices_ts < 30:
            return
        try:
            async with sess.get(f"{self.cfg.base_url}/api/prices") as resp:
                if resp.status != 200:
                    return
                import orjson

                body = orjson.loads(await resp.read())
            self._prices = {str(k).upper(): float(v) for k, v in (body or {}).items()}
            self._prices_ts = time.monotonic()
            for coin in self.coins:
                px = self._prices.get(coin)
                if px:
                    self.bus.publish(MarkPrice(coin=coin, ts_exchange_ms=_now_ms(), px=px))
        except Exception:  # noqa: BLE001
            log.warning("liqflow_prices_failed")

    async def _poll_coin(self, sess: aiohttp.ClientSession, coin: str) -> bool:
        base = self.cfg.base_url

        # -- liquidation levels --
        async with sess.get(f"{base}/api/coin/{coin}/liq-levels") as resp:
            if resp.status != 200:
                log.warning("liqflow_levels_http", coin=coin, status=resp.status)
                return False
            import orjson

            body = orjson.loads(await resp.read())
        levels = [
            LiqLevel(
                px=float(r["liq_price"]),
                long_notional=float(r.get("long_notional") or 0.0),
                short_notional=float(r.get("short_notional") or 0.0),
            )
            for r in (body.get("levels") or [])
            if r.get("liq_price") is not None
        ]
        if levels:
            self.bus.publish(
                LiqLevelsSnapshot(
                    coin=coin,
                    ts_exchange_ms=_now_ms(),
                    source="liqflow",
                    mid=self._prices.get(coin),
                    levels=levels,
                )
            )

        # -- stop clusters (страницы limit/offset, до ~2000 ордеров на монету) --
        # LiqFlow отдаёт size в базовых единицах → переводим в USD по mid
        mid = self._prices.get(coin)
        clusters: dict[float, StopCluster] = {}
        offset = 0
        page = min(max(self.cfg.stop_levels_limit, 100), 500)
        for _ in range(4):
            params = {"limit": page, "offset": offset}
            async with sess.get(f"{base}/api/coin/{coin}/stop-levels", params=params) as resp:
                if resp.status != 200:
                    log.warning("liqflow_stops_http", coin=coin, status=resp.status)
                    break
                body = orjson.loads(await resp.read())
            orders: list[dict[str, Any]] = body.get("orders") or []
            for o in orders:
                px = o.get("triggerPrice")
                if px is None:
                    continue
                px = float(px)
                cl = clusters.setdefault(px, StopCluster(px=px))
                usd = float(o.get("size") or 0.0) * (mid or px)
                if str(o.get("side", "")).upper() == "SELL":
                    cl.sell_notional += usd
                else:
                    cl.buy_notional += usd
                cl.n_orders += 1
            total = int(body.get("total") or len(orders))
            offset += len(orders)
            if not orders or offset >= total:
                break

        if clusters:
            self.bus.publish(
                StopClustersSnapshot(
                    coin=coin,
                    ts_exchange_ms=_now_ms(),
                    source="liqflow",
                    mid=self._prices.get(coin),
                    clusters=sorted(clusters.values(), key=lambda c: c.px),
                )
            )
        return True
