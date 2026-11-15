"""Ironflow: основной live-источник ликвидаций Hyperliquid.

Live: GET {base}/v1/liquidations/stream — SSE, событие ``liquidation``.
Historical catch-up: GET {base}/v1/liquidations?limit=&symbol=&after=<cursor>.
Авторизация: Bearer IRONFLOW_API_KEY.

Нормализация: market "BTC-PERP" → coin "BTC"; side buy/sell — сторона
принудительного филла (sell = ликвидируют лонги). kind: bankrupt=True либо
крупный номинал → вероятный BACKSTOP_HLP (эвристика).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import aiohttp

from hlff.config import IronflowConfig
from hlff.enums import LiquidationKind, Side
from hlff.events import LiquidationFill
from hlff.ingestion.base import EventBus, EventSource
from hlff.utils import get_logger

log = get_logger("ingestion.ironflow")

# Номинал, выше которого ликвидация считается вероятным backstop/HLP-случаем.
_HLP_MIN_NOTIONAL_USD = 250_000.0


def coin_from_market(market: str) -> str:
    """'BTC-PERP' -> 'BTC', 'para:DRV-PERP' -> 'DRV'."""
    base = market.split(":")[-1]
    for suf in ("-PERP", "-USDC", "-USDT"):
        if base.endswith(suf):
            base = base[: -len(suf)]
            break
    return base


class IronflowSource(EventSource):
    name = "ironflow"

    def __init__(self, bus: EventBus, cfg: IronflowConfig, api_key: str, coins: list[str]) -> None:
        super().__init__(bus)
        self.cfg = cfg
        self.api_key = api_key
        self.coins = {c.upper() for c in coins}  # пустое = все
        self._headers = {"Authorization": f"Bearer {api_key}"}
        self._seen: set[str] = set()  # дедупликация history vs stream
        self._seen_cap = 200_000

    async def run(self) -> None:
        await self._backoff_loop(
            self._session,
            min_delay=self.cfg.reconnect_min_sec,
            max_delay=self.cfg.reconnect_max_sec,
        )

    async def _session(self) -> AsyncIterator[None]:
        timeout = aiohttp.ClientTimeout(total=None, sock_read=90)
        async with aiohttp.ClientSession(timeout=timeout, headers=self._headers) as sess:
            await self._catch_up_history(sess)
            await self._stream(sess)
            self.bus.set_source_health(self.name, False)
            yield  # pragma: no cover — сессия закончилась, backoff снаружи

    # -- live SSE ------------------------------------------------------------

    async def _stream(self, sess: aiohttp.ClientSession) -> None:
        url = f"{self.cfg.base_url}{self.cfg.stream_path}"
        async with sess.get(url) as resp:
            if resp.status != 200:
                log.error("ironflow_stream_http", status=resp.status)
                self.bus.set_source_health(self.name, False)
                await asyncio.sleep(5)
                return
            log.info("ironflow_stream_connected")
            buf = b""
            async for chunk in resp.content.iter_any():
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line.startswith(b"data:"):
                        continue
                    payload = line[5:].strip()
                    if not payload or payload == b"[DONE]":
                        continue
                    try:
                        import orjson

                        ev = self._parse_liq(orjson.loads(payload), live=True)
                    except Exception:
                        log.exception("ironflow_parse_error")
                        continue
                    if ev is not None:
                        self.bus.publish(ev)

    # -- historical catch-up -------------------------------------------------

    async def _catch_up_history(self, sess: aiohttp.ClientSession) -> None:
        """Один проход по recent history, чтобы книга не начиналась пустой."""
        url = f"{self.cfg.base_url}{self.cfg.history_path}"
        try:
            async with sess.get(url, params={"limit": 200}) as resp:
                if resp.status != 200:
                    log.warning("ironflow_history_http", status=resp.status)
                    return
                import orjson

                body = orjson.loads(await resp.read())
            rows: list[dict[str, Any]] = body.get("data") or []
            n = 0
            for row in reversed(rows):  # хронологический порядок
                ev = self._parse_liq(row, live=False)
                if ev is not None:
                    self.bus.publish(ev)
                    n += 1
            if n:
                log.info("ironflow_history_loaded", events=n)
                self.bus.set_source_health(self.name, True)
        except Exception:
            log.exception("ironflow_history_failed")  # не критично — есть stream

    # -- parsing -------------------------------------------------------------

    def _parse_liq(self, row: dict[str, Any], live: bool) -> LiquidationFill | None:
        market = str(row.get("market") or row.get("display_symbol") or "")
        coin = coin_from_market(market)
        if self.coins and coin not in self.coins:
            return None
        if str(row.get("market_type", "perp")).lower() not in ("perp", "perpetual"):
            return None

        key = f"{row.get('hash')}:{row.get('address')}:{row.get('sequence')}:{row.get('side')}"
        if key in self._seen:
            return None
        self._seen.add(key)
        if len(self._seen) > self._seen_cap:
            # компактизация: выбрасываем половину (set порядка не гарантирует — ок)
            self._seen = set(list(self._seen)[self._seen_cap // 2 :])

        try:
            px = float(row["price"])
            sz = float(row["size"])
            ts = int(row["timestamp"])
        except (KeyError, TypeError, ValueError):
            return None

        notional = px * sz
        bankrupt = bool(row.get("bankrupt", False))
        kind = LiquidationKind.BACKSTOP_HLP if bankrupt or notional >= _HLP_MIN_NOTIONAL_USD else LiquidationKind.MARKET

        return LiquidationFill(
            coin=coin,
            ts_exchange_ms=ts,
            seq=row.get("sequence"),
            venue="ironflow",
            kind=kind,
            liquidated_user=str(row.get("address", "")),
            side=Side.from_str(str(row.get("side", "sell"))),
            px=px,
            sz=sz,
            realized_pnl=_f(row.get("realized_pnl")),
            bankrupt=bankrupt,
        )


def _f(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None
