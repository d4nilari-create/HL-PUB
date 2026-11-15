"""Side-channel: Binance forceOrder (бесплатный WS, опционально).

Даёт внешний контекст каскадов ликвидаций. По умолчанию выключен.
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator
from typing import Any

import websockets

from hlff.config import BinanceConfig
from hlff.enums import Side
from hlff.events import ExternalLiquidation
from hlff.ingestion.base import EventBus, EventSource
from hlff.utils import get_logger

log = get_logger("ingestion.side_channels")


def _now_ms() -> int:
    return int(time.time() * 1000)


class BinanceForceOrderSource(EventSource):
    name = "binance_forceorder"

    def __init__(self, bus: EventBus, cfg: BinanceConfig, coins: list[str]) -> None:
        super().__init__(bus)
        self.cfg = cfg
        self.coins = {c.upper() for c in coins}

    async def run(self) -> None:
        await self._backoff_loop(self._session, min_delay=2.0, max_delay=60.0)

    async def _session(self) -> AsyncIterator[None]:
        streams = "/".join(f"{s.lower()}@forceOrder" for s in self.cfg.symbols)
        url = f"{self.cfg.ws_url}/{streams}"
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            log.info("binance_forceorder_connected")
            self.bus.set_source_health(self.name, True)
            async for raw in ws:
                try:
                    msg: dict[str, Any] = json.loads(raw)
                    o = (msg.get("o") or {}).get("o") or {}
                    sym = str(o.get("s", "")).replace("USDT", "").upper()
                    if self.coins and sym not in self.coins:
                        continue
                    side = Side.SELL if str(o.get("S", "")).upper() == "SELL" else Side.BUY
                    px = float(o.get("ap") or o.get("p") or 0.0)
                    sz = float(o.get("q") or 0.0)
                    if px <= 0 or sz <= 0:
                        continue
                    self.bus.publish(
                        ExternalLiquidation(
                            coin=sym,
                            ts_exchange_ms=int(msg.get("T") or _now_ms()),
                            venue="binance",
                            side=side,
                            px=px,
                            sz=sz,
                            notional_usd=px * sz,
                        )
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                except Exception:
                    log.exception("binance_parse_error")
            yield  # pragma: no cover
