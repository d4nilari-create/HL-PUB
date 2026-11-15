"""StateEngine: четыре книги по монете + персистентность.

LiquidationBook — журнал исполненных ликвидаций и доноры.
StopsBook       — стоп-кластеры и карта ликвидаций (LiqFlow/0xArchive).
FlowQualityBook — online markout-когорты.
L2Book          — стакан и mid-price.

Диспетчеризация событий из шины — on_event (синхронно, в loop ingestion).
Тиковая логика (markout, decay, снапшоты) — on_tick, вызывается из CLI-цикла.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from hlff.config import Config
from hlff.events import (
    BookSnapshot,
    Event,
    ExternalLiquidation,
    Fill,
    LiqLevelsSnapshot,
    LiquidationFill,
    MarkPrice,
    StopClustersSnapshot,
)
from hlff.state.flow_quality import FlowQualityBook
from hlff.state.l2_book import L2Book
from hlff.state.liquidation_book import LiquidationBook
from hlff.state.stops_book import StopsBook
from hlff.utils import get_logger

log = get_logger("state.engine")


class CoinState:
    """Агрегат четырёх книг по одной монете."""

    def __init__(self, coin: str, cfg: Config) -> None:
        self.coin = coin
        self.liquidations = LiquidationBook(coin, window_sec=3600)
        self.stops = StopsBook(coin, cfg.ingestion.liqflow)
        self.stops.fatigue_halflife_sec = cfg.features.pressure.fatigue_halflife_sec
        self.flow = FlowQualityBook(coin, cfg.features)
        self.l2 = L2Book(coin, cfg.features.markout.price_buffer_sec)

    def snapshot(self) -> dict:
        return {
            "liquidations": self.liquidations.snapshot(),
            "stops": self.stops.snapshot(),
            "flow": self.flow.snapshot(),
            "l2": self.l2.snapshot(),
        }

    def restore(self, data: dict) -> None:
        self.liquidations.restore(data.get("liquidations") or {})
        self.stops.restore(data.get("stops") or {})
        self.flow.restore(data.get("flow") or {})
        self.l2.restore(data.get("l2") or {})


class StateEngine:
    """Подписчик шины: обновляет книги и отдаёт снапшоты API/фичам."""

    def __init__(self, cfg: Config, persist_path: str | None = None) -> None:
        self.cfg = cfg
        self.persist_path = Path(persist_path or cfg.system.state_persist_path)
        self.coins: dict[str, CoinState] = {}
        self.started_ms = int(time.time() * 1000)
        self.n_events: int = 0
        self._last_snapshot_ts: float = 0.0

    # -- доступ ------------------------------------------------------------------

    def coin(self, name: str) -> CoinState:
        name = name.upper()
        cs = self.coins.get(name)
        if cs is None:
            cs = CoinState(name, self.cfg)
            self.coins[name] = cs
        return cs

    def tracked_coins(self) -> list[str]:
        return sorted(self.coins)

    # -- подписка ------------------------------------------------------------------

    def on_event(self, ev: Event) -> None:
        self.n_events += 1
        cs = self.coin(ev.coin)
        if isinstance(ev, LiquidationFill):
            cs.liquidations.update(ev)
        elif isinstance(ev, Fill):
            cs.flow.update_fill(ev)
            if cs.l2.mid is None:
                cs.l2.prices.append(ev.ts_exchange_ms, ev.px)
        elif isinstance(ev, BookSnapshot):
            old_mid = cs.l2.mid
            cs.l2.update_book(ev)
            if ev.mid is not None:
                cs.flow.update_mark_price(MarkPrice(coin=ev.coin, ts_exchange_ms=ev.ts_exchange_ms, px=ev.mid))
            self._maybe_note_mid_move(cs, old_mid, cs.l2.mid, ev.ts_exchange_ms)
        elif isinstance(ev, MarkPrice):
            old_mid = cs.l2.mid
            cs.l2.update_mark(ev)
            cs.stops.update_mid(ev)
            cs.flow.update_mark_price(ev)
            self._maybe_note_mid_move(cs, old_mid, cs.l2.mid, ev.ts_exchange_ms)
        elif isinstance(ev, LiqLevelsSnapshot):
            cs.stops.update_liq_levels(ev)
        elif isinstance(ev, StopClustersSnapshot):
            cs.stops.update_stops(ev)
        elif isinstance(ev, ExternalLiquidation):
            pass  # контекст: режимный индикатор, в книги не идёт

    def _maybe_note_mid_move(self, cs: CoinState, old: float | None, new: float | None, ts: int) -> None:
        """Фиксирует проходы цены через стоп-кластеры (fatigue)."""
        if old is None or new is None or old <= 0 or new <= 0 or old == new:
            return
        for crossed in cs.stops.crossed_clusters(new, old):
            cs.stops.mark_triggered(crossed["px"], crossed["notional_usd"], ts)

    # -- тик -------------------------------------------------------------------------

    def on_tick(self, now_ms: int) -> None:
        """Раз в ~секунду: достройка markout и персистентность."""
        for cs in self.coins.values():
            cs.flow.on_tick(now_ms)
        interval = self.cfg.system.snapshot_interval_sec
        if interval > 0 and (now_ms / 1000.0 - self._last_snapshot_ts) >= interval:
            self.save_snapshot()
            self._last_snapshot_ts = now_ms / 1000.0

    # -- persistence --------------------------------------------------------------------

    def save_snapshot(self) -> None:
        try:
            self.persist_path.mkdir(parents=True, exist_ok=True)
            path = self.persist_path / "state.json"
            payload = {
                "saved_ms": int(time.time() * 1000),
                "coins": {c: cs.snapshot() for c, cs in self.coins.items()},
            }
            path.write_text(json.dumps(payload), encoding="utf-8")
            log.debug("state_snapshot_saved", coins=len(self.coins))
        except Exception:
            log.exception("state_snapshot_failed")

    def load_snapshot(self) -> int:
        path = self.persist_path / "state.json"
        if not path.exists():
            return 0
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            n = 0
            for coin, data in (payload.get("coins") or {}).items():
                self.coin(coin).restore(data)
                n += 1
            log.info("state_snapshot_loaded", coins=n)
            return n
        except Exception:
            log.exception("state_snapshot_load_failed")
            return 0

    def status(self) -> dict:
        return {
            "uptime_sec": round((int(time.time() * 1000) - self.started_ms) / 1000, 1),
            "n_events": self.n_events,
            "coins": self.tracked_coins(),
        }
