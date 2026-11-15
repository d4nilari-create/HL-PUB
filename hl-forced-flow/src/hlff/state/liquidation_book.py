"""LiquidationBook: журнал исполненных ликвидаций по монете.

Хранит rolling-окно ликвидаций, считает номинал/скорость по сторонам и kind
(MARKET vs BACKSTOP_HLP), ведёт доноров (суммарный принудительный номинал
и realized_pnl по адресам).
"""

from __future__ import annotations

import time
from collections import defaultdict, deque

from hlff.enums import LiquidationKind, Side
from hlff.events import LiquidationFill


class LiquidationBook:
    """Окно ликвидаций одной монеты. Потоко-безопасность обеспечивает шина
    (синхронная доставка из одного loop)."""

    def __init__(self, coin: str, window_sec: float = 1800.0, max_donors: int = 50) -> None:
        self.coin = coin
        self.window_ms = int(window_sec * 1000)
        self.max_donors = max_donors
        self._events: deque[LiquidationFill] = deque()
        self.donors: dict[str, dict] = defaultdict(
            lambda: {"notional": 0.0, "pnl": 0.0, "n": 0, "last_ts": 0}
        )
        self.total_notional: float = 0.0
        self.total_count: int = 0

    def update(self, ev: LiquidationFill) -> None:
        self._events.append(ev)
        self.total_notional += ev.notional
        self.total_count += 1
        d = self.donors[ev.liquidated_user]
        d["notional"] += ev.notional
        d["pnl"] += ev.realized_pnl or 0.0
        d["n"] += 1
        d["last_ts"] = ev.ts_exchange_ms
        if len(self.donors) > self.max_donors * 4:
            # компактизация: оставляем топ по номиналу
            top = sorted(self.donors.items(), key=lambda kv: -kv[1]["notional"])[
                : self.max_donors * 2
            ]
            self.donors = defaultdict(
                lambda: {"notional": 0.0, "pnl": 0.0, "n": 0, "last_ts": 0}, dict(top)
            )
        self._trim(ev.ts_exchange_ms)

    def _trim(self, now_ts: int) -> None:
        cutoff = now_ts - self.window_ms
        while self._events and self._events[0].ts_exchange_ms < cutoff:
            self._events.popleft()

    def window_notional(self, now_ts: int, sec: float, side: Side | None = None,
                        kind: LiquidationKind | None = None) -> float:
        """Принудительный номинал за последние sec секунд."""
        self._trim(now_ts)
        cutoff = now_ts - int(sec * 1000)
        total = 0.0
        for ev in reversed(self._events):
            if ev.ts_exchange_ms < cutoff:
                break
            if side is not None and ev.side is not side:
                continue
            if kind is not None and ev.kind is not kind:
                continue
            total += ev.notional
        return total

    def window_count(self, now_ts: int, sec: float) -> int:
        self._trim(now_ts)
        cutoff = now_ts - int(sec * 1000)
        n = 0
        for ev in reversed(self._events):
            if ev.ts_exchange_ms < cutoff:
                break
            n += 1
        return n

    def top_donors(self, limit: int = 10) -> list[dict]:
        rows = sorted(self.donors.items(), key=lambda kv: -kv[1]["notional"])[:limit]
        return [
            {
                "user": u,
                "notional_usd": round(v["notional"], 2),
                "realized_pnl": round(v["pnl"], 2),
                "liquidations": v["n"],
            }
            for u, v in rows
            if v["notional"] > 0
        ]

    # -- snapshot ---------------------------------------------------------------

    def snapshot(self) -> dict:
        return {
            "total_notional": self.total_notional,
            "total_count": self.total_count,
            "donors": {u: dict(v) for u, v in self.donors.items()},
        }

    def restore(self, data: dict) -> None:
        self.total_notional = float(data.get("total_notional", 0.0))
        self.total_count = int(data.get("total_count", 0))
        self.donors = defaultdict(
            lambda: {"notional": 0.0, "pnl": 0.0, "n": 0, "last_ts": 0},
            {u: dict(v) for u, v in (data.get("donors") or {}).items()},
        )


def now_ms() -> int:
    return int(time.time() * 1000)
