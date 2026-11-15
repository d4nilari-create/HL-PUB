"""L2Book: локальный срез стакана (bids/asks) по монете.

Используется для mid-price (fallback к MarkPrice) и точной оценки pressure
в ближайших бинах. Обновляется полными срезами публичного HL WS.
"""

from __future__ import annotations

from hlff.events import BookSnapshot, MarkPrice
from hlff.utils import PriceRingBuffer


class L2Book:
    def __init__(self, coin: str, price_buffer_sec: int = 1800) -> None:
        self.coin = coin
        self._bids: list[tuple[float, float]] = []  # (px, sz), desc
        self._asks: list[tuple[float, float]] = []  # (px, sz), asc
        self._ts: int = 0
        self._mid: float | None = None
        self.prices = PriceRingBuffer(price_buffer_sec)

    def update_book(self, snap: BookSnapshot) -> None:
        self._bids = sorted(((l.px, l.sz) for l in snap.bids), key=lambda x: -x[0])
        self._asks = sorted(((l.px, l.sz) for l in snap.asks), key=lambda x: x[0])
        self._ts = snap.ts_exchange_ms
        if snap.mid is not None:
            self._mid = snap.mid
            self.prices.append(snap.ts_exchange_ms, snap.mid)

    def update_mark(self, ev: MarkPrice) -> None:
        self._mid = ev.px
        self.prices.append(ev.ts_exchange_ms, ev.px)

    @property
    def mid(self) -> float | None:
        return self._mid

    @property
    def ts(self) -> int:
        return self._ts

    def best_bid(self) -> float | None:
        return self._bids[0][0] if self._bids else None

    def best_ask(self) -> float | None:
        return self._asks[0][0] if self._asks else None

    def depth_notional_within_bps(self, bps: float) -> dict:
        """Номинал покоящейся ликвидности в бине bps от mid, USD."""
        mid = self._mid
        result = {"bid": 0.0, "ask": 0.0}
        if mid is None or mid <= 0:
            return result
        edge_bid = mid * (1 - bps / 10_000)
        edge_ask = mid * (1 + bps / 10_000)
        for px, sz in self._bids:
            if px < edge_bid:
                break
            result["bid"] += px * sz
        for px, sz in self._asks:
            if px > edge_ask:
                break
            result["ask"] += px * sz
        return result

    def snapshot(self) -> dict:
        return {"mid": self._mid, "ts": self._ts}

    def restore(self, data: dict) -> None:
        self._mid = data.get("mid")
        self._ts = int(data.get("ts", 0))
