"""Общие перечисления системы."""

from __future__ import annotations

from enum import Enum


class Side(str, Enum):
    BUY = "B"
    SELL = "A"

    @property
    def sign(self) -> int:
        return 1 if self is Side.BUY else -1

    @property
    def opposite(self) -> Side:
        return Side.SELL if self is Side.BUY else Side.BUY

    @classmethod
    def from_str(cls, raw: str) -> Side:
        raw = raw.upper()
        if raw in ("B", "BUY", "LONG"):
            return cls.BUY
        if raw in ("A", "S", "SELL", "SHORT"):
            return cls.SELL
        raise ValueError(f"unknown side: {raw!r}")


class EventType(str, Enum):
    FILL = "fill"
    LIQUIDATION_FILL = "liquidation_fill"
    BOOK_SNAPSHOT = "book_snapshot"
    EXTERNAL_LIQUIDATION = "external_liquidation"
    EXTERNAL_CONTEXT = "external_context"
    MARK_PRICE = "mark_price"
    LIQ_LEVELS_SNAPSHOT = "liq_levels_snapshot"   # агрегированные уровни ликвидаций
    STOP_CLUSTERS_SNAPSHOT = "stop_clusters_snapshot"  # агрегированные стоп-кластеры


class LiquidationKind(str, Enum):
    """Как исполнена ликвидация. Разный характер импакта.

    MARKET — агрессивным потоком по стакану.
    BACKSTOP_HLP — вероятна передача в vault HLP (heuristic: bankrupt-флаг
    Ironflow либо крупный остаточный nominal после каскада). Эвристика,
    не истина: у бесплатных источников нет прямого флага HLP.
    """

    MARKET = "market"
    BACKSTOP_HLP = "hlp"


class Cohort(str, Enum):
    UNKNOWN = "unknown"
    MM = "mm"
    INFORMED = "informed"
    DONOR = "donor"
    LIQUIDATOR = "liquidator"


class SignalKind(str, Enum):
    CONTINUATION = "continuation"
    FADE = "fade"


class SignalState(str, Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    KILLED = "killed"


class Regime(str, Enum):
    CASCADE = "cascade"
    QUIET = "quiet"
    INFORMED_TREND = "informed_trend"
    MIXED = "mixed"
