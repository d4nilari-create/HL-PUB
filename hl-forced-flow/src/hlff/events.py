"""Модуль 1. Нормализованная схема событий.

Все ingestion-адаптеры (Ironflow, LiqFlow, 0xArchive, публичный Hyperliquid)
переводят сырые payload-ы в эти pydantic-модели. Система строго data-only:
ни одного модуля исполнения ордеров.

Временная модель:
  * ``ts_exchange_ms`` — время события по данным источника (для markout/окон).
  * ``ts_recv_ms`` — время получения процессом (мониторинг латентности).
"""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from hlff.enums import EventType, LiquidationKind, Side


def now_ms() -> int:
    import time

    return int(time.time() * 1000)


class BaseEvent(BaseModel):
    """Базовый класс всех событий потока."""

    type: EventType
    coin: str
    ts_exchange_ms: int          # время события по данным источника, мс
    ts_recv_ms: int = Field(default_factory=now_ms)
    venue: str = "hyperliquid"   # hyperliquid | binance | ironflow | liqflow | oxarchive
    seq: int | None = None       # последовательность источника, для gap-детекта


class Fill(BaseEvent):
    """Один агрессивный (taker/crossed) fill.

    У 0xArchive trades есть user_address и crossed-флаг; у публичного HL WS
    trades — только агрегированная сторона, user="" (анонимный тейкер-поток).
    """

    type: EventType = EventType.FILL
    user: str
    side: Side
    px: float
    sz: float
    tid: int | None = None
    crossed: bool = True

    @property
    def notional(self) -> float:
        return self.px * self.sz


class LiquidationFill(BaseEvent):
    """Ликвидация из Ironflow/LiqFlow. kind — эвристика MARKET vs BACKSTOP_HLP."""

    type: EventType = EventType.LIQUIDATION_FILL
    kind: LiquidationKind = LiquidationKind.MARKET
    liquidated_user: str
    side: Side                # сторона принудительного филла: SELL = ликвидируют лонги
    px: float
    sz: float
    realized_pnl: float | None = None
    bankrupt: bool = False

    @property
    def notional(self) -> float:
        return self.px * self.sz


class BookLevel(BaseModel):
    px: float
    sz: float

    @property
    def notional(self) -> float:
        return self.px * self.sz


class BookSnapshot(BaseEvent):
    """Полный срез L2-стакана (публичный HL WS, канал l2Book)."""

    type: EventType = EventType.BOOK_SNAPSHOT
    bids: list[BookLevel] = Field(default_factory=list)
    asks: list[BookLevel] = Field(default_factory=list)

    @property
    def best_bid(self) -> float | None:
        return self.bids[0].px if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0].px if self.asks else None

    @property
    def mid(self) -> float | None:
        bb, ba = self.best_bid, self.best_ask
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2.0


class LiqLevel(BaseModel):
    """Один ценовой уровень агрегированных ликвидаций (LiqFlow / 0xArchive)."""

    px: float
    long_notional: float = 0.0    # принудительный SELL-номинал (ликвидируемые лонги)
    short_notional: float = 0.0   # принудительный BUY-номинал (ликвидируемые шорты)

    @property
    def total_notional(self) -> float:
        return self.long_notional + self.short_notional


class LiqLevelsSnapshot(BaseEvent):
    """Срез карты ликвидаций по монете."""

    type: EventType = EventType.LIQ_LEVELS_SNAPSHOT
    source: str = "liqflow"       # liqflow | oxarchive
    mid: float | None = None
    levels: list[LiqLevel] = Field(default_factory=list)


class StopCluster(BaseModel):
    """Агрегированный кластер стоп-ордеров на уровне цены."""

    px: float
    sell_notional: float = 0.0    # стопы на продажу (лонги выходят) → принудительный SELL
    buy_notional: float = 0.0     # стопы на покупку (шорты выходят) → принудительный BUY
    n_orders: int = 0

    @property
    def total_notional(self) -> float:
        return self.sell_notional + self.buy_notional


class StopClustersSnapshot(BaseEvent):
    """Срез стоп-кластеров по монете (LiqFlow / 0xArchive trigger-levels)."""

    type: EventType = EventType.STOP_CLUSTERS_SNAPSHOT
    source: str = "liqflow"
    mid: float | None = None
    clusters: list[StopCluster] = Field(default_factory=list)


class ExternalLiquidation(BaseEvent):
    """Ликвидация с внешней биржи (Binance forceOrder). Только контекст."""

    type: EventType = EventType.EXTERNAL_LIQUIDATION
    venue: str = "binance"
    side: Side                  # SELL = ликвидируют лонги
    px: float
    sz: float
    notional_usd: float = 0.0


class ExternalContext(BaseEvent):
    """Режимный контекст (опционально): taker long/short ratio и т.п."""

    type: EventType = EventType.EXTERNAL_CONTEXT
    venue: str = "binance"
    metric: str
    value: float
    horizon_sec: int = 0


class MarkPrice(BaseEvent):
    type: EventType = EventType.MARK_PRICE
    px: float


Event = Annotated[
    Fill | LiquidationFill | BookSnapshot | LiqLevelsSnapshot | StopClustersSnapshot | ExternalLiquidation | ExternalContext | MarkPrice,
    Field(discriminator="type"),
]
"""Дискриминированное объединение — единый формат внутри системы."""
