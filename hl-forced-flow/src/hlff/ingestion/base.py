"""Инфраструктура ingestion: шина событий и интерфейс источников.

Архитектурный контракт:
  * Каждый источник (Ironflow, LiqFlow, 0xArchive, публичный HL WS) —
    самостоятельная asyncio-задача, публикующая нормализованные события в шину.
  * Подписчики (State Engine, storage-sink, API) получают события через шину
    и не знают об источниках.
  * Источник обязан сам переподключаться (backoff). Если источник недоступен,
    шина помечает его degraded — graceful degradation, остальные продолжают.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable

from hlff.events import Event
from hlff.utils import get_logger

log = get_logger("ingestion.base")

EventHandler = Callable[[Event], None]


class EventBus:
    """In-process шина: источники публикуют, подписчики обрабатывают синхронно
    (hot path без лишних await). Медленным подписчикам — асинхронная очередь."""

    def __init__(self, max_queue: int = 65536) -> None:
        self._handlers: list[EventHandler] = []
        self._source_health: dict[str, bool] = {}
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue)
        self._dropped = 0

    def subscribe(self, handler: EventHandler) -> None:
        self._handlers.append(handler)

    def publish(self, event: Event) -> None:
        """Синхронная доставка подписчикам (state engine)."""
        for h in self._handlers:
            try:
                h(event)
            except Exception:
                log.exception("handler_failed", etype=str(event.type))

    async def publish_async(self, event: Event) -> None:
        """Асинхронная очередь для медленных подписчиков (storage)."""
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            self._dropped += 1
            if self._dropped % 1000 == 1:
                log.warning("bus_queue_full", dropped=self._dropped)

    def set_source_health(self, name: str, healthy: bool) -> None:
        if self._source_health.get(name) != healthy:
            log.warning("source_health_changed", source=name, healthy=healthy)
        self._source_health[name] = healthy

    def degraded_sources(self) -> list[str]:
        return [k for k, v in self._source_health.items() if not v]

    def sources_health(self) -> dict[str, bool]:
        return dict(self._source_health)

    @property
    def dropped_events(self) -> int:
        return self._dropped


class EventSource(abc.ABC):
    """Интерфейс источника данных. Каждый источник сам управляет соединением."""

    name: str = "abstract"

    def __init__(self, bus: EventBus) -> None:
        self.bus = bus

    @abc.abstractmethod
    async def run(self) -> None:
        """Бесконечный цикл: подключение → чтение → публикация → reconnect."""

    async def _backoff_loop(
        self,
        run_once: Callable[[], AsyncIterator[None]],
        min_delay: float = 1.0,
        max_delay: float = 60.0,
    ) -> None:
        """Обёртка: run_once — асинхенератор одной сессии; переподключение с backoff."""
        delay = min_delay
        while True:
            try:
                async for _ in run_once():
                    delay = min_delay  # успешная работа — сброс backoff
                self.bus.set_source_health(self.name, False)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.bus.set_source_health(self.name, False)
                log.exception("source_error", source=self.name)
            log.warning("source_reconnecting", source=self.name, delay=delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2.0, max_delay)


async def cancel_all(tasks: list[asyncio.Task]) -> None:
    """Graceful shutdown: отмена задач с ожиданием завершения."""
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t
    log.info("all_tasks_cancelled", n=len(tasks))
