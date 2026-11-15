"""Общие утилиты: время, rate-limiter, EWMA, логирование."""

from __future__ import annotations

import asyncio
import logging
import time

import structlog


def now_ms() -> int:
    return int(time.time() * 1000)


def setup_logging(level: str = "INFO") -> None:
    """Единая настройка structlog для всех процессов системы."""
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.dev.ConsoleRenderer(),
        ],
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


class TokenBucket:
    """Простой асинхронный токен-бакет для соблюдения rate-limit внешних API."""

    def __init__(self, rate_per_sec: float, burst: int) -> None:
        self._rate = rate_per_sec
        self._capacity = float(burst)
        self._tokens = float(burst)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, cost: float = 1.0) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self._capacity, self._tokens + (now - self._last) * self._rate
                )
                self._last = now
                if self._tokens >= cost:
                    self._tokens -= cost
                    return
                await asyncio.sleep((cost - self._tokens) / self._rate)


class Ewma:
    """Экспоненциальное взвешенное среднее для online-оценок с decay."""

    def __init__(self, half_life_sec: float) -> None:
        self.half_life_sec = half_life_sec
        self.value: float = 0.0
        self.initialized = False

    @property
    def alpha(self) -> float:
        return 1.0 - 0.5 ** (1.0 / max(self.half_life_sec, 1e-9))

    def update(self, x: float) -> float:
        if not self.initialized:
            self.value = x
            self.initialized = True
        else:
            self.value = self.alpha * x + (1.0 - self.alpha) * self.value
        return self.value

    def decay(self, elapsed_sec: float) -> float:
        """Устаревание значения без новых наблюдений (для fatigue/staleness)."""
        if not self.initialized:
            return 0.0
        factor = 0.5 ** (elapsed_sec / max(self.half_life_sec, 1e-9))
        self.value *= factor
        return self.value


class PriceRingBuffer:
    """Ring buffer (ts_ms, px) для markout-горизонтов; отбрасывает старое."""

    def __init__(self, max_age_sec: float) -> None:
        self.max_age_ms = int(max_age_sec * 1000)
        self._buf: list[tuple[int, float]] = []

    def append(self, ts_ms: int, px: float) -> None:
        self._buf.append((ts_ms, px))
        self._trim(ts_ms)

    def _trim(self, now_ts: int) -> None:
        cutoff = now_ts - self.max_age_ms
        # буфер обычно небольшой/упорядоченный — линейная обрезка с начала
        drop = 0
        for ts, _ in self._buf:
            if ts < cutoff:
                drop += 1
            else:
                break
        if drop:
            del self._buf[:drop]

    def px_at_or_before(self, ts_ms: int) -> float | None:
        """Цена ближайшего среза не позже ts_ms (линейный поиск с конца)."""
        for i in range(len(self._buf) - 1, -1, -1):
            t, px = self._buf[i]
            if t <= ts_ms:
                return px
        return None

    def latest(self) -> float | None:
        return self._buf[-1][1] if self._buf else None

    def latest_ts(self) -> int | None:
        return self._buf[-1][0] if self._buf else None

    def __len__(self) -> int:
        return len(self._buf)
