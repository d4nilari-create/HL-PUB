"""FlowQualityBook: качество потока — online markout-когорты с decay.

По crossed-филлам с известным адресом (0xArchive) оцениваем, насколько
адрес «информирован» (положительный markout) или «донор» (отрицательный).
Адреса без user (публичный HL WS) учитываются только в агрегатном taker-потоке.

Механика: при филле фиксируем (ts, px, side_sign); на каждом тике достраиваем
markout на горизонтах из price-ring buffer mid-price; EWMA half-life decay.
"""

from __future__ import annotations

from collections import defaultdict, deque

from hlff.config import FeaturesConfig
from hlff.enums import Cohort
from hlff.events import Fill, MarkPrice
from hlff.utils import Ewma, PriceRingBuffer


class _PendingFill:
    __slots__ = ("notional", "px", "sign", "ts", "user")

    def __init__(self, ts: int, px: float, sign: int, user: str, notional: float) -> None:
        self.ts = ts
        self.px = px
        self.sign = sign
        self.user = user
        self.notional = notional


class FlowQualityBook:
    """Markout-оценки по адресам + агрегированный taker-поток по монете."""

    def __init__(self, coin: str, cfg: FeaturesConfig) -> None:
        self.coin = coin
        self.cfg = cfg
        self.horizons = sorted(cfg.markout_horizons_sec)
        self.prices = PriceRingBuffer(cfg.markout.price_buffer_sec)
        self._pending: deque[_PendingFill] = deque()
        self._max_pending = 50_000
        # user -> {horizon_sec: Ewma(bps), "n": int}
        self._users: dict[str, dict] = defaultdict(dict)
        self._max_users = 30_000
        # агрегаты taker-потока (все источники филлов)
        self.taker_notional_ewma = Ewma(60.0)     # USD/сек, полный поток
        self._taker_notional_last_ts: int | None = None
        self.informed_flow_bps_ewma = Ewma(cfg.markout.decay_half_life_sec)  # суммарный markout свежих тейкеров
        # общий счётчик событий
        self.n_fills: int = 0

    # -- обновления --------------------------------------------------------------

    def update_mark_price(self, ev: MarkPrice) -> None:
        self.prices.append(ev.ts_exchange_ms, ev.px)

    def update_fill(self, ev: Fill) -> None:
        if not ev.crossed:
            return
        self.n_fills += 1
        # скорость taker-потока
        if self._taker_notional_last_ts is not None and ev.ts_exchange_ms > self._taker_notional_last_ts:
            dt = (ev.ts_exchange_ms - self._taker_notional_last_ts) / 1000.0
            if dt > 0:
                self.taker_notional_ewma.update(ev.notional / dt)
        self._taker_notional_last_ts = ev.ts_exchange_ms
        # markout-когорты только по адресам
        if not ev.user:
            return
        self._pending.append(_PendingFill(ev.ts_exchange_ms, ev.px, ev.side.sign, ev.user, ev.notional))
        if len(self._pending) > self._max_pending:
            self._pending.popleft()
        # ранняя стадия: цену филла кладём и в price-buffer (fallback до mid)
        if self.prices.latest() is None:
            self.prices.append(ev.ts_exchange_ms, ev.px)
        if len(self._users) > self._max_users:
            # компактизация: выбрасываем половину мелких
            small = sorted(self._users.items(), key=lambda kv: kv[1].get("n", 0))[
                len(self._users) // 2 :
            ]
            for u, _ in small:
                del self._users[u]

    # -- тик: достройка markout ---------------------------------------------------

    def on_tick(self, now_ts: int) -> None:
        """Вызывается раз в секунду engine-ом: достраивает созревшие markout."""
        max_h = max(self.horizons) if self.horizons else 0
        while self._pending:
            head = self._pending[0]
            age = now_ts - head.ts
            if age < max_h * 1000:
                break
            f = self._pending.popleft()
            px_later = self.prices.px_at_or_before(f.ts + max_h * 1000)
            if px_later is None:
                continue
            base = self.prices.px_at_or_before(f.ts)
            if base is None:
                base = f.px
            for h in self.horizons:
                px_h = self.prices.px_at_or_before(f.ts + h * 1000) or px_later
                bps = (px_h - f.px) / f.px * 10_000 * f.sign
                self._record(f.user, h, bps)
                if h == self.horizons[0]:
                    self.informed_flow_bps_ewma.update(bps * (f.notional / 1000.0))

    def _record(self, user: str, horizon_sec: int, bps: float) -> None:
        rec = self._users[user]
        ew = rec.get(horizon_sec)
        if ew is None:
            ew = Ewma(self.cfg.markout.decay_half_life_sec)
            rec[horizon_sec] = ew
        ew.update(bps)
        rec["n"] = rec.get("n", 0) + 1

    # -- выборки -------------------------------------------------------------------

    def cohort_of(self, user: str) -> Cohort:
        rec = self._users.get(user)
        if not rec or rec.get("n", 0) < self.cfg.markout.min_fills_for_cohort:
            return Cohort.UNKNOWN
        h10 = self.horizons[0] if self.horizons else 10
        ew = rec.get(h10)
        if ew is None or not ew.initialized:
            return Cohort.UNKNOWN
        v = ew.value
        if v <= self.cfg.cohorts.donor_bps_threshold:
            return Cohort.DONOR
        if v >= self.cfg.cohorts.informed_bps_threshold:
            return Cohort.INFORMED
        return Cohort.MM

    def markout_bps(self, user: str, horizon_sec: int | None = None) -> float | None:
        rec = self._users.get(user)
        if not rec:
            return None
        h = horizon_sec or (self.horizons[0] if self.horizons else 10)
        ew = rec.get(h)
        return ew.value if ew and ew.initialized else None

    def top_informed(self, limit: int = 10) -> list[dict]:
        rows = []
        for user, rec in self._users.items():
            if rec.get("n", 0) < self.cfg.markout.min_fills_for_cohort:
                continue
            bps = self.markout_bps(user)
            if bps is None:
                continue
            rows.append({"user": user, "markout_10s_bps": round(bps, 2), "fills": rec["n"]})
        rows.sort(key=lambda r: -r["markout_10s_bps"])
        return rows[:limit]

    def top_donors(self, limit: int = 10) -> list[dict]:
        rows = []
        for user, rec in self._users.items():
            if rec.get("n", 0) < self.cfg.markout.min_fills_for_cohort:
                continue
            bps = self.markout_bps(user)
            if bps is None:
                continue
            rows.append({"user": user, "markout_10s_bps": round(bps, 2), "fills": rec["n"]})
        rows.sort(key=lambda r: r["markout_10s_bps"])
        return rows[:limit]

    def taker_notional_per_sec(self) -> float:
        return self.taker_notional_ewma.value if self.taker_notional_ewma.initialized else 0.0

    def informed_flow_bps(self) -> float:
        return (
            self.informed_flow_bps_ewma.value if self.informed_flow_bps_ewma.initialized else 0.0
        )

    # -- snapshot --------------------------------------------------------------------

    def snapshot(self) -> dict:
        return {
            "n_fills": self.n_fills,
            "users": {
                u: {str(h): ew.value for h, ew in rec.items() if isinstance(h, int)} | {"n": rec.get("n", 0)}
                for u, rec in self._users.items()
            },
        }

    def restore(self, data: dict) -> None:
        self.n_fills = int(data.get("n_fills", 0))
        hl = self.cfg.markout.decay_half_life_sec
        for u, rec in (data.get("users") or {}).items():
            d: dict = {"n": int(rec.get("n", 0))}
            for h, v in rec.items():
                if h == "n":
                    continue
                ew = Ewma(hl)
                ew.value = float(v)
                ew.initialized = True
                d[int(h)] = ew
            self._users[u] = d
