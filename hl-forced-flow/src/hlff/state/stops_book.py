"""StopsBook: книга стоп-кластеров и уровней ликвидаций по монете.

Хранит последние срезы LiqFlow (приоритет) и 0xArchive (дополнение), умеет:
  * агрегировать кластеры по бинам от mid (для pressure surface);
  * помечать «протухание» (staleness) — срезы не моложе refresh-интервала;
  * считать fatigue — срабатывание кластера при проходе цены (decay EWMA).
"""

from __future__ import annotations

import bisect
import time

from hlff.config import LiqflowConfig
from hlff.events import LiqLevelsSnapshot, MarkPrice, StopClustersSnapshot
from hlff.utils import Ewma


class StopsBook:
    """Стоп-кластеры + карта ликвидаций одной монеты (агрегаты, не ордера)."""

    def __init__(self, coin: str, cfg: LiqflowConfig) -> None:
        self.coin = coin
        self.refresh_ms = int(cfg.levels_refresh_sec * 1000)
        # приоритетный источник — liqflow; oxarchive — fallback/перекрёстная проверка
        self._stops: dict[str, StopClustersSnapshot] = {}
        self._liq_levels: dict[str, LiqLevelsSnapshot] = {}
        self._mid: float | None = None
        self._mid_ts: int = 0
        # fatigue: px -> Ewma сработавшего номинала
        self._fatigue: dict[float, Ewma] = {}
        self.fatigue_halflife_sec: float = 120.0

    # -- обновления -------------------------------------------------------------

    def update_stops(self, snap: StopClustersSnapshot) -> None:
        self._stops[snap.source] = snap
        if snap.mid:
            self._mid, self._mid_ts = snap.mid, snap.ts_exchange_ms

    def update_liq_levels(self, snap: LiqLevelsSnapshot) -> None:
        self._liq_levels[snap.source] = snap
        if snap.mid:
            self._mid, self._mid_ts = snap.mid, snap.ts_exchange_ms

    def update_mid(self, ev: MarkPrice) -> None:
        self._mid, self._mid_ts = ev.px, ev.ts_exchange_ms

    # -- доступ -----------------------------------------------------------------

    @property
    def mid(self) -> float | None:
        return self._mid

    def active_stops(self) -> StopClustersSnapshot | None:
        """Приоритет: liqflow; иначе oxarchive; None если протухло всё."""
        now = int(time.time() * 1000)
        for src in ("liqflow", "oxarchive"):
            snap = self._stops.get(src)
            if snap and now - snap.ts_recv_ms <= self.refresh_ms * 2.5:
                return snap
        return None

    def active_liq_levels(self) -> LiqLevelsSnapshot | None:
        now = int(time.time() * 1000)
        for src in ("liqflow", "oxarchive"):
            snap = self._liq_levels.get(src)
            if snap and now - snap.ts_recv_ms <= self.refresh_ms * 2.5:
                return snap
        return None

    def stale(self) -> bool:
        return self.active_stops() is None and self.active_liq_levels() is None

    # -- бины от mid ------------------------------------------------------------

    def clusters_in_bins(self, bins_bps: list[float]) -> dict[float, dict]:
        """{bin_bps: {'sell': usd_below, 'buy': usd_above}} — стоп-кластеры,
        приведённые к USD и разложенные по бинам от mid.

        sell-стопы ниже mid, buy-стопы выше mid; каждый кластер попадает в бин
        по дистанции от mid в bps.
        """
        mid = self._mid
        result: dict[float, dict] = {b: {"sell": 0.0, "buy": 0.0} for b in bins_bps}
        if mid is None or mid <= 0:
            return result
        snap = self.active_stops()
        if not snap:
            return result
        for cl in snap.clusters:
            dist_bps = abs(cl.px - mid) / mid * 10_000
            for b in bins_bps:
                if dist_bps <= b:
                    if cl.px < mid:
                        result[b]["sell"] += cl.sell_notional
                    else:
                        result[b]["buy"] += cl.buy_notional
                    break
        return result

    def nearest_liq_level(self, side: str = "long") -> dict | None:
        """Ближайший к mid значимый уровень ликвидаций.
        side='long' → уровни long-ликвидаций (принудительный sell), px < mid.
        side='short' → уровни short-ликвидаций, px > mid.
        """
        mid = self._mid
        snap = self.active_liq_levels()
        if mid is None or not snap:
            return None
        best = None
        for lv in snap.levels:
            notional = lv.long_notional if side == "long" else lv.short_notional
            if notional <= 0:
                continue
            if side == "long" and not (lv.px < mid):
                continue
            if side == "short" and not (lv.px > mid):
                continue
            if best is None or abs(lv.px - mid) < abs(best["px"] - mid):
                best = {"px": lv.px, "notional_usd": notional, "dist_bps": abs(lv.px - mid) / mid * 10_000}
        return best

    # -- fatigue ----------------------------------------------------------------

    def mark_triggered(self, px: float, notional_usd: float, now_ts: int) -> None:
        """Отметить срабатывание кластера на уровне px (для fade-логики)."""
        ew = self._fatigue.setdefault(px, Ewma(self.fatigue_halflife_sec))
        ew.update(notional_usd)

    def fatigue_at(self, px: float) -> float:
        ew = self._fatigue.get(px)
        return ew.value if ew and ew.initialized else 0.0

    def _sorted_px(self) -> list[float]:
        snap = self.active_stops()
        if not snap:
            return []
        return sorted(cl.px for cl in snap.clusters)

    def crossed_clusters(self, new_mid: float, old_mid: float) -> list[dict]:
        """Кластеры, через которые прошла цена от old_mid к new_mid."""
        snap = self.active_stops()
        if not snap or old_mid <= 0 or new_mid <= 0:
            return []
        lo, hi = sorted((old_mid, new_mid))
        crossed = []
        for cl in snap.clusters:
            if lo < cl.px < hi:
                notional = cl.sell_notional if new_mid < old_mid else cl.buy_notional
                crossed.append({"px": cl.px, "notional_usd": notional, "direction": "down" if new_mid < old_mid else "up"})
        return crossed

    # -- snapshot ----------------------------------------------------------------

    def snapshot(self) -> dict:
        return {
            "mid": self._mid,
            "mid_ts": self._mid_ts,
            "stops": {
                src: {
                    "ts_recv_ms": s.ts_recv_ms,
                    "mid": s.mid,
                    "clusters": [c.model_dump() for c in s.clusters],
                }
                for src, s in self._stops.items()
            },
            "liq_levels": {
                src: {
                    "ts_recv_ms": s.ts_recv_ms,
                    "mid": s.mid,
                    "levels": [l.model_dump() for l in s.levels],
                }
                for src, s in self._liq_levels.items()
            },
            "fatigue": {str(px): ew.value for px, ew in self._fatigue.items() if ew.initialized},
        }

    def restore(self, data: dict) -> None:
        from hlff.events import LiqLevel, StopCluster

        self._mid = data.get("mid")
        self._mid_ts = int(data.get("mid_ts", 0))
        self._stops = {}
        for src, s in (data.get("stops") or {}).items():
            self._stops[src] = StopClustersSnapshot(
                coin=self.coin,
                ts_exchange_ms=int(s.get("ts_recv_ms", 0)),
                ts_recv_ms=int(s.get("ts_recv_ms", 0)),
                source=src,
                mid=s.get("mid"),
                clusters=[StopCluster(**c) for c in s.get("clusters", [])],
            )
        self._liq_levels = {}
        for src, s in (data.get("liq_levels") or {}).items():
            self._liq_levels[src] = LiqLevelsSnapshot(
                coin=self.coin,
                ts_exchange_ms=int(s.get("ts_recv_ms", 0)),
                ts_recv_ms=int(s.get("ts_recv_ms", 0)),
                source=src,
                mid=s.get("mid"),
                levels=[LiqLevel(**l) for l in s.get("levels", [])],
            )
        self._fatigue = {}
        for px, v in (data.get("fatigue") or {}).items():
            ew = Ewma(self.fatigue_halflife_sec)
            ew.value = float(v)
            ew.initialized = True
            self._fatigue[float(px)] = ew


# re-export для совместимости
_ = bisect  # bisect зарезервирован для будущих бинарных поисков
