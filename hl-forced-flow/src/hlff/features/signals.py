"""Сигналы: Continuation / Fade с прозрачными условиями срабатывания.

Каждый сигнал несёт список conditions — человекочитаемые строки с фактами,
которые его породили. Никакой магии: можно воспроизвести вручную из API.

CONTINUATION — доминирующий ближний стоп/ликвидационный кластер + каскад:
  цена «магнитится» к кластеру, прорыв продолжит движение (forced flow).
FADE — кластер сработал и поглощён: forced-поток иссяк, цена стабилизировалась,
  доноры исчерпаны → отскок против направления каскада.
"""

from __future__ import annotations

import time
import uuid

from hlff.config import Config
from hlff.enums import LiquidationKind, Regime, Side, SignalKind, SignalState
from hlff.features.pressure import compute_pressure_surface
from hlff.features.regime import detect_regime
from hlff.state.engine import CoinState


class Signal:
    """Один живой сигнал."""

    __slots__ = (
        "coin",
        "conditions",
        "created_ms",
        "direction",
        "expires_ms",
        "id",
        "kind",
        "meta",
        "state",
    )

    def __init__(
        self,
        kind: SignalKind,
        coin: str,
        direction: Side,
        conditions: list[str],
        ttl_sec: float,
        meta: dict | None = None,
    ) -> None:
        self.id = uuid.uuid4().hex[:12]
        self.kind = kind
        self.coin = coin
        self.direction = direction
        self.conditions = conditions
        now = int(time.time() * 1000)
        self.created_ms = now
        self.expires_ms = now + int(ttl_sec * 1000)
        self.state = SignalState.ACTIVE
        self.meta = meta or {}

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "kind": self.kind.value,
            "coin": self.coin,
            "direction": "long" if self.direction is Side.BUY else "short",
            "conditions": self.conditions,
            "created_ms": self.created_ms,
            "expires_ms": self.expires_ms,
            "state": self.state.value,
            "meta": self.meta,
        }


class SignalEngine:
    """Порождение и TTL сигналов по состоянию книг."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self._active: list[Signal] = []
        self._cooldown_until: dict[str, float] = {}  # coin:kind:dir -> monotonic
        self._history: list[Signal] = []

    # -- публичный API ----------------------------------------------------------

    def active_signals(self) -> list[dict]:
        return [s.to_dict() for s in self._active if s.state is SignalState.ACTIVE]

    def signal_history(self, limit: int = 50) -> list[dict]:
        return [s.to_dict() for s in self._history[-limit:]]

    def on_tick(self, coins: dict[str, CoinState]) -> list[Signal]:
        """Раз в секунду: обновить TTL, попытаться породить новые. Возвращает новые."""
        now = int(time.time() * 1000)
        for s in self._active:
            if s.state is SignalState.ACTIVE and now >= s.expires_ms:
                s.state = SignalState.EXPIRED
                self._history.append(s)
        self._active = [s for s in self._active if s.state is SignalState.ACTIVE]

        fresh: list[Signal] = []
        for coin, cs in coins.items():
            sig = self._maybe_continuation(coin, cs, now)
            if sig is None:
                sig = self._maybe_fade(coin, cs, now)
            if sig is not None:
                self._active.append(sig)
                self._history.append(sig)
                fresh.append(sig)
                self._cooldown_until[f"{coin}:{sig.kind.value}:{sig.direction.value}"] = (
                    time.monotonic() + self.cfg.signals.ttl_sec
                )
        # ограничение на число активных на монету
        per_coin: dict[str, list[Signal]] = {}
        for s in self._active:
            per_coin.setdefault(s.coin, []).append(s)
        keep: list[Signal] = []
        for coin, sigs in per_coin.items():
            sigs.sort(key=lambda s: -s.created_ms)
            keep.extend(sigs[: self.cfg.signals.max_active_per_coin])
        self._active = keep
        return fresh

    # -- continuation ---------------------------------------------------------------

    def _maybe_continuation(self, coin: str, cs: CoinState, now_ms: int) -> Signal | None:
        cc = self.cfg.signals.continuation
        regime = detect_regime(cs, self.cfg.features, now_ms)
        if regime["regime"] != Regime.CASCADE.value:
            return None

        surface = compute_pressure_surface(cs, self.cfg.features, now_ms)
        mid = surface.get("mid")
        if mid is None:
            return None

        best: tuple[float, str, float] | None = None  # (ratio, dir, notional)
        for b, v in surface["bins"].items():
            for key, d in (("sell_pressure_ratio", Side.SELL), ("buy_pressure_ratio", Side.BUY)):
                ratio = v[key]
                notional = v["forced_sell_usd"] if d is Side.SELL else v["forced_buy_usd"]
                if ratio >= cc.min_pressure_ratio and notional >= cc.min_cluster_notional_usd and (
                    best is None or ratio > best[0]
                ):
                    best = (ratio, d, notional)
        if best is None:
            return None
        ratio, direction, notional = best

        informed = regime["informed_flow_bps"]
        if (
            cs.flow.informed_flow_bps_ewma.initialized
            and informed > cc.max_informed_flow_bps
        ):
            return None  # встречный informed-поток сильнее — не продолжаем

        cd_key = f"{coin}:continuation:{direction.value}"
        if time.monotonic() < self._cooldown_until.get(cd_key, 0.0):
            return None

        near = cs.stops.nearest_liq_level("long" if direction is Side.SELL else "short")
        conditions = [
            f"regime=CASCADE (liq {regime['liq_notional_window_usd']:.0f} USD / {regime['window_sec']}s)",
            f"{direction.value}-pressure ratio {ratio:.2f} >= {cc.min_pressure_ratio}",
            f"cluster notional {notional:.0f} USD >= {cc.min_cluster_notional_usd:.0f}",
            f"informed flow {informed:.2f} bps <= {cc.max_informed_flow_bps}",
        ]
        if near:
            conditions.append(
                f"nearest liq level {near['px']:.6g} ({near['dist_bps']:.1f} bps, {near['notional_usd']:.0f} USD)"
            )
        return Signal(
            SignalKind.CONTINUATION,
            coin,
            direction,
            conditions,
            self.cfg.signals.ttl_sec,
            meta={"pressure_ratio": round(ratio, 2), "cluster_usd": round(notional, 2)},
        )

    # -- fade --------------------------------------------------------------------------

    def _maybe_fade(self, coin: str, cs: CoinState, now_ms: int) -> Signal | None:
        fc = self.cfg.signals.fade
        # fade возможен только после недавнего каскада
        liq_recent = cs.liquidations.window_notional(
            now_ms, self.cfg.features.regime.window_sec, kind=LiquidationKind.MARKET
        )
        if liq_recent < fc.min_donor_notional_usd:
            return None
        # поглощение: суммарный сработавший номинал кластеров (fatigue) велика,
        # а скорость новых ликвидаций упала — forced-поток иссяк
        recent_speed = cs.liquidations.window_notional(now_ms, 20)
        absorbed = any(
            cs.stops.fatigue_at(px) >= fc.cluster_absorption_frac * liq_recent
            for px in cs.stops._fatigue
        )
        stabilized = cs.flow.informed_flow_bps() >= -0.2
        if not (absorbed and stabilized):
            return None
        # направление fade — против последнего каскадного потока
        sell_liq = cs.liquidations.window_notional(now_ms, 300, side=Side.SELL)
        buy_liq = cs.liquidations.window_notional(now_ms, 300, side=Side.BUY)
        if sell_liq > buy_liq * 1.5:
            direction = Side.BUY   # каскад продавцов иссяк → отскок вверх
        elif buy_liq > sell_liq * 1.5:
            direction = Side.SELL
        else:
            return None

        cd_key = f"{coin}:fade:{direction.value}"
        if time.monotonic() < self._cooldown_until.get(cd_key, 0.0):
            return None

        conditions = [
            f"recent MARKET liq {liq_recent:.0f} USD >= {fc.min_donor_notional_usd:.0f}",
            f"cluster fatigue absorbed >= {fc.cluster_absorption_frac:.0%} of flow",
            f"liq speed last 20s {recent_speed:.0f} USD (exhausted)",
            f"informed flow {cs.flow.informed_flow_bps():.2f} bps (stabilized)",
        ]
        return Signal(
            SignalKind.FADE,
            coin,
            direction,
            conditions,
            self.cfg.signals.ttl_sec,
            meta={"liq_window_usd": round(liq_recent, 2)},
        )
