"""Regime detection: классификация режима по монете.

CASCADE        — ликвидационный номинал за окно > порога;
QUIET          — taker-поток ниже порога и ликвидаций мало;
INFORMED_TREND — встречный informed-поток доминирует (markout-сумма положительна);
MIXED          — всё остальное.
"""

from __future__ import annotations

import time

from hlff.config import FeaturesConfig
from hlff.enums import Regime
from hlff.state.engine import CoinState


def detect_regime(cs: CoinState, cfg: FeaturesConfig, now_ms: int | None = None) -> dict:
    now_ms = now_ms or int(time.time() * 1000)
    rc = cfg.regime
    liq_usd = cs.liquidations.window_notional(now_ms, rc.window_sec)
    taker_usd = cs.flow.taker_notional_per_sec() * 60  # USD/мин
    informed_bps = cs.flow.informed_flow_bps()

    if liq_usd >= rc.cascade_liq_notional_usd:
        regime = Regime.CASCADE
    elif informed_bps >= 0.5:
        regime = Regime.INFORMED_TREND
    elif taker_usd <= rc.quiet_max_taker_notional_usd and liq_usd < rc.cascade_liq_notional_usd * 0.2:
        regime = Regime.QUIET
    else:
        regime = Regime.MIXED

    return {
        "regime": regime.value,
        "liq_notional_window_usd": round(liq_usd, 2),
        "taker_notional_per_min_usd": round(taker_usd, 2),
        "informed_flow_bps": round(informed_bps, 3),
        "window_sec": rc.window_sec,
    }
