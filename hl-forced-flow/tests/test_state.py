"""Тесты state engine: книги, pressure, сигналы."""

from __future__ import annotations

import time

from hlff.config import get_config
from hlff.enums import LiquidationKind, Regime, Side
from hlff.events import (
    Fill,
    LiqLevel,
    LiqLevelsSnapshot,
    LiquidationFill,
    MarkPrice,
    StopCluster,
    StopClustersSnapshot,
)
from hlff.features.pressure import compute_pressure_surface
from hlff.features.regime import detect_regime
from hlff.features.signals import SignalEngine
from hlff.state.engine import StateEngine


def _cfg():
    return get_config()


def test_liquidation_book_window():
    cfg = _cfg()
    eng = StateEngine(cfg, persist_path="/tmp/hlff-test")
    now = int(time.time() * 1000)
    for i in range(5):
        eng.on_event(
            LiquidationFill(
                coin="BTC", ts_exchange_ms=now - (5 - i) * 10_000,
                liquidated_user=f"0x{i}", side=Side.SELL, px=64_000.0, sz=1.0,
                kind=LiquidationKind.MARKET, realized_pnl=-100.0,
            )
        )
    cs = eng.coins["BTC"]
    assert cs.liquidations.window_notional(now, 60) == 5 * 64_000.0
    assert cs.liquidations.window_notional(now, 15) == 64_000.0  # только самый свежий
    assert cs.liquidations.window_notional(now, 60, side=Side.SELL) == 5 * 64_000.0
    donors = cs.liquidations.top_donors(3)
    assert len(donors) == 3 and donors[0]["notional_usd"] == 64_000.0


def test_stops_book_bins_and_staleness():
    cfg = _cfg()
    eng = StateEngine(cfg, persist_path="/tmp/hlff-test")
    now = int(time.time() * 1000)
    cs = eng.coin("BTC")
    cs.stops.update_stops(
        StopClustersSnapshot(
            coin="BTC", ts_exchange_ms=now, source="liqflow", mid=64_000.0,
            clusters=[
                StopCluster(px=63_900.0, sell_notional=500_000.0, n_orders=10),
                StopCluster(px=64_100.0, buy_notional=300_000.0, n_orders=7),
            ],
        )
    )
    bins = cs.stops.clusters_in_bins([5, 10, 25, 50])
    # 100 USD / 64000 ≈ 15.6 bps → кластеры попадают только в бин 25
    assert bins[5]["sell"] == 0.0
    assert bins[10]["sell"] == 0.0
    assert bins[25]["sell"] == 500_000.0
    assert bins[25]["buy"] == 300_000.0
    assert bins[50]["sell"] == 0.0  # break на первом подходящем бине
    assert not cs.stops.stale()


def test_pressure_surface_and_cascade_signal():
    cfg = _cfg()
    eng = StateEngine(cfg, persist_path="/tmp/hlff-test")
    now = int(time.time() * 1000)
    cs = eng.coin("BTC")
    # mid через mark price
    cs.l2.update_mark(MarkPrice(coin="BTC", ts_exchange_ms=now, px=64_000.0))
    # каскад ликвидаций сверх порога
    for i in range(10):
        eng.on_event(
            LiquidationFill(
                coin="BTC", ts_exchange_ms=now - i * 1_000,
                liquidated_user=f"0x{i}", side=Side.SELL, px=64_000.0, sz=4.0,
                kind=LiquidationKind.MARKET,
            )
        )
    # плотный кластер long-ликвидаций чуть ниже mid (1.5–7.8 bps → бины 5 и 10)
    levels = [LiqLevel(px=64_000.0 - d, long_notional=2_000_000.0) for d in (10, 20, 30, 40, 50)]
    cs.stops.update_liq_levels(
        LiqLevelsSnapshot(coin="BTC", ts_exchange_ms=now, source="liqflow", mid=64_000.0, levels=levels)
    )
    surf = compute_pressure_surface(cs, cfg.features, now)
    assert surf["mid"] == 64_000.0
    # d=10/20/30 → 1.6–4.7 bps (бин 5); d=40/50 → 6.3–7.8 bps (бин 10)
    assert surf["bins"]["5"]["forced_sell_usd"] == 6_000_000.0
    assert surf["bins"]["10"]["forced_sell_usd"] == 4_000_000.0
    regime = detect_regime(cs, cfg.features, now)
    assert regime["regime"] == Regime.CASCADE.value

    # кластер в 10 bps: 4M < порога 100k? нет — но ratio: медиана = 3M,
    # бин 5 ratio 6M/3M = 2.0 < 2.5 — добавим стопы, чтобы усилить ближний бин
    cs.stops.update_stops(
        StopClustersSnapshot(
            coin="BTC", ts_exchange_ms=now, source="liqflow", mid=64_000.0,
            clusters=[StopCluster(px=63_995.0, sell_notional=6_000_000.0, n_orders=100)],
        )
    )

    se = SignalEngine(cfg)
    fresh = se.on_tick(eng.coins)
    cont = [s for s in fresh if s.kind.value == "continuation"]
    assert cont and cont[0].direction is Side.SELL
    sig = cont[0].to_dict()
    assert len(sig["conditions"]) >= 3


def test_flow_quality_cohorts():
    cfg = _cfg()
    eng = StateEngine(cfg, persist_path="/tmp/hlff-test")
    now = int(time.time() * 1000)
    user = "0x" + "a" * 40
    # цена растёт +1% равномерно; филлы-покупки идут по рынку на каждом шаге
    t0 = now - 40_000
    for i in range(440):
        ts = t0 + i * 1_000
        px = 3_000.0 + 30.0 * (i / 440)
        eng.on_event(MarkPrice(coin="ETH", ts_exchange_ms=ts, px=px))
        if 0 < i <= 40:
            eng.on_event(Fill(coin="ETH", ts_exchange_ms=ts, user=user, side=Side.BUY, px=px, sz=1.0))
    cs = eng.coin("ETH")
    cs.flow.on_tick(now + 400_000)
    assert cs.flow.cohort_of(user).value == "informed"
    assert cs.flow.markout_bps(user, 10) is not None


def test_snapshot_roundtrip():
    cfg = _cfg()
    eng = StateEngine(cfg, persist_path="/tmp/hlff-test-snap")
    now = int(time.time() * 1000)
    eng.on_event(
        LiquidationFill(
            coin="BTC", ts_exchange_ms=now, liquidated_user="0xd", side=Side.SELL,
            px=64_000.0, sz=2.0, kind=LiquidationKind.MARKET, realized_pnl=-5.0,
        )
    )
    eng.save_snapshot()
    eng2 = StateEngine(cfg, persist_path="/tmp/hlff-test-snap")
    assert eng2.load_snapshot() >= 1
    donors = eng2.coins["BTC"].liquidations.top_donors(1)
    assert donors and donors[0]["notional_usd"] == 128_000.0
