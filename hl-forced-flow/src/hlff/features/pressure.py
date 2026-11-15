"""Pressure surface: бины 5/10/25/50 bps от mid.

Для каждого бина считаем:
  * forced_sell_usd — стопы на продажу ниже mid + long-ликвидации в бине
    (подтопливающий принудительный sell-поток, если цена пройдёт вниз);
  * forced_buy_usd  — стопы на покупку выше mid + short-ликвидации;
  * book_bid/book_ask — покоящаяся ликвидность из L2Book;
  * ratio — доминантный форсед-номинал бина / медиана по монете.

Единственная точка расчёта pressure — здесь; API и сигналы берут готовое.
"""

from __future__ import annotations

import time

from hlff.config import FeaturesConfig
from hlff.state.engine import CoinState


def compute_pressure_surface(
    cs: CoinState, cfg: FeaturesConfig, now_ms: int | None = None
) -> dict:
    """Полный pressure surface по монете. Все суммы в USD."""
    now_ms = now_ms or int(time.time() * 1000)
    mid = cs.l2.mid or (cs.stops.mid if cs.stops else None)

    def _bin_key(b: float) -> str:
        return str(int(b)) if float(b).is_integer() else str(b)

    bins = sorted(cfg.depth_bins_bps)
    out: dict = {"mid": mid, "bins": {}, "window_sec": cfg.regime.window_sec}
    if mid is None or mid <= 0:
        return out

    # стопы + ликвидации по бинам (USD)
    stop_bins = cs.stops.clusters_in_bins(bins)
    liq_snap = cs.stops.active_liq_levels()
    liq_sell = {b: 0.0 for b in bins}   # long-ликвидации ниже mid (forced sell)
    liq_buy = {b: 0.0 for b in bins}    # short-ликвидации выше mid (forced buy)
    if liq_snap:
        for lv in liq_snap.levels:
            if lv.long_notional > 0 and lv.px < mid:
                dist = (mid - lv.px) / mid * 10_000
                for b in bins:
                    if dist <= b:
                        liq_sell[b] += lv.long_notional
                        break
            if lv.short_notional > 0 and lv.px > mid:
                dist = (lv.px - mid) / mid * 10_000
                for b in bins:
                    if dist <= b:
                        liq_buy[b] += lv.short_notional
                        break

    # свежие ликвидации из журнала (скорость за окно) как «живой» forced-поток
    win = cfg.regime.window_sec
    liq_sell_flow = cs.liquidations.window_notional(now_ms, win, side=None)
    sell_speed = liq_sell_flow  # номинал за окно; ниже нормируем на окно

    for b in bins:
        book = cs.l2.depth_notional_within_bps(b)
        forced_sell = stop_bins[b]["sell"] + liq_sell[b]
        forced_buy = stop_bins[b]["buy"] + liq_buy[b]
        out["bins"][_bin_key(b)] = {
            "book_bid_usd": round(book["bid"], 2),
            "book_ask_usd": round(book["ask"], 2),
            "forced_sell_usd": round(forced_sell, 2),
            "forced_buy_usd": round(forced_buy, 2),
            "sell_pressure_ratio": 0.0,
            "buy_pressure_ratio": 0.0,
        }

    # ratio доминантного бина к медиане форсед-номиналов монеты
    totals = [
        max(v["forced_sell_usd"], v["forced_buy_usd"]) for v in out["bins"].values()
    ]
    med = sorted(totals)[len(totals) // 2] if totals else 0.0
    for b_str, v in out["bins"].items():
        for key, val in (("sell_pressure_ratio", v["forced_sell_usd"]),
                         ("buy_pressure_ratio", v["forced_buy_usd"])):
            v[key] = round(val / med, 2) if med > 0 else 0.0

    out["liq_flow_window_usd"] = round(sell_speed, 2)
    out["stops_stale"] = cs.stops.stale()
    return out
