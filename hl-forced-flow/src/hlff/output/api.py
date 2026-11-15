"""FastAPI: JSON API + статический dashboard.

Запускается из CLI-процесса через uvicorn (in-process, тот же loop).
Все данные — живое состояние engine-а, без своей логики.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from hlff.config import Config
from hlff.features.pressure import compute_pressure_surface
from hlff.features.regime import detect_regime
from hlff.features.signals import SignalEngine
from hlff.ingestion.base import EventBus
from hlff.output.dashboard import DASHBOARD_HTML
from hlff.state.engine import StateEngine


def create_app(
    cfg: Config, bus: EventBus, engine: StateEngine, signals: SignalEngine
) -> FastAPI:
    app = FastAPI(title="hl-forced-flow", docs_url="/api/docs", openapi_url="/api/openapi.json")

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> str:
        return DASHBOARD_HTML

    @app.get("/api/status")
    async def status() -> dict:
        st = engine.status()
        st["sources"] = bus.sources_health()
        st["degraded_sources"] = bus.degraded_sources()
        st["dropped_events"] = bus.dropped_events
        return st

    @app.get("/api/signals")
    async def signals_endpoint() -> dict:
        return {"active": signals.active_signals(), "history": signals.signal_history()}

    @app.get("/api/pressure")
    async def pressure(coin: str | None = None) -> dict:
        coins = [coin.upper()] if coin else engine.tracked_coins()
        out = []
        for c in coins:
            cs = engine.coins.get(c)
            if cs is None:
                continue
            surf = compute_pressure_surface(cs, cfg.features)
            regime = detect_regime(cs, cfg.features)
            out.append(
                {
                    "coin": c,
                    "mid": surf.get("mid"),
                    "bins": [
                        {"bin": b, **v} for b, v in surf["bins"].items()
                    ],
                    "liq_flow_window_usd": surf.get("liq_flow_window_usd"),
                    "stops_stale": surf.get("stops_stale"),
                    "regime": regime["regime"],
                }
            )
        return {"coins": out}

    @app.get("/api/clusters/{coin}")
    async def clusters(coin: str) -> dict:
        cs = engine.coins.get(coin.upper())
        if cs is None:
            return {"coin": coin.upper(), "stops": None, "liq_levels": None}
        stops = cs.stops.active_stops()
        levels = cs.stops.active_liq_levels()
        return {
            "coin": coin.upper(),
            "mid": cs.stops.mid,
            "stale": cs.stops.stale(),
            "stops": [c.model_dump() for c in stops.clusters] if stops else None,
            "liq_levels": [l.model_dump() for l in levels.levels] if levels else None,
        }

    @app.get("/api/donors")
    async def donors(coin: str | None = None, limit: int = 10) -> dict:
        coins = [coin.upper()] if coin else engine.tracked_coins()
        return {
            "coins": [
                {"coin": c, "donors": engine.coins[c].liquidations.top_donors(limit)}
                for c in coins
                if c in engine.coins
            ]
        }

    @app.get("/api/flow/{coin}")
    async def flow(coin: str) -> dict:
        cs = engine.coins.get(coin.upper())
        if cs is None:
            return {"coin": coin.upper()}
        return {
            "coin": coin.upper(),
            "taker_notional_per_sec": round(cs.flow.taker_notional_per_sec(), 2),
            "informed_flow_bps": round(cs.flow.informed_flow_bps(), 3),
            "n_fills": cs.flow.n_fills,
            "top_informed": cs.flow.top_informed(5),
            "markout_donors": cs.flow.top_donors(5),
        }

    return app
