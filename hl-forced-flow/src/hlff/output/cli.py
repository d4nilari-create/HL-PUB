"""CLI: ``hlff live`` — единая точка входа.

Собирает: config → secrets → bus → sources → engine → signals → API.
Все источники — независимые asyncio-задачи с graceful degradation.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Annotated

import typer
import uvicorn
from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table

from hlff import __version__
from hlff.config import get_config, get_secrets
from hlff.features.pressure import compute_pressure_surface
from hlff.features.regime import detect_regime
from hlff.features.signals import SignalEngine
from hlff.ingestion.base import EventBus, cancel_all
from hlff.ingestion.hl_public import HlPublicSource
from hlff.ingestion.ironflow import IronflowSource
from hlff.ingestion.liqflow import LiqflowSource
from hlff.ingestion.oxarchive import OxarchiveSource
from hlff.ingestion.side_channels import BinanceForceOrderSource
from hlff.output.alerts import AlertManager
from hlff.output.api import create_app
from hlff.state.engine import StateEngine
from hlff.utils import get_logger, now_ms, setup_logging

log = get_logger("output.cli")
console = Console()

app = typer.Typer(
    add_completion=False,
    help="hl-forced-flow — бесплатная forced-flow аналитика Hyperliquid (data-only).",
)


@app.command()
def live(
    config: Annotated[Path | None, typer.Option("--config", help="Путь к yaml-конфигу")] = None,
    log_level: Annotated[str, typer.Option("--log-level", help="DEBUG/INFO/WARNING")] = "INFO",
    no_api: Annotated[bool, typer.Option("--no-api", help="Не поднимать дашборд")] = False,
) -> None:
    """Запустить live-сбор данных + дашборд http://127.0.0.1:8400."""
    setup_logging(log_level)
    cfg = get_config(str(config) if config else None)
    secrets = get_secrets()

    coins = [c.upper() for c in cfg.markets.coins]
    if not coins:
        console.print("[red]markets.coins пуст. Укажите монеты в config/default.yaml или HLFF_MARKETS_COINS[/red]")
        raise typer.Exit(code=2)

    bus = EventBus()
    engine = StateEngine(cfg)
    restored = engine.load_snapshot()
    if restored:
        log.info("state_restored", coins=restored)
    signals = SignalEngine(cfg)
    alerts = AlertManager(secrets)

    sources = []
    ic = cfg.ingestion
    if ic.ironflow.enabled:
        sources.append(IronflowSource(bus, ic.ironflow, secrets.IRONFLOW_API_KEY, coins))
    if ic.liqflow.enabled:
        sources.append(LiqflowSource(bus, ic.liqflow, secrets.LIQFLOW_API_KEY, coins))
    if ic.oxarchive.enabled:
        sources.append(OxarchiveSource(bus, ic.oxarchive, secrets.OXARCHIVE_API_KEY, coins))
    if ic.hl.enabled:
        sources.append(HlPublicSource(bus, ic.hl, coins, cfg.markets.book_levels))
    if ic.side_channels.binance.enabled:
        sources.append(BinanceForceOrderSource(bus, ic.side_channels.binance, coins))

    if not sources:
        console.print("[red]Все источники выключены в конфиге[/red]")
        raise typer.Exit(code=2)

    engine_sub = engine.on_event
    bus.subscribe(engine_sub)

    async def _run() -> None:
        tasks: list[asyncio.Task] = [asyncio.create_task(s.run()) for s in sources]

        if not no_api:
            api = create_app(cfg, bus, engine, signals)
            server = uvicorn.Server(
                uvicorn.Config(
                    api, host=cfg.dashboard.host, port=cfg.dashboard.port,
                    log_level=log_level.lower(), access_log=False,
                )
            )
            tasks.append(asyncio.create_task(server.serve()))
            console.print(
                f"[bold green]Dashboard:[/bold green] http://{cfg.dashboard.host}:{cfg.dashboard.port}"
            )

        last_alerted: dict[str, float] = {}

        try:
            with Live(_render(engine, signals, bus), refresh_per_second=1, console=console) as rich_live:
                while True:
                    now = now_ms()
                    engine.on_tick(now)
                    for sig in signals.on_tick(engine.coins):
                        d = sig.to_dict()
                        log.info(
                            "signal",
                            kind=d["kind"], coin=d["coin"], direction=d["direction"],
                            conditions=d["conditions"],
                        )
                        key = f"{d['id']}"
                        if alerts.enabled and time.monotonic() - last_alerted.get(key, 0) > 60:
                            last_alerted[key] = time.monotonic()
                            await alerts.send(
                                f"<b>{d['kind'].upper()}</b> {d['coin']} {d['direction']}\n"
                                + "\n".join(f"· {c}" for c in d["conditions"])
                            )
                    rich_live.update(_render(engine, signals, bus))
                    await asyncio.sleep(1.0)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            engine.save_snapshot()
            await cancel_all(tasks)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass

    console.print(f"[yellow]Остановлено. Snapshot сохранён в {cfg.system.state_persist_path}[/yellow]")


@app.command()
def version() -> None:
    """Версия пакета."""
    console.print(f"hl-forced-flow {__version__}")


def _render(engine: StateEngine, signals: SignalEngine, bus: EventBus) -> Table:
    """Rich-таблица для Live-вывода."""
    table = Table(box=box.SIMPLE, title="hl-forced-flow live (data-only)")
    table.add_column("coin", style="bold")
    table.add_column("mid", justify="right")
    table.add_column("regime")
    table.add_column("liq 3m USD", justify="right")
    table.add_column("pressure s/b", justify="right")
    table.add_column("сигналы")
    table.add_column("donor #1", overflow="fold")

    cfg = get_config()
    act = signals.active_signals()
    now = now_ms()
    for coin in engine.tracked_coins():
        cs = engine.coins[coin]
        mid = cs.l2.mid
        regime = detect_regime(cs, cfg.features, now)
        surf = compute_pressure_surface(cs, cfg.features, now)
        ratios = [
            (v["sell_pressure_ratio"], v["buy_pressure_ratio"])
            for v in surf["bins"].values()
        ]
        pr = max(max(r) for r in ratios) if ratios else 0.0
        liq3m = cs.liquidations.window_notional(now, 180)
        coin_sigs = [s for s in act if s["coin"] == coin]
        sig_txt = "; ".join(f"{s['kind']} {s['direction']}" for s in coin_sigs) or "—"
        donors = cs.liquidations.top_donors(1)
        donor_txt = (
            f"{donors[0]['user'][:6]}… {donors[0]['notional_usd']:,.0f}$" if donors else "—"
        )
        table.add_row(
            coin,
            f"{mid:,.4g}" if mid else "—",
            regime["regime"],
            f"{liq3m:,.0f}",
            f"{pr:.2f}" if pr else "—",
            sig_txt,
            donor_txt,
        )

    health = bus.sources_health()
    foot = "  ".join(
        f"[{'green' if ok else 'red'}]{name}[/{'green' if ok else 'red'}]" for name, ok in health.items()
    )
    table.caption = f"sources: {foot or '…'}"
    return table


if __name__ == "__main__":
    app()
