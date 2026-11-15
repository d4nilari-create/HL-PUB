"""Загрузка и валидация конфигурации.

Источники (по убыванию приоритета):
  1. env-переменные с префиксом HLFF_ (разделитель «__»),
     например HLFF_FEATURES__PRESSURE__EWMA_HALFLIFE_SEC=20
  2. пользовательский yaml, путь через HLFF_CONFIG или --config
  3. config/default.yaml, поставляемый с проектом

API-ключи берутся из окружения / .env (см. .env.example). Ключи уже вшиты
в .env и в default-значения — пользователю ничего вставлять не нужно.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_CONFIG_PATH = _PROJECT_ROOT / "config" / "default.yaml"


# ---------------------------------------------------------------------------
# API-ключи и алерты (env / .env)
# ---------------------------------------------------------------------------


def _find_dotenv() -> Path | None:
    for cand in (Path.cwd() / ".env", _PROJECT_ROOT / ".env"):
        if cand.exists():
            return cand
    return None


class Secrets(BaseSettings):
    """Секреты и алерты. Ключи по умолчанию — уже выданные бесплатные ключи,
    поэтому система работает даже без .env."""

    model_config = SettingsConfigDict(
        env_file=_find_dotenv() or "", env_file_encoding="utf-8", extra="ignore"
    )

    IRONFLOW_API_KEY: str = "if_fc92c6ba818933378902b035a5c7fcc7afbd198e004c969d84cd25697d61b101"
    OXARCHIVE_API_KEY: str = "0xa_607a538a4ef3c145d2b672fac32e0edd6cae8f18496cf9ed817780e7d3406f27"
    LIQFLOW_API_KEY: str = "lqf_B9ptL05FeO13FOQSYADJpOXrEbPO0iKgAxyI2qpdQps"

    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""
    DISCORD_WEBHOOK: str = ""


# ---------------------------------------------------------------------------
# Конфигурационные модели
# ---------------------------------------------------------------------------


class SystemConfig(BaseModel):
    timezone: str = "UTC"
    snapshot_interval_sec: float = 60.0
    state_persist_path: str = "./data/state"


class MarketsConfig(BaseModel):
    coins: list[str] = Field(default_factory=list)
    book_levels: int = 20


class IronflowConfig(BaseModel):
    enabled: bool = True
    base_url: str = "https://api.ironflow.sh"
    stream_path: str = "/v1/liquidations/stream"
    history_path: str = "/v1/liquidations"
    reconnect_min_sec: float = 1.0
    reconnect_max_sec: float = 60.0


class LiqflowConfig(BaseModel):
    enabled: bool = True
    base_url: str = "https://node.liqflow.app"
    levels_refresh_sec: float = 300.0
    stop_levels_limit: int = 500


class OxarchiveConfig(BaseModel):
    enabled: bool = True
    base_url: str = "https://api.0xarchive.io"
    trades_poll_sec: float = 5.0
    levels_poll_sec: float = 300.0
    trades_limit: int = 200


class HlConfig(BaseModel):
    enabled: bool = True
    info_url: str = "https://api.hyperliquid.xyz/info"
    ws_url: str = "wss://api.hyperliquid.xyz/ws"
    mids_poll_sec: float = 5.0
    reconnect_min_sec: float = 1.0
    reconnect_max_sec: float = 60.0


class BinanceConfig(BaseModel):
    enabled: bool = False
    ws_url: str = "wss://fstream.binance.com/ws"
    symbols: list[str] = Field(default_factory=list)


class SideChannelsConfig(BaseModel):
    binance: BinanceConfig = Field(default_factory=BinanceConfig)


class IngestionConfig(BaseModel):
    ironflow: IronflowConfig = Field(default_factory=IronflowConfig)
    liqflow: LiqflowConfig = Field(default_factory=LiqflowConfig)
    oxarchive: OxarchiveConfig = Field(default_factory=OxarchiveConfig)
    hl: HlConfig = Field(default_factory=HlConfig)
    side_channels: SideChannelsConfig = Field(default_factory=SideChannelsConfig)


class MarkoutFeatureConfig(BaseModel):
    price_buffer_sec: int = 1800
    min_fills_for_cohort: int = 30
    decay_half_life_sec: float = 3600.0


class CohortThresholds(BaseModel):
    donor_bps_threshold: float = -2.0
    informed_bps_threshold: float = 2.0


class PressureConfig(BaseModel):
    ewma_halflife_sec: float = 15.0
    fatigue_halflife_sec: float = 120.0


class RegimeConfig(BaseModel):
    window_sec: int = 180
    cascade_liq_notional_usd: float = 250000.0
    quiet_max_taker_notional_usd: float = 50000.0


class FeaturesConfig(BaseModel):
    depth_bins_bps: list[float] = Field(default_factory=lambda: [5.0, 10.0, 25.0, 50.0])
    markout_horizons_sec: list[int] = Field(default_factory=lambda: [10, 60, 300])
    markout: MarkoutFeatureConfig = Field(default_factory=MarkoutFeatureConfig)
    cohorts: CohortThresholds = Field(default_factory=CohortThresholds)
    pressure: PressureConfig = Field(default_factory=PressureConfig)
    regime: RegimeConfig = Field(default_factory=RegimeConfig)


class ContinuationConfig(BaseModel):
    min_pressure_ratio: float = 2.5
    min_cluster_notional_usd: float = 100000.0
    max_informed_flow_bps: float = -1.0


class FadeConfig(BaseModel):
    cluster_absorption_frac: float = 0.8
    min_donor_notional_usd: float = 50000.0
    stabilization_sec: float = 60.0


class SignalsConfig(BaseModel):
    ttl_sec: float = 300.0
    max_active_per_coin: int = 3
    continuation: ContinuationConfig = Field(default_factory=ContinuationConfig)
    fade: FadeConfig = Field(default_factory=FadeConfig)


class DashboardConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8400


class StorageConfig(BaseModel):
    parquet_dir: str = "./data/parquet"


class Config(BaseModel):
    system: SystemConfig = Field(default_factory=SystemConfig)
    markets: MarketsConfig = Field(default_factory=MarketsConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    features: FeaturesConfig = Field(default_factory=FeaturesConfig)
    signals: SignalsConfig = Field(default_factory=SignalsConfig)
    dashboard: DashboardConfig = Field(default_factory=DashboardConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)


# ---------------------------------------------------------------------------
# Загрузка: yaml + env-переопределения
# ---------------------------------------------------------------------------


def _deep_update(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = v
    return base


def _coerce(raw: str) -> Any:
    low = raw.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    if "," in raw:
        return [p.strip() for p in raw.split(",") if p.strip()]
    return raw


def _apply_env_overrides(data: dict[str, Any]) -> dict[str, Any]:
    for key, val in os.environ.items():
        if not key.startswith("HLFF_"):
            continue
        path = key[len("HLFF_"):].lower().split("__")
        node = data
        for part in path[:-1]:
            node = node.setdefault(part, {})
        node[path[-1]] = _coerce(val)
    return data


def _load_yaml_dict(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache(maxsize=4)
def get_config(config_path: str | None = None) -> Config:
    data: dict[str, Any] = _load_yaml_dict(_DEFAULT_CONFIG_PATH)

    custom = config_path or os.environ.get("HLFF_CONFIG")
    if custom and Path(custom).exists():
        _deep_update(data, _load_yaml_dict(Path(custom)))

    _apply_env_overrides(data)
    return Config.model_validate(data)


def get_secrets() -> Secrets:
    return Secrets()
