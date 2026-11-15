# hl-forced-flow

**Бесплатная** аналитика вынужденного потока (forced flow) Hyperliquid для ручной торговли.
Data-only: система только читает публичные данные — никаких ордеров, приватных ключей и подписей.

## Быстрый старт

```bash
# 1. скачал и распаковал архив
cd hl-forced-flow

# 2. поставил зависимости (одна команда)
pip install -e .

# 3. запустил — всё уже настроено, ключи вшиты
hlff live

# 4. открыл дашборд
# http://127.0.0.1:8400
```

Больше ничего настраивать не нужно: бесплатные API-ключи уже лежат в `.env`
(и продублированы как default-значения в коде — система работает даже без `.env`).

## Что работает из коробки

| Компонент | Источник | Что даёт |
|---|---|---|
| Live-ликвидации | **Ironflow** (`/v1/liquidations/stream`, SSE) | поток ликвидаций в реальном времени + historical catch-up |
| Liquidation levels + стоп-кластеры | **LiqFlow** (нода) | реальные цены ликвидации всех кошельков и неисполненные стопы |
| Trades + уровни | **0xArchive Free** | taker-филлы с адресами (markout-когорты), карты ликвидаций, trigger levels |
| Стакан + mids | **Публичный HL** (`wss://api.hyperliquid.xyz/ws`, `/info`) | L2 book, mid-price, анонимный тейкер-поток |
| Внешний контекст (опц.) | Binance forceOrder | бесплатный, по умолчанию выключен |

Все источники — независимые asyncio-задачи: падение любого не останавливает
остальные (graceful degradation, health виден в `/api/status` и в CLI).

## Что считает система

- **LiquidationBook** — окно исполненных ликвидаций, номинал/скорость по
  сторонам, MARKET vs вероятный BACKSTOP_HLP (эвристика: `bankrupt`-флаг
  Ironflow или номинал ≥ $250k), топ-доноры по адресам.
- **StopsBook** — стоп-кластеры и уровни ликвидаций с приоритетом LiqFlow,
  разложение по бинам от mid, обработка «протухания» срезов (staleness),
  fatigue после прохода цены через кластер.
- **FlowQualityBook** — online markout-когорты (donor / informed / mm) по
  адресам из 0xArchive trades с EWMA-decay; агрегированный taker-поток.
- **L2Book** — стакан и mid-price.
- **Pressure surface** — бины 5/10/25/50 bps: forced sell/buy номинал
  (стопы + ликвидации) против покоящейся ликвидности, ratio к медиане.
- **Режим** — cascade / quiet / informed_trend / mixed.
- **Сигналы Continuation / Fade** — с полным списком conditions: каждый
  сигнал показывает, какие факты его породили.

## CLI

```bash
hlff live                 # сбор + дашборд на :8400
hlff live --no-api        # только сбор, без дашборда
hlff live --config my.yaml
hlff live --log-level DEBUG
hlff version
```

Переопределение конфига через env: `HLFF_MARKETS_COINS=BTC,ETH`,
`HLFF_DASHBOARD__PORT=8500` и т.п.

## API дашборда

| Endpoint | Описание |
|---|---|
| `GET /` | dashboard |
| `GET /api/status` | uptime, события, health источников |
| `GET /api/signals` | активные и история сигналов |
| `GET /api/pressure?coin=BTC` | pressure surface + режим |
| `GET /api/clusters/{coin}` | стоп-кластеры и уровни ликвидаций |
| `GET /api/donors?coin=BTC` | топ-доноры ликвидаций |
| `GET /api/flow/{coin}` | качество потока, markout-когорты |

## Алерты (опционально)

```bash
export TELEGRAM_BOT_TOKEN=... TELEGRAM_CHAT_ID=...
# или
export DISCORD_WEBHOOK=https://discord.com/api/webhooks/...
```

Без них — просто молча выключены.

## Конфигурация

Все пороги — в `config/default.yaml` (бины, окна каскада, пороги сигналов,
half-life decay, интервалы поллинга). Снапшоты состояния пишутся в
`./data/state/state.json` и поднимаются при рестарте.

```bash
# скачать историю trades с 0xArchive (30 дней бесплатно)
python scripts/fetch_history.py --coin BTC --hours 24
```

## Ограничения бесплатной версии

- **Нет пер-ордерного L4-стрима**: стоп-кластеры — агрегаты LiqFlow/0xArchive
  (refresh ~5 мин), а не поток placement/cancel каждого ордера. Pressure
  ближних бинов опирается на эти срезы + L2 book.
- **MARKET vs BACKSTOP_HLP** — эвристика, не прямой флаг ноды.
- **Markout-когорты** строятся по crossed-филлам 0xArchive; у анонимного
  HL WS-потока адресов нет — он идёт только в агрегаты.
- Лимиты бесплатных ключей: LiqFlow ~25k вызовов/мес (хватает с запасом
  для 3-10 монет), 0xArchive free-tier настроен под лёгкий поллинг.

## Архитектура

```
ingestion (ironflow | liqflow | oxarchive | hl_public | binance?)
        │  нормализованные события (pydantic)
        ▼
     EventBus ──► StateEngine ──► features (pressure, regime, signals)
        │                                    │
        └── storage sink (opt)               ▼
                                     FastAPI :8400 + dashboard + alerts
```

Каждый источник — самостоятельная задача с reconnect/backoff. Расширяемость:
новый источник = новый EventSource, публикующий те же события.

## Безопасность

Нет приватных ключей, нет подписи, нет отправки ордеров — только публичные
read-only данные. Ключи в `.env` — это read-only API-ключи сторонних
сервисов данных, не ключи кошелька.
