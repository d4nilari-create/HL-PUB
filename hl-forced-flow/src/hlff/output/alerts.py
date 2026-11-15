"""Алерты: Telegram / Discord через env. Не настроены — молча отключены."""

from __future__ import annotations

import aiohttp

from hlff.config import Secrets
from hlff.utils import get_logger

log = get_logger("output.alerts")


class AlertManager:
    def __init__(self, secrets: Secrets) -> None:
        self.s = secrets

    @property
    def enabled(self) -> bool:
        return bool(
            (self.s.TELEGRAM_BOT_TOKEN and self.s.TELEGRAM_CHAT_ID) or self.s.DISCORD_WEBHOOK
        )

    async def send(self, text: str) -> None:
        if not self.enabled:
            return
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            if self.s.TELEGRAM_BOT_TOKEN and self.s.TELEGRAM_CHAT_ID:
                url = f"https://api.telegram.org/bot{self.s.TELEGRAM_BOT_TOKEN}/sendMessage"
                try:
                    async with sess.post(
                        url,
                        json={"chat_id": self.s.TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                    ) as resp:
                        if resp.status != 200:
                            log.warning("telegram_alert_failed", status=resp.status)
                except Exception:
                    log.exception("telegram_alert_error")
            if self.s.DISCORD_WEBHOOK:
                try:
                    async with sess.post(self.s.DISCORD_WEBHOOK, json={"content": text[:1900]}) as resp:
                        if resp.status not in (200, 204):
                            log.warning("discord_alert_failed", status=resp.status)
                except Exception:
                    log.exception("discord_alert_error")
