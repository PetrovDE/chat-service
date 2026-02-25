import base64
import logging
from datetime import datetime, timedelta
from typing import Optional

import httpx

from app.core.config import settings
from app.observability.metrics import inc_counter, observe_ms
from app.utils.retry import async_retry

logger = logging.getLogger(__name__)


class AIHubAuthManager:
    def __init__(self):
        self._token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self.keycloak_host = settings.AIHUB_KEYCLOAK_HOST
        self.username = settings.AIHUB_USERNAME
        self.password = settings.AIHUB_PASSWORD
        self.client_id = settings.AIHUB_CLIENT_ID
        self.client_secret = settings.AIHUB_CLIENT_SECRET
        self.verify_ssl = settings.AIHUB_VERIFY_SSL
        logger.info("AIHubAuthManager configured: host=%s verify_ssl=%s", self.keycloak_host, self.verify_ssl)

    async def get_token(self) -> Optional[str]:
        if self._token and self._token_expires_at:
            if datetime.now() < self._token_expires_at - timedelta(seconds=60):
                return self._token
        return await self._request_token()

    async def _request_token(self) -> Optional[str]:
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
        headers = {"Authorization": f"Basic {encoded_credentials}", "Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "password", "username": self.username, "password": self.password}

        started = __import__("time").perf_counter()
        try:
            async def _call() -> httpx.Response:
                async with httpx.AsyncClient(verify=self.verify_ssl, timeout=30.0) as client:
                    response = await client.post(self.keycloak_host, data=data, headers=headers)
                    response.raise_for_status()
                    return response

            response = await async_retry(_call, retries=2)
            token_info = response.json()
            token = token_info.get("access_token")
            if not token:
                logger.error("AI HUB auth response does not contain access_token")
                inc_counter("llm_provider_error_total", provider="aihub", operation="auth")
                return None

            expires_in = int(token_info.get("expires_in", 300))
            self._token = token
            self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            observe_ms("llm_provider_duration_ms", (__import__("time").perf_counter() - started) * 1000.0, provider="aihub", operation="auth")
            inc_counter("llm_provider_success_total", provider="aihub", operation="auth")
            return self._token
        except Exception as e:
            logger.error("AI HUB auth failed: %s", e, exc_info=True)
            inc_counter("llm_provider_error_total", provider="aihub", operation="auth")
            return None

    def clear_token(self):
        self._token = None
        self._token_expires_at = None

    def is_token_valid(self) -> bool:
        if not self._token or not self._token_expires_at:
            return False
        return datetime.now() < self._token_expires_at - timedelta(seconds=60)

