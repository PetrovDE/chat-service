"""
AI HUB Authentication Manager
Модуль для аутентификации через Keycloak
"""
import logging
import base64
from typing import Optional
from datetime import datetime, timedelta
import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


class AIHubAuthManager:
    """Менеджер аутентификации для AI HUB через Keycloak"""

    def __init__(self):
        self._token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self.keycloak_host = settings.AIHUB_KEYCLOAK_HOST
        self.username = settings.AIHUB_USERNAME
        self.password = settings.AIHUB_PASSWORD
        self.client_id = settings.AIHUB_CLIENT_ID
        self.client_secret = settings.AIHUB_CLIENT_SECRET
        self.verify_ssl = settings.AIHUB_VERIFY_SSL

        # Определяем режим аутентификации
        self.use_client_credentials = bool(
            self.client_id and
            self.client_secret and
            not (self.username and self.password)
        )

        self._log_config()

    def _log_config(self):
        """Логирование конфигурации (без секретов!)"""
        logger.info("=" * 60)
        logger.info("🔑 AI HUB Authentication Configuration")
        logger.info("=" * 60)
        logger.info(f"Keycloak Host: {self.keycloak_host}")
        logger.info(f"Auth Mode: {'Client Credentials (Basic Auth)' if self.use_client_credentials else 'Password Grant'}")
        logger.info(f"Verify SSL: {self.verify_ssl}")

        if self.use_client_credentials:
            logger.info(f"Client ID: {self.client_id}")
            logger.info(f"Client Secret: {'*' * min(8, len(self.client_secret)) if self.client_secret else 'NOT SET'}")
        else:
            logger.info(f"Username: {self.username}")
            logger.info(f"Password: {'*' * min(8, len(self.password)) if self.password else 'NOT SET'}")
            logger.info(f"Client ID: {self.client_id}")
            logger.info(f"Client Secret: {'*' * min(8, len(self.client_secret)) if self.client_secret else 'NOT SET'}")
        logger.info("=" * 60)

    async def get_token(self) -> Optional[str]:
        """
        Получить JWT токен через Keycloak
        Поддерживает два режима:
        1. Client Credentials (Basic Auth) - если нет username/password
        2. Password Grant - если есть учетные данные
        """
        # Проверяем актуальность кешированного токена (с запасом 60 секунд)
        if self._token and self._token_expires_at:
            if datetime.now() < self._token_expires_at - timedelta(seconds=60):
                logger.debug("🔑 Using cached token")
                return self._token

        # Получаем новый токен
        if self.use_client_credentials:
            return await self._get_token_client_credentials()
        else:
            return await self._get_token_password_grant()

    async def _get_token_client_credentials(self) -> Optional[str]:
        """Получение токена через Client Credentials flow с Basic Auth"""
        logger.info("🔑 Requesting new token (Client Credentials with Basic Auth)...")

        # Кодируем client credentials для Basic Auth
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            "grant_type": "client_credentials"
        }

        return await self._request_token(headers, data, "Client Credentials")

    async def _get_token_password_grant(self) -> Optional[str]:
        """Получение токена через Password Grant flow"""
        logger.info("🔑 Requesting new token (Password Grant)...")

        data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        return await self._request_token(headers, data, "Password Grant")

    async def _request_token(
            self,
            headers: dict,
            data: dict,
            auth_type: str
    ) -> Optional[str]:
        """Общий метод для запроса токена"""
        try:
            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                logger.debug(f"🔗 POST {self.keycloak_host}")
                logger.debug(f"📤 Headers: {list(headers.keys())}")

                response = await client.post(
                    self.keycloak_host,
                    data=data,
                    headers=headers,
                    timeout=30.0
                )

                logger.info(f"📥 Keycloak response: {response.status_code}")

                if response.status_code == 200:
                    return self._handle_success_response(response, auth_type)
                else:
                    self._handle_error_response(response)
                    return None

        except httpx.TimeoutException:
            logger.error("❌ Keycloak authentication timeout (30s)")
            return None
        except httpx.ConnectError as e:
            logger.error(f"❌ Connection error: {e}")
            logger.error(f"❌ Check that Keycloak is accessible at: {self.keycloak_host}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error: {type(e).__name__}: {e}", exc_info=True)
            return None

    def _handle_success_response(self, response, auth_type: str) -> Optional[str]:
        """Обработка успешного ответа"""
        try:
            token_info = response.json()
            self._token = token_info.get("access_token")

            if not self._token:
                logger.error("❌ Response missing 'access_token' field!")
                logger.error(f"Available keys: {list(token_info.keys())}")
                return None

            # Вычисляем время истечения токена
            expires_in = token_info.get("expires_in", 300)
            self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)

            # Показываем preview токена
            token_preview = self._get_token_preview(self._token)
            logger.info(f"✅ Token obtained ({auth_type})")
            logger.info(f"✅ Token preview: {token_preview}")
            logger.info(f"✅ Expires in: {expires_in}s ({expires_in // 60} min)")

            return self._token

        except Exception as e:
            logger.error(f"❌ Error parsing success response: {e}")
            return None

    def _handle_error_response(self, response):
        """Обработка ошибочного ответа"""
        logger.error("=" * 60)
        logger.error(f"❌ Authentication failed: {response.status_code}")
        logger.error("=" * 60)
        logger.error(f"Response headers: {dict(response.headers)}")

        try:
            error_info = response.json()
            logger.error("Error details:")
            for key, value in error_info.items():
                logger.error(f"  {key}: {value}")
        except Exception:
            logger.error(f"Raw response: {response.text[:500]}")

        logger.error("=" * 60)

    @staticmethod
    def _get_token_preview(token: str) -> str:
        """Получить preview токена для логов"""
        if len(token) > 40:
            return f"{token[:20]}...{token[-20:]}"
        return "[short token]"

    def clear_token(self):
        """Очистить кешированный токен"""
        self._token = None
        self._token_expires_at = None
        logger.info("🗑️ Token cache cleared")

    def is_token_valid(self) -> bool:
        """Проверить, валиден ли текущий токен"""
        if not self._token or not self._token_expires_at:
            return False
        return datetime.now() < self._token_expires_at - timedelta(seconds=60)
