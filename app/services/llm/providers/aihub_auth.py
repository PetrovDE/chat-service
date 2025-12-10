"""
AI HUB Authentication Manager
Модуль для аутентификации через Keycloak (Password Grant с Basic Auth)
"""
import logging
import base64
from typing import Optional
from datetime import datetime, timedelta
import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


class AIHubAuthManager:
    """Менеджер аутентификации для AI HUB через Keycloak (Password Grant с Basic Auth)"""

    def __init__(self):
        self._token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self.keycloak_host = settings.AIHUB_KEYCLOAK_HOST
        self.username = settings.AIHUB_USERNAME
        self.password = settings.AIHUB_PASSWORD
        self.client_id = settings.AIHUB_CLIENT_ID
        self.client_secret = settings.AIHUB_CLIENT_SECRET
        self.verify_ssl = settings.AIHUB_VERIFY_SSL

        self._log_config()

    def _log_config(self):
        """Логирование конфигурации (без секретов!)"""
        logger.info("=" * 60)
        logger.info("🔑 AI HUB Authentication Configuration")
        logger.info("=" * 60)
        logger.info(f"Keycloak Host: {self.keycloak_host}")
        logger.info(f"Auth Mode: Password Grant with Basic Auth")
        logger.info(f"Verify SSL: {self.verify_ssl}")
        logger.info(f"Username: {self.username}")
        logger.info(f"Password: {'*' * min(8, len(self.password)) if self.password else 'NOT SET'}")
        logger.info(f"Client ID: {self.client_id}")
        logger.info(f"Client Secret: {'*' * min(8, len(self.client_secret)) if self.client_secret else 'NOT SET'}")
        logger.info("=" * 60)

    async def get_token(self) -> Optional[str]:
        """Получить JWT токен через Keycloak"""
        import sys

        print("=" * 80, file=sys.stderr)
        print("🔑 get_token() CALLED!", file=sys.stderr)
        print(f"🔑 Current token: {self._token is not None}", file=sys.stderr)
        print(f"🔑 Token expires at: {self._token_expires_at}", file=sys.stderr)
        print("=" * 80, file=sys.stderr)

        logger.info("🔑 get_token() called")

        # Проверяем актуальность кешированного токена
        if self._token and self._token_expires_at:
            print(f"🔑 Checking cached token... expires_at={self._token_expires_at}", file=sys.stderr)
            logger.info(f"🔑 Checking cached token... expires_at={self._token_expires_at}")

            if datetime.now() < self._token_expires_at - timedelta(seconds=60):
                print("🔑 Using cached token", file=sys.stderr)
                logger.info("🔑 Using cached token")
                return self._token
            else:
                print("🔑 Cached token expired, requesting new one...", file=sys.stderr)
                logger.info("🔑 Cached token expired, requesting new one...")
        else:
            print("🔑 No cached token, requesting new one...", file=sys.stderr)
            logger.info("🔑 No cached token, requesting new one...")

        # Получаем новый токен
        print("🔑 Calling _request_token()...", file=sys.stderr)
        logger.info("🔑 Calling _request_token()...")

        token = await self._request_token()

        print(f"🔑 _request_token() returned: {token is not None}", file=sys.stderr)
        if token:
            print(f"🔑 Token preview: {token[:30]}...", file=sys.stderr)
            logger.info(f"🔑 _request_token() returned token: {token[:30]}...")
        else:
            print("🔑 _request_token() returned None!", file=sys.stderr)
            logger.error("🔑 _request_token() returned None!")

        return token

    async def _request_token(self) -> Optional[str]:
        """Запрос токена через Password Grant с Basic Auth"""
        import sys

        print("=" * 80, file=sys.stderr)
        print("🔑 _request_token() STARTED!", file=sys.stderr)
        print("=" * 80, file=sys.stderr)

        logger.info("=" * 80)
        logger.info("🔑 _request_token() STARTED")
        logger.info("=" * 80)

        # ✅ Кодируем client credentials для Basic Auth
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

        # ✅ Headers с Basic Auth
        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        # ✅ Data ТОЛЬКО с grant_type, username, password
        data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }

        try:
            logger.info(f"🔗 POST {self.keycloak_host}")
            logger.info(f"📤 Headers: Authorization=Basic {encoded_credentials[:20]}...")
            logger.info(f"📤 Data keys: {list(data.keys())}")
            logger.info(f"🔒 SSL Verify: {self.verify_ssl}")

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                logger.info("📡 Sending POST to Keycloak...")

                response = await client.post(
                    self.keycloak_host,
                    data=data,
                    headers=headers,
                    timeout=30.0
                )

                logger.info(f"📥 Keycloak response: {response.status_code}")

                if response.status_code == 200:
                    logger.info("✅ Got 200 OK, parsing response...")
                    return self._handle_success_response(response)
                else:
                    logger.error(f"❌ Got {response.status_code}, handling error...")
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

    def _handle_success_response(self, response) -> Optional[str]:
        """Обработка успешного ответа"""
        try:
            token_info = response.json()
            self._token = token_info.get("access_token")

            if not self._token:
                logger.error("❌ Response missing 'access_token' field!")
                logger.error(f"Available keys: {list(token_info.keys())}")
                return None

            # Вычисляем время истечения токена
            expires_in = token_info.get("expires_in", 300)  # По умолчанию 5 минут
            self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)

            # Показываем preview токена
            token_preview = self._get_token_preview(self._token)

            logger.info("=" * 60)
            logger.info("✅ Token obtained successfully")
            logger.info("=" * 60)
            logger.info(f"Token preview: {token_preview}")
            logger.info(f"Expires in: {expires_in}s ({expires_in // 60} min)")
            logger.info(f"Valid until: {self._token_expires_at.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)

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

        # Подсказки по частым ошибкам
        if response.status_code == 401:
            logger.error("💡 Hint: Check username, password, or client credentials in Basic Auth")
        elif response.status_code == 400:
            logger.error("💡 Hint: Check request format or grant_type parameter")

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
