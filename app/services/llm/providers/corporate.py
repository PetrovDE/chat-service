# app/corporate_connector.py

import logging
import requests
from typing import Optional, Any, Dict
from app.core.config import settings

logger = logging.getLogger(__name__)


class CorporateConnector:
    def __init__(self,
                 base_url: Optional[str] = None,
                 system_user: Optional[str] = None,
                 token: Optional[str] = None):
        self.base_url = base_url or settings.CORPORATE_API_URL
        self.system_user = system_user or settings.CORPORATE_API_USERNAME
        self.token = token or settings.CORPORATE_API_TOKEN

    def set_token(self, token: str):
        self.token = token

    def get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-System-User": self.system_user,
            "Content-Type": "application/json"
        }

    def get(self, endpoint: str, params: Optional[dict] = None) -> Any:
        url = f"{self.base_url}{endpoint}"
        resp = requests.get(url, headers=self.get_headers(), params=params or {})
        if resp.status_code != 200:
            logger.error(f"Corporate GET error ({url}): {resp.text}")
            raise RuntimeError(f"Corporate GET error: {resp.text}")
        return resp.json()

    def post(self, endpoint: str, payload: dict) -> Any:
        url = f"{self.base_url}{endpoint}"
        resp = requests.post(url, headers=self.get_headers(), json=payload)
        if resp.status_code != 200:
            logger.error(f"Corporate POST error ({url}): {resp.text}")
            raise RuntimeError(f"Corporate POST error: {resp.text}")
        return resp.json()

    # Можно добавить другие методы (put, delete, etc.) и endpoint-specific wrappers


corporate_connector = CorporateConnector()
