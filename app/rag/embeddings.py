# app/rag/embeddings.py

import logging
from typing import List, Optional
from app.core.config import settings

logger = logging.getLogger(__name__)


class EmbeddingsManager:
    def __init__(self, mode: str = "local", ollama_url: Optional[str] = None, hub_url: Optional[str] = None,
                 keycloak_token: Optional[str] = None, system_user: Optional[str] = None):
        self.mode = mode
        self.ollama_url = ollama_url or settings.EMBEDDINGS_BASEURL
        self.hub_url = hub_url or settings.CORPORATE_API_URL
        self.keycloak_token = keycloak_token or settings.CORPORATE_API_TOKEN
        self.system_user = system_user or settings.CORPORATE_API_USERNAME
        self.model = settings.EMBEDDINGS_MODEL

    def switch_mode(self, mode: str):
        if mode not in ["local", "corporate"]:
            raise ValueError("Incorrect mode: must be 'local' or 'corporate'")
        self.mode = mode

    def switch_model(self, model: str):
        self.model = model

    def update_token(self, keycloak_token: str):
        self.keycloak_token = keycloak_token

    def get_available_models(self) -> List[str]:
        from app.services.llm.manager import llm_manager
        prev_mode = llm_manager.mode
        llm_manager.switch_mode(self.mode)
        models = llm_manager.get_available_models()
        llm_manager.switch_mode(prev_mode)
        return models

    def embedd_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Получить эмбеддинги для списка текстов.

        Важно: Ollama API >= v0.3 возвращает "embedding" (единственное число),
        поэтому для каждого текста делаем отдельный запрос.
        """
        if self.mode == "local":
            import requests
            endpoint = f"{self.ollama_url}/api/embeddings"

            all_embeddings = []

            for text in texts:
                payload = {
                    "model": self.model,
                    "prompt": text  # Используем "prompt" вместо "input"
                }

                try:
                    logger.debug(f"🔌 Requesting embedding from Ollama for text ({len(text)} chars)")
                    resp = requests.post(endpoint, json=payload, timeout=30)

                    # Проверка статуса
                    if resp.status_code != 200:
                        logger.error(f"❌ Ollama API error (status {resp.status_code}): {resp.text}")
                        raise RuntimeError(f"Ollama embeddings error: HTTP {resp.status_code} - {resp.text}")

                    # Парсинг JSON
                    try:
                        resp_json = resp.json()
                    except Exception as e:
                        logger.error(f"❌ Ollama response is not valid JSON: {resp.text}")
                        raise RuntimeError(f"Ollama non-JSON response: {resp.text}")

                    # Проверка наличия ключа embedding (единственное число!)
                    if "embedding" not in resp_json:
                        logger.error(f"❌ Ollama response missing 'embedding' key: {resp_json}")
                        raise RuntimeError(f"Ollama response format error: {resp_json}")

                    embedding = resp_json["embedding"]

                    # Проверка, что embedding не пустой
                    if not embedding or len(embedding) == 0:
                        logger.error(f"❌ Empty embedding returned from Ollama")
                        raise RuntimeError("Empty embedding returned from Ollama")

                    all_embeddings.append(embedding)
                    logger.debug(f"✅ Embedding received: {len(embedding)} dimensions")

                except requests.exceptions.RequestException as e:
                    logger.error(f"❌ Network error connecting to Ollama: {e}")
                    raise RuntimeError(f"Cannot connect to Ollama at {endpoint}: {e}")

            logger.info(f"✅ Generated {len(all_embeddings)} embeddings")
            return all_embeddings

        elif self.mode == "corporate":
            import requests
            endpoint = f"{self.hub_url}/llm/embeddings"
            headers = {
                "Authorization": f"Bearer {self.keycloak_token}",
                "X-System-User": self.system_user,
                "Content-Type": "application/json"
            }
            payload = {
                "model": self.model,
                "input": texts
            }

            try:
                logger.info(f"🔌 Requesting embeddings from Corporate API: {endpoint}")
                resp = requests.post(endpoint, json=payload, headers=headers, timeout=30)

                if resp.status_code != 200:
                    logger.error(f"❌ HUB API error (status {resp.status_code}): {resp.text}")
                    raise RuntimeError(f"HUB embeddings error: HTTP {resp.status_code} - {resp.text}")

                try:
                    resp_json = resp.json()
                except Exception as e:
                    logger.error(f"❌ HUB response is not valid JSON: {resp.text}")
                    raise RuntimeError(f"HUB non-JSON response: {resp.text}")

                if "embeddings" not in resp_json:
                    logger.error(f"❌ HUB response missing 'embeddings' key: {resp_json}")
                    raise RuntimeError(f"HUB response format error: {resp_json}")

                logger.info(f"✅ Embeddings received from HUB: {len(resp_json['embeddings'])} vectors")
                return resp_json["embeddings"]

            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Network error connecting to HUB: {e}")
                raise RuntimeError(f"Cannot connect to HUB at {endpoint}: {e}")
        else:
            raise RuntimeError(f"Unknown mode: {self.mode}")


embeddings_manager = EmbeddingsManager()
