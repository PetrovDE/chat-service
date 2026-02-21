# app/observability/middleware.py
from __future__ import annotations

import uuid
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.observability.context import request_id_ctx


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("x-request-id") or str(uuid.uuid4())
        token = request_id_ctx.set(rid)
        try:
            response = await call_next(request)
            response.headers["x-request-id"] = rid
            return response
        finally:
            request_id_ctx.reset(token)