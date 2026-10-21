import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from secrets import compare_digest
from typing import Annotated, TypedDict

from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.db.session import create_engine, create_session_factory


class ApplicationState(TypedDict):
    session_factory: async_sessionmaker[AsyncSession]


class HealthResponse(BaseModel):
    status: str = "ok"


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings if settings is not None else Settings()
    logging.basicConfig(level=settings.log_level)
    expected_api_key = settings.api_key.get_secret_value().encode()
    api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

    async def authenticate(
        api_key: Annotated[str | None, Security(api_key_header)],
    ) -> None:
        if api_key is None or not compare_digest(api_key.encode(), expected_api_key):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing API key",
            )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[ApplicationState]:
        engine = create_engine(settings)
        try:
            yield {"session_factory": create_session_factory(engine)}
        finally:
            await engine.dispose()

    app = FastAPI(
        title="Payment processing service",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        openapi_url=None,
    )

    @app.get("/health")
    async def health() -> HealthResponse:
        return HealthResponse()

    return app
