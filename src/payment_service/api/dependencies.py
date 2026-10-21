import logging
from collections.abc import AsyncIterator
from typing import TypedDict

from fastapi import HTTPException, Request
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger(__name__)


class ApplicationState(TypedDict):
    session_factory: async_sessionmaker[AsyncSession]


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    session_factory: async_sessionmaker[AsyncSession] = request.state["session_factory"]
    
    try:
        async with session_factory() as session:
            yield session
    except (OSError, PoolTimeoutError, OperationalError) as error:
        logger.error("Database connection unavailable (%s)", type(error).__name__)
        raise HTTPException(status_code=503, detail="Database temporarily unavailable") from error
    except SQLAlchemyError as error:
        # Driver error messages can include entire rows, including merchant metadata.
        logger.error("Database operation failed (%s)", type(error).__name__)
        if isinstance(error, DBAPIError) and error.connection_invalidated:
            raise HTTPException(
                status_code=503, detail="Database temporarily unavailable"
            ) from error
        raise HTTPException(status_code=500, detail="Database operation failed") from error
