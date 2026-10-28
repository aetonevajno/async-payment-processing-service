import asyncio
import logging
import signal
from functools import partial

from aio_pika.exceptions import (
    ChannelInvalidStateError,
    IncompatibleProtocolError,
    ProbableAuthenticationError,
)
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from payment_service.config import WorkerSettings
from payment_service.db.outbox import LeaseLost
from payment_service.db.session import create_engine, create_session_factory
from payment_service.messaging.broker import create_broker
from payment_service.messaging.outbox import publish_next
from payment_service.messaging.publisher import publish_event
from payment_service.messaging.topology import declare_payment_topology

logger = logging.getLogger(__name__)


async def run_publisher(settings: WorkerSettings) -> None:
    engine = create_engine(settings)
    sessions = create_session_factory(engine)
    broker = create_broker(settings)
    publish = partial(publish_event, broker=broker, settings=settings)
    try:
        while True:
            try:
                await broker.connect()
                await declare_payment_topology(broker)
                break
            except (ProbableAuthenticationError, IncompatibleProtocolError):
                raise
            except (OSError, ChannelInvalidStateError) as error:
                logger.warning("RabbitMQ connection unavailable: %s", type(error).__name__)
                await broker.stop()
                await asyncio.sleep(settings.outbox_poll_interval)
        while True:
            try:
                if await publish_next(sessions, publish, settings):
                    continue
            except LeaseLost as error:
                logger.warning("Outbox event reclaimed: %s", error)
            except (DBAPIError, PoolTimeoutError, OSError) as error:
                if isinstance(error, DBAPIError):
                    sqlstate = str(getattr(error.orig, "sqlstate", ""))
                    if not error.connection_invalidated and not sqlstate.startswith(
                        ("08", "53", "57")
                    ):
                        # Invalid SQL and schema errors require operator action.
                        raise
                logger.warning("Outbox database unavailable: %s", type(error).__name__)
            await asyncio.sleep(settings.outbox_poll_interval)
    finally:
        try:
            await broker.stop()
        finally:
            await engine.dispose()


async def main() -> None:
    settings = WorkerSettings()
    logging.basicConfig(level=settings.log_level)
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await run_publisher(settings)
    except asyncio.CancelledError as error:
        logger.info("Outbox publisher stopped: %s", type(error).__name__)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


if __name__ == "__main__":
    asyncio.run(main())
