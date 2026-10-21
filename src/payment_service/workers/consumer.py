import asyncio
import logging
import signal
from functools import partial
from uuid import UUID

from faststream import AckPolicy
from faststream.rabbit import RabbitMessage
from faststream.rabbit.schemas import Channel
from pamqp.commands import Basic

from payment_service.config import WorkerSettings
from payment_service.db.session import create_engine, create_session_factory
from payment_service.messaging.broker import create_broker
from payment_service.messaging.topology import (
    DEAD_EXCHANGE,
    DEAD_QUEUE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    declare_payment_topology,
)
from payment_service.payments.gateway import simulate_payment
from payment_service.payments.webhook import deliver_webhook
from payment_service.workers.outbox import PublishNotConfirmed
from payment_service.workers.processing import InvalidEvent, process_event

logger = logging.getLogger(__name__)


async def run_consumer(settings: WorkerSettings) -> None:
    logging.getLogger("httpx2").setLevel(logging.WARNING)
    engine = create_engine(settings)
    sessions = create_session_factory(engine)
    broker = create_broker(settings)
    webhook = partial(deliver_webhook, settings=settings)

    @broker.subscriber(
        PAYMENTS_QUEUE,
        PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        decoder=lambda message: message.body,
        channel=Channel(prefetch_count=1),
    )
    async def consume(message: RabbitMessage) -> None:
        try:
            try:
                event_id = UUID(message.message_id)
            except ValueError as error:
                raise InvalidEvent("invalid_message_id") from error
            await process_event(event_id, sessions, simulate_payment, webhook, settings)
        except InvalidEvent as error:
            # Malformed messages have no payment FK, so cannot enter the database Outbox.
            # Confirm a persistent dead-letter copy before acknowledging the original.
            try:
                async with asyncio.timeout(settings.outbox_publish_timeout):
                    confirmation = await broker.publish(
                        {"reason": str(error), "original_message_id": message.message_id},
                        exchange=DEAD_EXCHANGE,
                        routing_key=DEAD_QUEUE.routing_key,
                        persist=True,
                        mandatory=True,
                        message_id=message.message_id,
                    )
                if not isinstance(confirmation, Basic.Ack):
                    raise PublishNotConfirmed()
            except Exception as publish_error:
                logger.error("Poison event transfer failed: %s", type(publish_error).__name__)
                await asyncio.sleep(settings.outbox_poll_interval)
                await message.nack(requeue=True)
                return
        except Exception as error:
            # Infrastructure/programming failures must never turn into a successful ACK.
            logger.error("Consumer transaction failed: %s", type(error).__name__)
            await asyncio.sleep(settings.outbox_poll_interval)
            await message.nack(requeue=True)
            return
        await message.ack()

    try:
        await broker.connect()
        await declare_payment_topology(broker)
        await broker.start()
        await asyncio.Event().wait()
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
        await run_consumer(settings)
    except asyncio.CancelledError as error:
        logger.info("Consumer stopped: %s", type(error).__name__)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


if __name__ == "__main__":
    asyncio.run(main())
