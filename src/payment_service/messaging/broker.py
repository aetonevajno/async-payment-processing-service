from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel

from payment_service.config import WorkerSettings


def create_broker(settings: WorkerSettings) -> RabbitBroker:
    return RabbitBroker(
        str(settings.rabbitmq_url),
        timeout=settings.rabbitmq_connection_timeout,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
