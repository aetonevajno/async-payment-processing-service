from faststream.rabbit import RabbitBroker

from payment_service.config import Settings


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        str(settings.rabbitmq_url),
        timeout=settings.rabbitmq_connection_timeout,
    )
