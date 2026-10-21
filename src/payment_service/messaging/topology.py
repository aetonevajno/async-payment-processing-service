from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments", durable=True)
PAYMENTS_QUEUE = RabbitQueue("payments.new", durable=True, routing_key="payments.new")

DEAD_EXCHANGE = RabbitExchange("payments.dlx", durable=True)
DEAD_QUEUE = RabbitQueue("payments.dlq", durable=True, routing_key="payments.dead")


async def declare_payment_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    await queue.bind(exchange, routing_key=PAYMENTS_QUEUE.routing_key)

    dead_exchange = await broker.declare_exchange(DEAD_EXCHANGE)
    dead_queue = await broker.declare_queue(DEAD_QUEUE)
    await dead_queue.bind(dead_exchange, routing_key=DEAD_QUEUE.routing_key)
