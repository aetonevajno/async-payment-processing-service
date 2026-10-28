# Async payment processing service

## Структура

```text
src/payment_service/
├── config.py             
├── api/
│   ├── app.py
│   ├── dependencies.py
│   ├── payments.py
│   └── schemas.py
├── payments/
│   ├── schemas.py
│   ├── service.py        # создание платежа и идемпотентность
│   └── processing.py     # попытки шлюза и доставки webhook
├── db/
│   ├── session.py
│   ├── models.py
│   ├── payments.py       # репозиторий платежей
│   └── outbox.py         # репозиторий событий и владение публикацией
├── messaging/
│   ├── broker.py
│   ├── topology.py
│   ├── publisher.py      # отправка и подтверждения RabbitMQ
│   └── outbox.py         # публикация с повторами
└── workers/              # запуск, жизненный цикл и ACK/NACK
tests/
├── conftest.py
├── integration/  
└── unit/
    ├── test_app.py
    └── test_config.py
pyproject.toml
uv.lock
migrations/
alembic.ini
Dockerfile
docker-compose.yaml
.env.example
```

## Конфигурация

| Переменная | Назначение | По умолчанию |
| --- | --- | --- |
| `PAYMENTS_API_KEY` | Ключ для всех API endpoints | Обязательна |
| `PAYMENTS_DATABASE_URL` | PostgreSQL DSN, строго `postgresql+asyncpg://...` | Обязательна |
| `PAYMENTS_RABBITMQ_URL` | RabbitMQ DSN, `amqp://...` или `amqps://...` | Обязательна |
| `PAYMENTS_RABBITMQ_CONNECTION_TIMEOUT` | Таймаут подключения к RabbitMQ, секунды | `10` |
| `PAYMENTS_DB_POOL_SIZE` | Размер пула PostgreSQL | `5` |
| `PAYMENTS_DB_MAX_OVERFLOW` | Дополнительные соединения при нагрузке | `5` |
| `PAYMENTS_DB_POOL_TIMEOUT` | Ожидание свободного соединения, секунды | `10` |
| `PAYMENTS_LOG_LEVEL` | DEBUG / INFO / WARNING / ERROR / CRITICAL | `INFO` |
| `PAYMENTS_PROCESSING_RETRY_BASE` | Начальная задержка повторной обработки и webhook, секунды | `1` |
| `PAYMENTS_GATEWAY_TIMEOUT` | Максимальное ожидание платёжного шлюза, секунды | `10` |
| `PAYMENTS_WEBHOOK_TIMEOUT` | Максимальное ожидание webhook, включая DNS, секунды | `10` |
| `PAYMENTS_WEBHOOK_PRIVATE_HOSTS` | JSON-массив разрешённых приватных хостов для webhook | `[]` |
| `PAYMENTS_OUTBOX_POLL_INTERVAL` | Пауза при пустой очереди Outbox / недоступности инфраструктуры, секунды | `1` |
| `PAYMENTS_OUTBOX_PUBLISH_TIMEOUT` | Максимальное ожидание публикации и подтверждения, секунды | `10` |
| `PAYMENTS_OUTBOX_RETRY_BASE` | Начальная задержка повторной публикации, секунды | `1` |
| `PAYMENTS_OUTBOX_RETRY_MAX` | Верхняя граница задержки повторной публикации, секунды | `60` |
