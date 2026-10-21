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
│   └── service.py
├── db/
│   ├── session.py
│   └── models.py
└── messaging/broker.py
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
