FROM ghcr.io/astral-sh/uv:0.13.4 AS uv

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

COPY --from=uv /uv /uvx /bin/
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable \
    && groupadd --system payments \
    && useradd --system --gid payments --no-create-home payments

USER payments
EXPOSE 8000

CMD ["uvicorn", "payment_service.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
