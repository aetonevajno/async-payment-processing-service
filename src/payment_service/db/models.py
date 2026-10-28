from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key"),
        CheckConstraint("processing_attempts BETWEEN 0 AND 3", name="processing_attempts_range"),
        CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="webhook_attempts_range"),
        CheckConstraint("amount > 0 AND amount <> 'NaN'::numeric", name="amount_positive"),
        CheckConstraint("length(btrim(idempotency_key)) > 0", name="idempotency_key_not_blank"),
        CheckConstraint("jsonb_typeof(metadata) = 'object'", name="metadata_object"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name="status_processed_at",
        ),
        CheckConstraint("processed_at >= created_at", name="processed_after_creation"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(
        Enum(Currency, native_enum=False, create_constraint=True, name="currency")
    )
    description: Mapped[str] = mapped_column(Text)
    payment_metadata: Mapped[dict[str, JsonValue]] = mapped_column(
        "metadata", JSONB(none_as_null=True), server_default=text("'{}'::jsonb")
    )
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(
            PaymentStatus,
            values_callable=lambda statuses: [status.value for status in statuses],
            native_enum=False,
            create_constraint=True,
            name="status",
        ),
        server_default=text("'pending'"),
    )
    idempotency_key: Mapped[str] = mapped_column(String(255))
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    processing_attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    processing_exhausted: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))
    webhook_attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    webhook_exhausted: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_processing_error: Mapped[str | None] = mapped_column(Text)
    last_webhook_error: Mapped[str | None] = mapped_column(Text)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxEvent(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        UniqueConstraint("payment_id", "event_type", "attempt", name="uq_outbox_logical_attempt"),
        Index(
            "ix_outbox_ready", "available_at", "id", postgresql_where=text("published_at IS NULL")
        ),
        CheckConstraint("attempt >= 1", name="attempt_positive"),
        CheckConstraint("publish_attempts >= 0", name="publish_attempts_nonnegative"),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="payload_object"),
        CheckConstraint("length(btrim(event_type)) > 0", name="event_type_not_blank"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id", ondelete="RESTRICT"))
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB(none_as_null=True))
    attempt: Mapped[int] = mapped_column(Integer, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
    processing_token: Mapped[UUID | None] = mapped_column()
    processing_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_token: Mapped[UUID | None] = mapped_column()
    publish_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
