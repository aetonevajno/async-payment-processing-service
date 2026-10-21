from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from payment_service.db.models import Currency, JsonValue


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("amount", mode="before")
    @classmethod
    def require_decimal_string(cls, amount: object) -> object:
        if not isinstance(amount, str):
            raise ValueError("amount must be a decimal string")
        return amount

    @field_validator("webhook_url")
    @classmethod
    def reject_credentials_and_fragment(cls, url: HttpUrl) -> HttpUrl:
        if url.username is not None or url.password is not None or url.fragment is not None:
            raise ValueError("webhook_url must not contain credentials or a fragment")
        return url
