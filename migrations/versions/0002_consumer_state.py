"""Persist processing and webhook attempts separately"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for phase in ("processing", "webhook"):
        op.add_column(
            "payments",
            sa.Column(f"{phase}_attempts", sa.Integer(), nullable=False, server_default="0"),
        )
        op.add_column(
            "payments",
            sa.Column(
                f"{phase}_exhausted", sa.Boolean(), nullable=False, server_default=sa.false()
            ),
        )
        op.add_column("payments", sa.Column(f"last_{phase}_error", sa.Text(), nullable=True))
        op.create_check_constraint(
            f"{phase}_attempts_range", "payments", f"{phase}_attempts BETWEEN 0 AND 3"
        )
    op.add_column(
        "payments", sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("payments", "webhook_delivered_at")
    for phase in ("processing", "webhook"):
        op.drop_constraint(op.f(f"ck_payments_{phase}_attempts_range"), "payments", type_="check")
        for suffix in ("attempts", "exhausted"):
            op.drop_column("payments", f"{phase}_{suffix}")
        op.drop_column("payments", f"last_{phase}_error")
