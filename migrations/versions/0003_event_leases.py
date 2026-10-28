"""Lease events across external calls without holding database locks"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for phase in ("processing", "publish"):
        op.add_column("outbox", sa.Column(f"{phase}_token", sa.Uuid(), nullable=True))
        op.add_column(
            "outbox", sa.Column(f"{phase}_until", sa.DateTime(timezone=True), nullable=True)
        )


def downgrade() -> None:
    for phase in ("processing", "publish"):
        op.drop_column("outbox", f"{phase}_until")
        op.drop_column("outbox", f"{phase}_token")
