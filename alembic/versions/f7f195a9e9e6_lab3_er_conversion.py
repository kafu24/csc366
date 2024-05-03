"""lab3 ER conversion

Revision ID: f7f195a9e9e6
Revises: 
Create Date: 2024-05-02 20:37:41.403402

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import ENUM, YEAR


# revision identifiers, used by Alembic.
revision: str = 'f7f195a9e9e6'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "bill",
        sa.Column("bill_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column(
            "prefix",
            ENUM("AB", "SB")
        ),
        sa.Column("number", sa.Integer),
        sa.Column("session", YEAR),
        sa.Column("version", sa.Integer)
    )

    op.create_table(
        "lobbyist",
        sa.Column("lobbyist_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("first", sa.VARCHAR(255)),
        sa.Column("middle", sa.VARCHAR(255)),
        sa.Column("last", sa.VARCHAR(255)),
        sa.Column("completed_ethics_course", sa.Boolean)
    )


def downgrade() -> None:
    op.drop_table("bill")
    op.drop_table("lobbyist")
