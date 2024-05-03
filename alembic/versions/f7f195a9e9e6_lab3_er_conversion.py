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
            ENUM("AB", "SB") # TODO
        ),
        sa.Column("number", sa.Integer),
        sa.Column("session", YEAR),
        sa.Column("version", sa.Integer)
    )

    op.create_table(
        "organization",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("address", sa.VARCHAR(255))
    )

    op.create_table(
        "organization_name",
        sa.Column("organization_id", sa.Integer, sa.ForeignKey("organization.id"), primary_key=True, autoincrement=False),
        sa.Column("name", sa.VARCHAR(255))
    )

    op.create_table(
        "organization_filer",
        sa.Column("organization_id", sa.Integer, sa.ForeignKey("organization.id"), primary_key=True, autoincrement=False),
        sa.Column("filer_id", sa.VARCHAR(255))
    )

    op.create_table(
        "person",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("first", sa.VARCHAR(255)),
        sa.Column("middle", sa.VARCHAR(255)),
        sa.Column("last", sa.VARCHAR(255))
    )

    op.create_table(
        "individual_filer",
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person.id"), primary_key=True, autoincrement=False),
        sa.Column("filer_id", sa.VARCHAR(255))
    )

    op.create_table(
        "contract_lobbyist",
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person.id"), primary_key=True, autoincrement=False),
        sa.Column("completed_ethics_course", sa.Boolean)
    )

    op.create_table(
        "in_house_lobbyist",
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person.id"), primary_key=True, autoincrement=False),
        sa.Column("completed_ethics_course", sa.Boolean)
    )

    op.create_table(
        "lobbying_firm",
        sa.Column("organization_id", sa.Integer, sa.ForeignKey("organization.id"), primary_key=True)
    )

    op.create_table(
        "permanent_employment",
        sa.Column("lobbyist_id", sa.Integer, sa.ForeignKey("contract_lobbyist.person_id"), primary_key=True),
        sa.Column("lobbying_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id")),
        sa.Column("601_filing_id", sa.Integer),
        sa.Column("601_amendment_id", sa.Integer),
        sa.Column("604_filing_id", sa.Integer),
        sa.Column("604_amendment_id", sa.Integer)
    )

    op.create_table(
        "legislative_session",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("subcontracting_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id")),
        sa.Column("subcontracted_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id")),
        sa.Column("601_filing_id", sa.Integer),
        sa.Column("601_amendment_id", sa.Integer),
        sa.UniqueConstraint("subcontracting_firm_id", "subcontracted_firm_id", "601_filing_id", "601_amendment_id")
    )

    op.create_table(
        "subcontract",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("subcontracting_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id")),
        sa.Column("subcontracted_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id")),
        sa.Column("601_filing_id", sa.Integer),
        sa.Column("601_amendment_id", sa.Integer),
        sa.UniqueConstraint("subcontracting_firm_id", "subcontracted_firm_id", "601_filing_id", "601_amendment_id")
    )

    # op.create_table(
    #     "candidate",
    #     sa.Column("person_id", sa.Integer, sa.ForeignKey("person.id"), primary_key=True, autoincrement=False),
    #     sa.Column("term", sa.Integer),
    #     sa.Column("chamber", sa.Enum("Senate", "Assembly")),
    #     sa.Column("district", sa.Enum('District1', 'District2')),  # TODO
    #     sa.Column("party", sa.Enum('Party1', 'Party2')),  # TODO
    # )


def downgrade() -> None:
    # op.drop_table("candidate")
    op.drop_table("subcontract")
    op.drop_table("legislative_session")
    op.drop_table("permanent_employment")
    op.drop_table("lobbying_firm")
    op.drop_table("in_house_lobbyist")
    op.drop_table("contract_lobbyist")
    op.drop_table("individual_filer")
    op.drop_table("person")
    op.drop_table("organization_filer")
    op.drop_table("organization_name")
    op.drop_table("organization")
    op.drop_table("bill")
