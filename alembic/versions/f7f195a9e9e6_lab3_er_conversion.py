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
revision: str = "f7f195a9e9e6"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "bill",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("prefix", sa.VARCHAR(7)),
        sa.Column("number", sa.Integer),
        sa.Column("legislative_session", YEAR),
        sa.Column("version", sa.Integer)
    )

    op.create_table(
        "person",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("DDDBPid", sa.Integer, sa.ForeignKey("DDDB2016Aug.Person.pid")),
        # sa.Column("DDDBPid", sa.Integer),
        sa.Column("first", sa.VARCHAR(255)),
        sa.Column("middle", sa.VARCHAR(255)),
        sa.Column("last", sa.VARCHAR(255)),
        sa.Column("suffix", sa.VARCHAR(255)),
        sa.Column("title", sa.VARCHAR(255))
    )

    op.create_table(
        "filer",
        sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid")),
        # sa.Column("organization_id", sa.Integer),
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person._id"), autoincrement=False),
        sa.Column("filer_id", sa.VARCHAR(255), primary_key=True)
    )

    op.create_table(
        "lobbyist",
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person._id"), primary_key=True, autoincrement=False),
        sa.Column("completed_ethics_course", sa.Boolean)
    )

    op.create_table(
        "lobbying_firm",
        sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid"), primary_key=True, autoincrement=False)
        # sa.Column("organization_id", sa.Integer, primary_key=True, autoincrement=False)
    )

    op.create_table(
        "permanent_employment",
        sa.Column("lobbyist_id", sa.Integer, sa.ForeignKey("lobbyist.person_id"), primary_key=True, autoincrement=False),
        sa.Column("lobbying_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id"), primary_key=True, autoincrement=False),
        sa.Column("604_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("604_amendment_id", sa.Integer),
        sa.Column("601_filing_id", sa.Integer),
        sa.Column("601_amendment_id", sa.Integer),
        sa.Column("legislative_session", YEAR)
    )

    op.create_table(
        "subcontract",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("subcontracting_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id")),
        sa.Column("subcontracted_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id")),
        sa.Column("601_filing_id", sa.Integer),
        sa.Column("601_amendment_id", sa.Integer),
        sa.Column("effective_date", sa.Date),
        sa.Column("period_of_contract", sa.VARCHAR(255)),
        sa.Column("legislative_session", YEAR),
        sa.UniqueConstraint("601_filing_id", "subcontracting_firm_id", "subcontracted_firm_id")
    )

    op.create_table(
        "subcontracted_lobbying",
        sa.Column("subcontract_id", sa.Integer, sa.ForeignKey("subcontract._id"), primary_key=True, autoincrement=False),
        sa.Column("bill_id", sa.Integer, sa.ForeignKey("bill._id"), primary_key=True, autoincrement=False),
        sa.Column("625_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("625_amendment_id", sa.Integer),
    )

    op.create_table(
        "lobbyist_employer",
        sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid"), primary_key=True, autoincrement=False)
        # sa.Column("organization_id", sa.Integer, primary_key=True, autoincrement=False)
    )

    op.create_table(
        "direct_employment",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("lobbyist_id", sa.Integer, sa.ForeignKey("lobbyist.person_id")),
        sa.Column("lobbyist_employer_id", sa.Integer, sa.ForeignKey("lobbyist_employer.organization_id")),
        sa.Column("603_filing_id", sa.Integer),
        sa.Column("603_amendment_id", sa.Integer),
        sa.Column("604_filing_id", sa.Integer),
        sa.Column("604_amendment_id", sa.Integer),
        sa.Column("legislative_session", YEAR),
        sa.UniqueConstraint("604_filing_id", "lobbyist_id", "lobbyist_employer_id")
    )

    op.create_table(
        "employed_lobbying",
        sa.Column("employed_id", sa.Integer, sa.ForeignKey("direct_employment._id"), primary_key=True, autoincrement=False),
        sa.Column("bill_id", sa.Integer, sa.ForeignKey("bill._id"), primary_key=True, autoincrement=False),
        sa.Column("635_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("635_amendment_id", sa.Integer)
    )

    op.create_table(
        "contract",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("lobbying_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id")),
        sa.Column("lobbyist_employer_id", sa.Integer, sa.ForeignKey("lobbyist_employer.organization_id")),
        sa.Column("601_filing_id", sa.Integer),
        sa.Column("601_amendment_id", sa.Integer),
        sa.Column("603_filing_id", sa.Integer),
        sa.Column("603_amendment_id", sa.Integer),
        sa.Column("effective_date", sa.Date),
        sa.Column("period_of_contract", sa.VARCHAR(255)),
        sa.Column("legislative_session", YEAR),
        sa.UniqueConstraint("601_filing_id", "lobbying_firm_id", "lobbyist_employer_id")
    )

    op.create_table(
        "contracted_lobbying",
        sa.Column("contract_id", sa.Integer, sa.ForeignKey("contract._id"), primary_key=True, autoincrement=False),
        sa.Column("bill_id", sa.Integer, sa.ForeignKey("bill._id"), primary_key=True, autoincrement=False),
        sa.Column("625_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("625_amendment_id", sa.Integer)
    )

    op.create_table(
        "district",
        sa.Column("chamber", ENUM("Senate", "Assembly"), primary_key=True),
        sa.Column("number", sa.Integer, primary_key=True, autoincrement=False),
    )

    op.create_table(
        "office",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("district_chamber", ENUM("Senate", "Assembly")),
        sa.Column("district_number", sa.Integer),
        sa.Column("type", ENUM("executive", "legislative", "municipal", "judicial")),
        sa.ForeignKeyConstraint(["district_chamber", "district_number"],
                                ["district.chamber", "district.number"])
    )

    op.create_table(
        "election",
        sa.Column("office_id", sa.Integer , sa.ForeignKey("office._id"), primary_key=True, autoincrement=False),
        sa.Column("election_type", ENUM("primary", "general", "recall", "special", "runoff")),
        sa.Column("date", sa.Date)
    )

    op.create_table(
        "candidate",
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person._id"), primary_key=True, autoincrement=False),
        sa.Column("office_id", sa.Integer , sa.ForeignKey("office._id"), autoincrement=False),
        sa.Column("party", sa.VARCHAR(20))
    )

    op.create_table(
        "lawmaker",
        sa.Column("candidate_id", sa.Integer, sa.ForeignKey("candidate.person_id"), primary_key=True, autoincrement=False),
    )

    op.create_table(
        "committee",
        sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid"), primary_key=True, autoincrement=False),
        # sa.Column("organization_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("410_filing_id", sa.Integer),
        sa.Column("410_amendment_id", sa.Integer)
    )

    op.create_table(
        "controlled_committee",
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id"), primary_key=True, autoincrement=False),
        sa.Column("candidate_id", sa.Integer, sa.ForeignKey("candidate.person_id"), autoincrement=False)
    )

    op.create_table(
        "ballot_committee",
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id"), primary_key=True, autoincrement=False)
    )

    op.create_table(
        "independent_committee",
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id"), primary_key=True, autoincrement=False)
    )

    op.create_table(
        "general_committee",
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id"), primary_key=True, autoincrement=False)
    )

    op.create_table(
        "ballot",
        sa.Column("ballot_number", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("legislative_session", YEAR, primary_key=True)
    )

    op.create_table(
        "ballot_support",
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id"), primary_key=True, autoincrement=False),
        sa.Column("candidate_id", sa.Integer, sa.ForeignKey("candidate.person_id"), primary_key=True, autoincrement=False),
        sa.Column("position", sa.Boolean)
    )

    op.create_table(
        "candidate_support",
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id"), primary_key=True, autoincrement=False),
        sa.Column("ballot_number", sa.Integer, sa.ForeignKey("ballot.ballot_number"), primary_key=True, autoincrement=False),
        sa.Column("position", sa.Boolean)
    )

    op.create_table(
        "expenditure",
        sa.Column("460_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("460_amendment_id", sa.Integer),
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id")),
        sa.Column("transaction_date", sa.Date),
        sa.Column("monetary_amount", sa.Integer),
        sa.Column("code", sa.VARCHAR(3)),
        sa.Column("description", sa.VARCHAR(255)),
        sa.Column("purpose", sa.VARCHAR(255))
    )

    op.create_table(
        "donor",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid")),
        # sa.Column("organization_id", sa.Integer),
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person._id")),
        sa.UniqueConstraint("person_id")
    )

    op.create_table(
        "sponsorship",
        sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid"), autoincrement=False),
        # sa.Column("organization_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("donor_id", sa.Integer, sa.ForeignKey("donor._id"), primary_key=True, autoincrement=False),
        sa.Column("transaction_date", sa.Date),
        sa.Column("monetary_amount", sa.Integer),
        sa.Column("code", sa.VARCHAR(3)),
        sa.Column("description", sa.VARCHAR(255)),
        sa.Column("purpose", sa.VARCHAR(255))
    )

    op.create_table(
        "contribution",
        sa.Column("460_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("460_amendment_id", sa.Integer),
        sa.Column("donor_id", sa.Integer, sa.ForeignKey("donor._id"), primary_key=True, autoincrement=False),
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id"), primary_key=True, autoincrement=False),
        sa.Column("amount", sa.Integer),
        sa.Column("classification", ENUM("major donor", "other"))
    )

    op.create_table(
        "independent_expenditure_ballot",
        sa.Column("461_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("461_amendment_id", sa.Integer),
        sa.Column("donor_id", sa.Integer, sa.ForeignKey("donor._id"), primary_key=True, autoincrement=False),
        sa.Column("ballot_number", sa.Integer, sa.ForeignKey("ballot.ballot_number"), primary_key=True, autoincrement=False),
        sa.Column("transaction_date", sa.Date),
        sa.Column("monetary_amount", sa.Integer),
        sa.Column("code", sa.VARCHAR(3)),
        sa.Column("description", sa.VARCHAR(255)),
        sa.Column("purpose", sa.VARCHAR(255))
    )

    op.create_table(
        "independent_expenditure_candidate",
        sa.Column("461_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("461_amendment_id", sa.Integer),
        sa.Column("donor_id", sa.Integer, sa.ForeignKey("donor._id"), primary_key=True, autoincrement=False),
        sa.Column("candidate_id", sa.Integer, sa.ForeignKey("candidate.person_id"), primary_key=True, autoincrement=False),
        sa.Column("transaction_date", sa.Date),
        sa.Column("monetary_amount", sa.Integer),
        sa.Column("code", sa.VARCHAR(3)),
        sa.Column("description", sa.VARCHAR(255)),
        sa.Column("purpose", sa.VARCHAR(255))
    )


def downgrade() -> None:
    op.drop_table("independent_expenditure_candidate")
    op.drop_table("independent_expenditure_ballot")
    op.drop_table("contribution")
    op.drop_table("sponsorship")
    op.drop_table("donor")
    op.drop_table("expenditure")
    op.drop_table("candidate_support")
    op.drop_table("ballot_support")
    op.drop_table("ballot")
    op.drop_table("general_committee")
    op.drop_table("independent_committee")
    op.drop_table("ballot_committee")
    op.drop_table("controlled_committee")
    op.drop_table("committee")
    op.drop_table("lawmaker")
    op.drop_table("candidate")
    op.drop_table("election")
    op.drop_table("office")
    op.drop_table("district")
    op.drop_table("contracted_lobbying")
    op.drop_table("contract")
    op.drop_table("employed_lobbying")
    op.drop_table("direct_employment")
    op.drop_table("lobbyist_employer")
    op.drop_table("subcontracted_lobbying")
    op.drop_table("subcontract")
    op.drop_table("permanent_employment")
    op.drop_table("lobbying_firm")
    op.drop_table("lobbyist")
    op.drop_table("filer")
    op.drop_table("person")
    op.drop_table("bill")
