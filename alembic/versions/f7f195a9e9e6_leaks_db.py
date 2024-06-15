"""LEAKS DB

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
        "person",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        # sa.Column("DDDBPid", sa.Integer, sa.ForeignKey("DDDB2016Aug.Person.pid")),
        sa.Column("DDDBPid", sa.Integer),
        sa.Column("first", sa.VARCHAR(45)),
        sa.Column("middle", sa.VARCHAR(45)),
        sa.Column("last", sa.VARCHAR(200)),
        sa.Column("suffix", sa.VARCHAR(10)),
        sa.Column("title", sa.VARCHAR(10))
    )

    op.create_table(
        "filer_id",
        # sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid")),
        sa.Column("organization_id", sa.Integer),
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person._id"), autoincrement=False),
        sa.Column("filer_id", sa.VARCHAR(9))
    )

    op.create_table(
        "activity",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("activity", sa.VARCHAR(400)),
        sa.Column("filing_id", sa.Integer),
        sa.Column("amendment_id", sa.Integer)
    )

    op.create_table(
        "lobbyist",
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person._id"), primary_key=True, autoincrement=False),
    )

    op.create_table(
        "lobbying_firm",
        # sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid"), primary_key=True, autoincrement=False)
        sa.Column("organization_id", sa.Integer, primary_key=True, autoincrement=False)
    )
    
    op.create_table(
        "permanent_employment",
        sa.Column("lobbyist_id", sa.Integer, sa.ForeignKey("lobbyist.person_id"), primary_key=True, autoincrement=False),
        sa.Column("lobbying_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id"), primary_key=True, autoincrement=False),
        sa.Column("start", sa.Date, primary_key=True),
        sa.Column("end", sa.Date),
        sa.Column("legislative_session", YEAR),
        sa.Column("ethics_completion", sa.Date)
    )

    op.create_table(
        "lobbyist_employer",
        # sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid"), primary_key=True, autoincrement=False)
        sa.Column("organization_id", sa.Integer, primary_key=True, autoincrement=False)
    )

    op.create_table(
        "direct_employment",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("lobbyist_id", sa.Integer, sa.ForeignKey("lobbyist.person_id"), nullable=False),
        sa.Column("lobbyist_employer_id", sa.Integer, sa.ForeignKey("lobbyist_employer.organization_id"), nullable=False),
        sa.Column("start", sa.Date, nullable=False),
        sa.Column("end", sa.Date),
        sa.Column("legislative_session", YEAR),
        sa.Column("ethics_completion", sa.Date),
        sa.UniqueConstraint("lobbyist_id", "lobbyist_employer_id", "start")
    )

    op.create_table(
        "employed_lobbying",
        sa.Column("employed_id", sa.Integer, sa.ForeignKey("direct_employment._id"), primary_key=True, autoincrement=False),
        # sa.Column("bill_id", sa.VARCHAR(23), sa.ForeignKey("DDDB2016Aug.Bill.bid"), primary_key=True, autoincrement=False),
        sa.Column("activity_id", sa.Integer, sa.ForeignKey("activity._id"), primary_key=True, autoincrement=False)
    )

    op.create_table(
        "contract",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("lobbying_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id"), nullable=False),
        sa.Column("lobbyist_employer_id", sa.Integer, sa.ForeignKey("lobbyist_employer.organization_id"), nullable=False),
        sa.Column("601_filing_id", sa.Integer),
        sa.Column("601_amendment_id", sa.Integer),
        sa.Column("filing_date", sa.Date, nullable=False),
        sa.Column("effective_date", sa.Date, nullable=False),
        sa.Column("period_of_contract", sa.VARCHAR(30)),
        sa.Column("legislative_session", YEAR),
        sa.UniqueConstraint("lobbying_firm_id", "lobbyist_employer_id", "filing_date", "effective_date")
    )

    op.create_table(
        "contracted_lobbying",
        sa.Column("contract_id", sa.Integer, sa.ForeignKey("contract._id"), primary_key=True, autoincrement=False),
        # sa.Column("bill_id", sa.VARCHAR(23), sa.ForeignKeWy("DDDB2016Aug.Bill.bid"), primary_key=True, autoincrement=False),
        sa.Column("activity_id", sa.Integer, sa.ForeignKey("activity._id"), primary_key=True, autoincrement=False),
    )
    
    op.create_table(
        "subcontract",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("subcontracting_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id"), nullable=False),
        sa.Column("subcontracted_firm_id", sa.Integer, sa.ForeignKey("lobbying_firm.organization_id"), nullable=False),
        sa.Column("employer_id", sa.Integer, sa.ForeignKey("lobbyist_employer.organization_id"), nullable=False),
        sa.Column("601_filing_id", sa.Integer),
        sa.Column("601_amendment_id", sa.Integer),
        sa.Column("filing_date", sa.Date, nullable=False),
        sa.Column("effective_date", sa.Date, nullable=False),
        sa.Column("period_of_contract", sa.VARCHAR(30)),
        sa.Column("legislative_session", YEAR),
        sa.UniqueConstraint("subcontracting_firm_id", "subcontracted_firm_id", "employer_id", "filing_date", "effective_date")
    )

    op.create_table(
        "subcontracted_lobbying",
        sa.Column("subcontract_id", sa.Integer, sa.ForeignKey("subcontract._id"), primary_key=True, autoincrement=False),
        # sa.Column("bill_id", sa.VARCHAR(23), sa.ForeignKey("DDDB2016Aug.Bill.bid"), primary_key=True, autoincrement=False),
        sa.Column("activity_id", sa.Integer, sa.ForeignKey("activity._id"), primary_key=True, autoincrement=False),
    )

    op.create_table(
        "individual_lobbying",
        # sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid"), primary_key=True, autoincrement=False),
        sa.Column("organization_id", sa.Integer, primary_key=True, autoincrement=False),
        # sa.Column("bill_id", sa.VARCHAR(23), sa.ForeignKey("DDDB2016Aug.Bill.bid"), primary_key=True, autoincrement=False),
        sa.Column("activity_id", sa.Integer, sa.ForeignKey("activity._id"), primary_key=True, autoincrement=False),
        sa.Column("filing_date", sa.Date),
        sa.Column("legislative_session", YEAR),
    )

    op.create_table(
        "district",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("chamber", sa.VARCHAR(50), nullable=False),
        sa.Column("number", sa.VARCHAR(4), nullable=False),
        sa.UniqueConstraint("chamber", "number")
    )

    op.create_table(
        "office",
        sa.Column("_id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("title", sa.VARCHAR(80), nullable=False),
        sa.Column("agency", sa.VARCHAR(200), nullable=False),
        sa.Column("jurisdiction", sa.VARCHAR(30), nullable=False),
        sa.Column("district_id", sa.Integer, sa.ForeignKey("district._id")),
        sa.UniqueConstraint("title", "agency", "jurisdiction")
    )

    op.create_table(
        "election",
        sa.Column("office_id", sa.Integer , sa.ForeignKey("office._id"), primary_key=True, autoincrement=False),
        sa.Column("type", ENUM("N/A", "GENERAL", "PRIMARY", "RECALL", "SPECIAL ELECTION", "OFFICEHOLDER", "SPECIAL RUNOFF", "UNKNOWN"), primary_key=True),
        sa.Column("year", YEAR, primary_key=True)
    )

    op.create_table(
        "candidate",
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person._id"), primary_key=True, autoincrement=False),
        sa.Column("party", ENUM("N/A", "DEMOCRATIC", "REPUBLICAN", "GREEN PARTY", "REFORM PARTY", "AMERICAN INDEPENDENT PARTY", "PEACE AND FREEDOM", "INDEPENDENT", "LIBERTARIAN", "NON PARTISAN", "NATURAL LAW", "UNKNOWN", "NO PARTY PREFERENCE", "AMERICANS ELECT", "UNKNOWN", "PEACE AND FREEDOM"), primary_key=True)
    )

    op.create_table(
        "running",
        sa.Column("candidate_id", sa.Integer, sa.ForeignKey("candidate.person_id"), primary_key=True, autoincrement=False),
        sa.Column("election_office_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("election_type", ENUM("N/A", "GENERAL", "PRIMARY", "RECALL", "SPECIAL ELECTION", "OFFICEHOLDER", "SPECIAL RUNOFF", "UNKNOWN"), primary_key=True),
        sa.Column("election_year", YEAR, primary_key=True),
        sa.Column("501_filing_id", sa.Integer),
        sa.Column("501_amendment_id", sa.Integer),
        sa.ForeignKeyConstraint(["election_office_id", "election_type", "election_year"], ["election.office_id", "election.type", "election.year"])
    )

    op.create_table(
        "lawmaker",
        sa.Column("candidate_id", sa.Integer, sa.ForeignKey("candidate.person_id"), primary_key=True, autoincrement=False),
    )

    op.create_table(
        "committee",
        # sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid"), primary_key=True, autoincrement=False),
        sa.Column("organization_id", sa.Integer, primary_key=True, autoincrement=False),
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
        sa.Column("ballot_number", sa.VARCHAR(4), primary_key=True, autoincrement=False),
        sa.Column("legislative_session", YEAR, primary_key=True),
        sa.Column("name", sa.VARCHAR(200)),
        sa.Column("jurisdiction", sa.VARCHAR(40))
    )

    op.create_table(
        "ballot_support",
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id"), primary_key=True, autoincrement=False),
        sa.Column("ballot_number", sa.VARCHAR(4), primary_key=True, autoincrement=False),
        sa.Column("legislative_session", YEAR, primary_key=True, autoincrement=False),
        sa.Column("position", sa.Boolean),
        sa.ForeignKeyConstraint(["ballot_number", "legislative_session"], ["ballot.ballot_number", "ballot.legislative_session"])
    )

    op.create_table(
        "candidate_support",
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id"), primary_key=True, autoincrement=False),
        sa.Column("candidate_id", sa.Integer, sa.ForeignKey("candidate.person_id"), primary_key=True, autoincrement=False),
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
        # sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid")),
        sa.Column("organization_id", sa.Integer),
        sa.Column("person_id", sa.Integer, sa.ForeignKey("person._id")),
        sa.UniqueConstraint("person_id"),
        sa.UniqueConstraint("organization_id")
    )

    op.create_table(
        "sponsorship",
        # sa.Column("organization_id", sa.Integer, sa.ForeignKey("DDDB2016Aug.Organizations.oid"), autoincrement=False),
        sa.Column("organization_id", sa.Integer, primary_key=True, autoincrement=False),
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
        sa.Column("donor_id", sa.Integer, sa.ForeignKey("donor._id")),
        sa.Column("committee_id", sa.Integer, sa.ForeignKey("committee.organization_id")),
        sa.Column("amount", sa.Integer),
        sa.Column("classification", sa.VARCHAR(255)),
        sa.UniqueConstraint("donor_id", "committee_id")
    )

    op.create_table(
        "independent_expenditure_ballot",
        sa.Column("461_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("461_amendment_id", sa.Integer),
        sa.Column("donor_id", sa.Integer, sa.ForeignKey("donor._id")),
        sa.Column("ballot_number", sa.VARCHAR(4)),
        sa.Column("legislative_session", YEAR),
        sa.Column("transaction_date", sa.Date),
        sa.Column("monetary_amount", sa.Integer),
        sa.Column("code", sa.VARCHAR(3)),
        sa.Column("description", sa.VARCHAR(255)),
        sa.Column("purpose", sa.VARCHAR(255)),
        sa.ForeignKeyConstraint(["ballot_number", "legislative_session"], ["ballot.ballot_number", "ballot.legislative_session"]),
        sa.UniqueConstraint("donor_id", "ballot_number", "legislative_session")
    )

    op.create_table(
        "independent_expenditure_candidate",
        sa.Column("461_filing_id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("461_amendment_id", sa.Integer),
        sa.Column("donor_id", sa.Integer, sa.ForeignKey("donor._id")),
        sa.Column("candidate_id", sa.Integer, sa.ForeignKey("candidate.person_id")),
        sa.Column("transaction_date", sa.Date),
        sa.Column("monetary_amount", sa.Integer),
        sa.Column("code", sa.VARCHAR(3)),
        sa.Column("description", sa.VARCHAR(255)),
        sa.Column("purpose", sa.VARCHAR(255)),
        sa.UniqueConstraint("donor_id", "candidate_id")
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
    op.drop_table("running")
    op.drop_table("candidate")
    op.drop_table("election")
    op.drop_table("office")
    op.drop_table("district")
    op.drop_table("individual_lobbying")
    op.drop_table("subcontracted_lobbying")
    op.drop_table("subcontract")
    op.drop_table("contracted_lobbying")
    op.drop_table("contract")
    op.drop_table("employed_lobbying")
    op.drop_table("direct_employment")
    op.drop_table("lobbyist_employer")
    op.drop_table("permanent_employment")
    op.drop_table("lobbying_firm")
    op.drop_table("lobbyist")
    op.drop_table("activity")
    op.drop_table("filer_id")
    op.drop_table("person")
