import os
import dotenv
import sqlalchemy as sa
from sqlalchemy import create_engine


def database_connection_url(db_name):
    dotenv.load_dotenv()
    DB_USER: str = os.environ.get("MYSQL_USER")
    DB_PASSWD = os.environ.get("MYSQL_PWD")
    DB_HOST: str = os.environ.get("MYSQL_HOST")
    DB_PORT: str = os.environ.get("MYSQL_TCP_PORT")
    DB_NAME: str = os.environ.get("MYSQL_DB")
    return f"mysql+mysqldb://{DB_USER}:{DB_PASSWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


engine = create_engine(database_connection_url(), pool_pre_ping=True)

ballot = sa.Table("ballot", autoload_with=engine)
ballot_committee = sa.Table("ballot_committee", autload_with=engine) 
ballot_committee_position = sa.Table("ballot_committee_position", autload_with=engine) 
bill = sa.Table("bill", autload_with=engine) 
candidate = sa.Table("candidate", autload_with=engine) 
committee = sa.Table("committee", autload_with=engine) 
contract = sa.Table("contract", autload_with=engine) 
contract_lobbyist = sa.Table("contract_lobbyist", autload_with=engine) 
contracted_lobbying = sa.Table("contracted_lobbying", autload_with=engine) 
controlled_committee = sa.Table("controlled_committee", autload_with=engine) 
direct_employment = sa.Table("direct_employment", autload_with=engine) 
election = sa.Table("election", autload_with=engine) 
employed_lobbying = sa.Table("employed_lobbying", autload_with=engine) 
expenditure = sa.Table("expenditure", autload_with=engine) 
general_committee = sa.Table("general_committee", autload_with=engine) 
general_committee_candidate_position = sa.Table("general_committee_candidate_position", autload_with=engine) 
general_committee_position = sa.Table("general_committee_position", autload_with=engine) 
in_house_lobbyist = sa.Table("in_house_lobbyist", autload_with=engine) 
independent_committee = sa.Table("independent_committee", autload_with=engine) 
independent_committee_position = sa.Table("independent_committee_position", autload_with=engine) 
individual_ballot_expenditure = sa.Table("individual_ballot_expenditure", autload_with=engine) 
individual_candidate_expenditure = sa.Table("individual_candidate_expenditure", autload_with=engine) 
individual_contribution = sa.Table("individual_contribution", autload_with=engine) 
individual_filer = sa.Table("individual_filer", autload_with=engine) 
lawmaker = sa.Table("lawmaker", autload_with=engine) 
lobbying_firm = sa.Table("lobbying_firm", autload_with=engine) 
lobbyist_employer = sa.Table("lobbyist_employer", autload_with=engine) 
office = sa.Table("office", autload_with=engine) 
organization = sa.Table("organization", autload_with=engine) 
organization_ballot_expenditure = sa.Table("organization_ballot_expenditure", autload_with=engine) 
organization_candidate_expenditure = sa.Table("organization_candidate_expenditure", autload_with=engine) 
organization_contribution = sa.Table("organization_contribution", autload_with=engine) 
organization_filer = sa.Table("organization_filer", autload_with=engine) 
organization_name = sa.Table("organization_name", autload_with=engine) 
permanent_employment = sa.Table("permanent_employment", autload_with=engine) 
person = sa.Table("person", autload_with=engine) 
subcontract = sa.Table("subcontract", autload_with=engine) 
subcontracted_lobbying = sa.Table("subcontracted_lobbying", autload_with=engine)
