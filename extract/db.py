import os
import dotenv
import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData


def database_connection_url():
    dotenv.load_dotenv()
    DB_USER: str = os.environ.get("MYSQL_USER")
    DB_PASSWD = os.environ.get("MYSQL_PWD")
    DB_HOST: str = os.environ.get("MYSQL_HOST")
    DB_PORT: str = os.environ.get("MYSQL_TCP_PORT")
    DB_NAME: str = os.environ.get("MYSQL_DB")
    return f"mysql+mysqldb://{DB_USER}:{DB_PASSWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

metadata = MetaData()

engine = create_engine(database_connection_url(), pool_pre_ping=True)

ballot = sa.Table("ballot", metadata, autoload_with=engine)
ballot_committee = sa.Table("ballot_committee", metadata, autoload_with=engine) 
ballot_committee_position = sa.Table("ballot_committee_position", metadata, autoload_with=engine) 
bill = sa.Table("bill", metadata, autoload_with=engine) 
candidate = sa.Table("candidate", metadata, autoload_with=engine) 
committee = sa.Table("committee", metadata, autoload_with=engine) 
contract = sa.Table("contract", metadata, autoload_with=engine) 
contract_lobbyist = sa.Table("contract_lobbyist", metadata, autoload_with=engine) 
contracted_lobbying = sa.Table("contracted_lobbying", metadata, autoload_with=engine) 
controlled_committee = sa.Table("controlled_committee", metadata, autoload_with=engine) 
direct_employment = sa.Table("direct_employment", metadata, autoload_with=engine) 
election = sa.Table("election", metadata, autoload_with=engine) 
employed_lobbying = sa.Table("employed_lobbying", metadata, autoload_with=engine) 
expenditure = sa.Table("expenditure", metadata, autoload_with=engine) 
general_committee = sa.Table("general_committee", metadata, autoload_with=engine) 
general_committee_candidate_position = sa.Table("general_committee_candidate_position", metadata, autoload_with=engine) 
general_committee_position = sa.Table("general_committee_position", metadata, autoload_with=engine) 
in_house_lobbyist = sa.Table("in_house_lobbyist", metadata, autoload_with=engine) 
independent_committee = sa.Table("independent_committee", metadata, autoload_with=engine) 
independent_committee_position = sa.Table("independent_committee_position", metadata, autoload_with=engine) 
individual_ballot_expenditure = sa.Table("individual_ballot_expenditure", metadata, autoload_with=engine) 
individual_candidate_expenditure = sa.Table("individual_candidate_expenditure", metadata, autoload_with=engine) 
individual_contribution = sa.Table("individual_contribution", metadata, autoload_with=engine) 
individual_filer = sa.Table("individual_filer", metadata, autoload_with=engine) 
lawmaker = sa.Table("lawmaker", metadata, autoload_with=engine) 
lobbying_firm = sa.Table("lobbying_firm", metadata, autoload_with=engine) 
lobbyist_employer = sa.Table("lobbyist_employer", metadata, autoload_with=engine) 
office = sa.Table("office", metadata, autoload_with=engine) 
organization = sa.Table("organization", metadata, autoload_with=engine) 
organization_ballot_expenditure = sa.Table("organization_ballot_expenditure", metadata, autoload_with=engine) 
organization_candidate_expenditure = sa.Table("organization_candidate_expenditure", metadata, autoload_with=engine) 
organization_contribution = sa.Table("organization_contribution", metadata, autoload_with=engine) 
organization_filer = sa.Table("organization_filer", metadata, autoload_with=engine) 
organization_name = sa.Table("organization_name", metadata, autoload_with=engine) 
permanent_employment = sa.Table("permanent_employment", metadata, autoload_with=engine) 
person = sa.Table("person", metadata, autoload_with=engine) 
subcontract = sa.Table("subcontract", metadata, autoload_with=engine) 
subcontracted_lobbying = sa.Table("subcontracted_lobbying", metadata, autoload_with=engine)