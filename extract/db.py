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
ballot_support = sa.Table("ballot_support", metadata, autoload_with=engine) 
candidate = sa.Table("candidate", metadata, autoload_with=engine)
candidate_support = sa.Table("candidate_support", metadata, autoload_with=engine)
committee = sa.Table("committee", metadata, autoload_with=engine) 
contract = sa.Table("contract", metadata, autoload_with=engine) 
contracted_lobbying = sa.Table("contracted_lobbying", metadata, autoload_with=engine) 
contribution = sa.Table("contribution", metadata, autoload_with=engine) 
controlled_committee = sa.Table("controlled_committee", metadata, autoload_with=engine) 
direct_employment = sa.Table("direct_employment", metadata, autoload_with=engine) 
district = sa.Table("district", metadata, autoload_with=engine) 
donor = sa.Table("donor", metadata, autoload_with=engine) 
election = sa.Table("election", metadata, autoload_with=engine) 
employed_lobbying = sa.Table("employed_lobbying", metadata, autoload_with=engine) 
expenditure = sa.Table("expenditure", metadata, autoload_with=engine) 
filer_id = sa.Table("filer_id", metadata, autoload_with=engine) 
general_committee = sa.Table("general_committee", metadata, autoload_with=engine) 
independent_committee = sa.Table("independent_committee", metadata, autoload_with=engine)
independent_expenditure_ballot = sa.Table("independent_expenditure_ballot", metadata, autoload_with=engine) 
independent_expenditure_candidate = sa.Table("independent_expenditure_candidate", metadata, autoload_with=engine)   
lawmaker = sa.Table("lawmaker", metadata, autoload_with=engine) 
lobbying_firm = sa.Table("lobbying_firm", metadata, autoload_with=engine) 
lobbyist = sa.Table("lobbyist", metadata, autoload_with=engine) 
lobbyist_employer = sa.Table("lobbyist_employer", metadata, autoload_with=engine) 
office = sa.Table("office", metadata, autoload_with=engine)  
permanent_employment = sa.Table("permanent_employment", metadata, autoload_with=engine) 
person = sa.Table("person", metadata, autoload_with=engine) 
sponsorship = sa.Table("sponsorship", metadata, autoload_with=engine) 
subcontract = sa.Table("subcontract", metadata, autoload_with=engine) 
subcontracted_lobbying = sa.Table("subcontracted_lobbying", metadata, autoload_with=engine)