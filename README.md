# CSC 366 Digital Democracy Database Project

## Installation

Install dependencies from `requirements.txt`

```bash
$ pip install -r requirements.txt
```

Set up `.env` with the same parameters you use to connect to the database:

```
MYSQL_USER=""
MYSQL_PWD=""
MYSQL_HOST=""
MYSQL_TCP_PORT=""
MYSQL_DB=""
```

## Usage

If you want, you could probably connect to the command line with these env variables
as well:

```bash
$ source .env
$ mysql --host $MYSQL_HOST --user $MYSQL_USER --port $MYSQL_TCP_PORT $MYSQL_DB
```

Run online script with:

```bash
$ alembic upgrade f7f1
```

Generate SQL with --sql parameter:

```bash
$ alembic upgrade f7f1 --sql
```

Downgrade has a similar operation:

```bash
$ alembic downgrade base
$ alembic downgrade f7f1:base --sql
```

## Extraction

Files can be found in `/extract/`. Run them to extract the following information:

- `1_lobbying.py`: Lobbyist registration, which includes all information related to the lobbyist, the organization they are employed by, and the employment relation. Fills tables:
  - **person** for each lobbyist
  - **filer_id** for each lobbyist and organization
  - **lobbyist** for each lobbyist
  - **Organizations** for each organization
  - **lobbying_firm** for each organization that is classified as a lobbying firm
  - **permanent_employment** for each employment of a lobbyist by a lobbying firm (one relation per year)
  - **lobbyist_employer** for each organization that is classified as a lobbyist employer
  - **direct_employment** for each employment of a lobbyist by a lobbyist employer (one relation per year)
  - **activity** for each lobbying activity
  - **employed_lobbying** for each engagement of a lobbying activity by a lobbyist that is employed by an employer (one relation per quarter)
- `2_lobbying.py`: Contracts between lobbyist firms and lobbyist employers, which includes all information relating to the firms and employers, along with the contract. Fills tables:
  - **Organizations** for each organization
  - **lobbying_firm** for each lobbying firm
  - **lobbyist_employer** for each lobbyist employer
  - **filer_id** for each lobbying firm
  - **contract** for each contract between a lobbying firm and a lobbyist employer (one relation per quarter)
  - **activity** for each lobbying activity
  - **contract_lobbying** for each engagement of a lobbying activity as a result of a contract (one relation per quarter)
- `3_lobbying.py`:

1. Subcontracts between a subcontracting lobbying firm, subcontracted lobbying firm, and lobbyist employer, which includes all information relating to the organizations, along with the subcontract. Fills tables:

   - **Organizations** for each organization
   - **lobbying_firm** for each subcontracting and subcontracted lobbying firm
   - **lobbyist_employer** for each lobbyist employer
   - **filer_id** for each subcontracting lobbying firm
   - **subcontract** for each subcontract between a subcontracting lobbying firm, a subcontracted lobbying firm, and a lobbyist employer (one relation per quarter)
   - **activity** for each lobbying activity
   - **subcontract_lobbying** for each engagement of a lobbying activity as a result of a subcontract (one relation per quarter)

2. Entities spending $5,000 or more to influence lobbying activities, which includes all information relating to the entity, along with information of the activity they want to influence. Fills tables:
   - **Organizations** for each entity
   - **filer_id** for each entity
   - **activity** for each lobbying activity
   - **individual_lobbying** for each attempt to influence of a lobbying activity by the entity

- `4_candidates.py`: Candidate intention, along with information about the election they are running in, for what office they are running for, and which district that office presides over (if applicable). Fills tables:
  - **office**
  - **district** for each office being sought (populated for Senate, Assembly, or Board of Equalization)
  - **election** for each type and office being sought
  - **candidate**
  - **person** for each candidate
  - **running** for each time a candidate runs for an office

- `5_committee.py`: Committees, along with information about any associated ballot measure and candidate. Fills tables:
  - **Organizations** when not found
  - **filer_id** when new organization
  - **committee**
    - **ballot_committee** when type is marked as "B" and has all the necessary support to fill in ballot
      - **ballot**
      - **ballot_support**
    - **candidate_committee** when the candidate exists in **candidate** and type is marked as "C" or control_YN is marked as "Y"
    - **independent_committee** when type is marked as "P"
    - **general_committee** when type is marked as "G"