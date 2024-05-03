# CSC 366 Digital Democracy Database Project

## Installation
Install dependencies from `requirements.txt`

```bash
pip install -r requirements.txt
```

Set up `.env` with the same parameters you use to connect to the database:

```
MYSQL_USER=""
MYSQL_PWD=""
MYSQL_HOST=""
MYSQL_TCP_PORT=""
MYSQL_DB=""
```

If you want, you could probably connect to the command line with these env variables
as well:
```bash
mysql --host $MYSQL_HOST --user $MYSQL_USER --port $MYSQL_TCP_PORT $MYSQL_DB
```

Run online script with:

```bash
alembic upgrade f7f1
```

Generate SQL with --sql parameter:

```bash
alembic upgrade f7f1 --sql
```

Downgrade has a similar operation:

```bash
alembic downgrade base
alembic downgrade f7f1:base --sql
```
