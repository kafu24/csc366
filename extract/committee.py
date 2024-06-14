import sqlalchemy as sa
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
import db
from collections import defaultdict
import re
import json
from datetime import datetime

committee_orgs = []
cand_formatted = []

party_codes = {
    "": 'N/A',
    "0": 'N/A',
    "16001": 'DEMOCRATIC',
    "16002": 'REPUBLICAN',
    "16003": 'GREEN PARTY',
    "16004": 'REFORM PARTY',
    "16005": 'AMERICAN INDEPENDENT PARTY',
    "16006": 'PEACE AND FREEDOM',
    "16007": 'INDEPENDENT',
    "16008": 'LIBERTARIAN',
    "16009": 'NON PARTISAN',
    "16010": 'NATURAL LAW',
    "16011": 'UNKNOWN',
    "16012": 'NO PARTY PREFERENCE',
    "16013": 'AMERICANS ELECT',
    "16014": 'UNKNOWN',
    "16020": 'PEACE AND FREEDOM'
}

with db.engine.begin() as conn:
    all_orgs = conn.execute(sa.text("""
        WITH cte AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY FILING_ID ORDER BY CAST(AMEND_ID AS UNSIGNED) DESC) AS "ord"
            FROM CalAccess.`CVR_SO_CD`
            WHERE FORM_TYPE = 'F410'
        )
        SELECT FILING_ID, AMEND_ID, ENTITY_CD, FILER_ID, FILER_NAML, CITY, ST
        FROM cte
        WHERE ord = 1
    """)).all()

    for org in all_orgs:
        filer_id = org.FILER_ID.strip()
        entity_cd = org.ENTITY_CD.strip()
        org_name = org.FILER_NAML.strip()
        check_filer = conn.execute(sa.text("""
            SELECT * FROM filer_id WHERE filer_id = :filer_id
        """), {"filer_id": filer_id}).first()
        if check_filer is None:
            # check if it exists in DDDB, handle accordingly
            city = org.CITY.strip()
            state = org.ST.strip()
            try:
                # exactly one match
                organization_id = conn.execute(sa.text("""
                    SELECT DISTINCT oid
                    FROM DDDB2016Aug.Organizations
                    WHERE name = :name
                """), {"name": org_name}).scalar_one()
            except MultipleResultsFound:
                # multiple exact matches, more specific
                try:
                    organization_id = conn.execute(sa.text("""
                        SELECT DISTINCT oid
                        FROM DDDB2016Aug.Organizations
                        WHERE name = :name AND (city = :city OR stateHeadquartered = :state)
                    """), {"name": org_name, "city": city, "state": state}).scalar_one()
                except (NoResultFound, MultipleResultsFound):
                    # still multiple matches, or none, insert new record
                    try:
                        organization_id = conn.execute(sa.text("""
                            INSERT INTO DDDB2016Aug.Organizations (name, city, stateHeadquartered)
                            VALUES (:name, :city, :state)
                        """), {"name": org_name, "city": city, "state": state}).lastrowid
                    except:
                        organization_id = conn.execute(sa.text("""
                            INSERT INTO DDDB2016Aug.Organizations (name)
                            VALUES (:name)
                        """), {"name": org_name}).lastrowid
                    print("inserted new. no org or multiple orgs in dddb:", org_name)
                    # organization_id = 0
            except NoResultFound:
                # zero matches, insert new record
                # TODO: not sure what other attributes to fill out
                try:
                    organization_id = conn.execute(sa.text("""
                        INSERT INTO DDDB2016Aug.Organizations (name, city, stateHeadquartered)
                        VALUES (:name, :city, :state)
                    """), {"name": org_name, "city": city, "state": state}).lastrowid
                except:
                    organization_id = conn.execute(sa.text("""
                        INSERT INTO DDDB2016Aug.Organizations (name)
                        VALUES (:name)
                    """), {"name": org_name}).lastrowid
                print("inserted new. no org in dddb:", org_name)
            # associate organization with filer id
            if filer_id != "":
                conn.execute(sa.text("""
                    INSERT INTO PWProd.filer_id (organization_id, filer_id)
                    VALUES (:org_id, :filer_id)
                """), {"org_id": organization_id, "filer_id": filer_id})
        else:
            organization_id = check_filer.organization_id

        filing_id = org.FILING_ID.strip()
        amend_id = org.AMEND_ID.strip()

        committee_insert = conn.execute(sa.text("""
            INSERT IGNORE INTO PWProd.committee (organization_id, 410_filing_id, 410_amendment_id)
            VALUES (:org_id, :filing_id, :amend_id)
        """), {"org_id": organization_id, "filing_id": filing_id, "amend_id": amend_id})


        candidates = conn.execute(sa.text("""
            WITH cte AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY CalAccess.F501_502_CD.FILING_ID ORDER BY CAST(CalAccess.F501_502_CD.AMEND_ID AS UNSIGNED) DESC) AS "501_ord"
                FROM CalAccess.F501_502_CD
                WHERE FORM_TYPE = 'F501' AND COMMITTEE_ID = :filer_id
            )
            SELECT * FROM cte WHERE 501_ord = 1;
        """), {"filer_id": filer_id}).all()

        for row in candidates:
            filer_id = row.FILER_ID.strip()
            party = party_codes[row.PARTY_CD.strip()]
            middle = row.CAN_NAMM.title().strip() if row.CAN_NAMM else None
            last = row.CAND_NAML.title().strip()
            first = row.CAND_NAMF.title().strip().split()
            title = row.CAND_NAMT.title().strip() if row.CAND_NAMT else None
            suffix = None 
            if len(first) > 1:
                if last == "Bates":
                    first = "Patricia"
                elif last == "Brown":
                    if first[0] == "Edmund":
                        middle = "Jerry"
                        first = "Edmund"
                    else:
                        middle = first[1]
                        first = first[0]
                elif last == "Campos":
                    first = "David F."
                elif last == "Chang":
                    first = "Ling Ling"
                elif last == "Chavez":
                    first = "Edward"
                elif last == "Cook":
                    if first[0] == "Colonel":
                        title = "Colonel"
                        middle = "A."
                        first = "Jim"
                    else:
                        middle = first[1]
                        first = first[0]
                elif last == "Dhanuka":
                    first = "PK"
                elif last == "Ford":
                    first = "Mary Jo"
                elif last == "Gaines":
                    if first[0] == "Edward":
                        middle = None
                        first = "Ted"
                    else:
                        middle = first[1]
                        first = first[0]
                elif last == "Gonzales":
                    if first[0] == "Alfonso":
                        first = "Alfonso"
                    else:
                        middle = first[1]
                        first = first[0]
                elif last == "Harrington":
                    middle = "D."
                    first = "Michael"
                elif last == "Horne":
                    midddle = "M."
                    first = "Susan"
                elif last == "Houston":
                    first = "Guy"
                elif last == "Huff":
                    first = "Bob"
                elif last == "Jawahar":
                    first = "CJ"
                elif last == "Jones":
                    if first[0] == "William":
                        middle = "L."
                        first = "William"
                    else:
                        middle = first[1]
                        first = first[0]
                elif last == "Leslie":
                    first = "R. Tim"
                elif last == "Limon":
                    first = "S. Monique"
                elif last == "Perata":
                    first = "Don"
                elif last == "Perez":
                    first = "V. Manuel"
                elif last == "Petrie-Norris":
                    first = "Cottie"
                elif last == "Prosper":
                    first = "Pierre-Richard"
                elif last == "Quirk":
                    first = "Bill"
                elif last == "Reyes":
                    middle = "Gomez"
                    first = "Eloise"
                elif last == "Simon":
                    suffix = "Jr."
                    middle = "E."
                    first = "William"
                elif last == "Sztraicher":
                    first = "Gustavo"
                elif last == "Vicente":
                    first = "Bulmaro"
                elif last == "Wright":
                    first = "Roderick"
                else:
                    middle = first[1]
                    first = first[0]
            else:
                first = first[0]
            
            if "Jr." in last:
                suffix = "Jr."
                if first == "Angel":
                    last = "Sanchez"
                elif first == "Francisco":
                    last = "Carrillo"
                elif first == "Herbert":
                    last = "Wesson"
                elif first == "James":
                    if middle == "F.":
                        last = "Battin"
                    else:
                        last = "Frazier"
                elif first == "William":
                    last = "Simon"
            
            if suffix is None:
                if middle is None:
                    dddb_search = conn.execute(sa.text("""
                        WITH cte AS (
                            SELECT pid, ROW_NUMBER() OVER (PARTITION BY first, middle, last ORDER BY pid DESC) AS "person_ord"
                            FROM DDDB2016Aug.Person
                            WHERE first = :first AND last = :last
                        )
                        SELECT * FROM cte WHERE person_ord = 1
                    """), {"first": first, "last": last}).all()
                else:
                    dddb_search = conn.execute(sa.text("""
                        WITH cte AS (
                            SELECT pid, ROW_NUMBER() OVER (PARTITION BY first, middle, last ORDER BY pid DESC) AS "person_ord"
                            FROM DDDB2016Aug.Person
                            WHERE first = :first AND middle = :middle AND last = :last
                        )
                        SELECT * FROM cte WHERE person_ord = 1
                    """), {"first": first, "middle": middle, "last": last}).all()
            else:
                if middle is None:
                    dddb_search = conn.execute(sa.text("""
                        WITH cte AS (
                            SELECT pid, ROW_NUMBER() OVER (PARTITION BY first, middle, last ORDER BY pid DESC) AS "person_ord"
                            FROM DDDB2016Aug.Person
                            WHERE first = :first AND last = :last AND suffix = :suffix
                        )
                        SELECT * FROM cte WHERE person_ord = 1
                    """), {"first": first, "last": last, "suffix": suffix}).all()
                else:
                    dddb_search = conn.execute(sa.text("""
                        WITH cte AS (
                            SELECT pid, ROW_NUMBER() OVER (PARTITION BY first, middle, last ORDER BY pid DESC) AS "person_ord"
                            FROM DDDB2016Aug.Person
                            WHERE first = :first AND middle = :middle AND last = :last AND suffix = :suffix
                        )
                        SELECT * FROM cte WHERE person_ord = 1
                    """), {"first": first, "middle": middle, "last": last, "suffix": suffix}).all()
            
            if len(dddb_search) == 1:
                # Exact one record match
                for dddb_row in dddb_search:
                    DDDBPid = dddb_row.pid
            else:
                # > 1 or 0
                DDDBPid = None

            id = conn.execute(sa.text("""
                INSERT INTO PWProd.person (DDDBPid, first, middle, last, title)
                VALUES (:dddbpid, :first, :middle, :last, :title)
            """), {"dddbpid": DDDBPid, "first": first, "middle": middle, "last": last, "title": title}).lastrowid

            conn.execute(sa.text("""
                INSERT INTO PWProd.candidate (person_id, party) VALUES (:person_id, :party)
            """), {"person_id": id, "party": party})

            conn.execute(sa.text("""
                INSERT INTO PWProd.filer_id (organization_id, person_id, filer_id) VALUES (:organization_id, :person_id, :filer_id)
            """), {"organization_id": organization_id, "person_id": id, "filer_id": filer_id})

            cand_formatted.append({
                "_id": id,
                "DDDBPid": DDDBPid,
                "first": first,
                "middle": middle,
                "last": last,
                "suffix": suffix,
                "title": title,
            })

            # Uh, this is a controlled committee, probably
            conn.execute(sa.text("""
                INSERT IGNORE INTO PWProd.controlled_committee (committee_id, candidate_id) VALUES (:committee_id, :candidate_id)
            """), {"committee_id": organization_id, "candidate_id": id})

        committee_orgs.append({
            str((org_name, filer_id, entity_cd)): organization_id
        })
        
with open("committee_candidate.json", "w") as f:
    json.dump(cand_formatted, f, indent=4)

with open("commitee_orgs.json", 'w') as f:
    json.dump(committee_orgs, f)