import sqlalchemy as sa
import db
import json

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
    # Only inserting the Candidates with a Committee associated with them
    # Thankfully, most of these forms don't have broken names
    candidates = conn.execute(sa.text("""
        WITH cte AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY FILING_ID ORDER BY CAST(AMEND_ID AS UNSIGNED) DESC) AS "cvr_ord"
            FROM `CalAccess`.`CVR_SO_CD`
            WHERE FORM_TYPE = 'F410'
        ), cte2 AS (
            SELECT
                FILING_ID AS 'cvr_FILING_ID',
                AMEND_ID AS 'cvr_AMEND_ID',
                FILER_ID AS 'cvr_FILER_ID',
                ENTITY_CD AS 'cvr_ENTITY_CD',
                FILER_NAML AS 'cvr_FILER_NAML'
            FROM cte
            WHERE cvr_ord = 1
        ), cte3 AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY CalAccess.`F501_502_CD`.FILING_ID ORDER BY CAST(CalAccess.`F501_502_CD`.AMEND_ID AS UNSIGNED) DESC) AS "501_ord"
            FROM cte2 INNER JOIN CalAccess.`F501_502_CD` ON cte2.cvr_FILER_ID = `COMMITTEE_ID`
            WHERE FORM_TYPE = 'F501'
        ), cte4 AS (
            SELECT * FROM cte3 WHERE 501_ord = 1
        )
        SELECT * FROM cte4;
    """)).all()

    cand_formatted = []
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
            INSERT INTO PWDev.person (DDDBPid, first, middle, last, title)
            VALUES (:dddbpid, :first, :middle, :last, :title)
        """), {"dddbpid": DDDBPid, "first": first, "middle": middle, "last": last, "title": title}).lastrowid

        conn.execute(sa.text("""
            INSERT INTO PWDev.candidate (person_id, party) VALUES (:person_id, :party)
        """), {"person_id": id, "party": party})

        conn.execute(sa.text("""
            INSERT INTO PWDev.filer_id (person_id, filer_id) VALUES (:person_id, :filer_id)
        """), {"person_id": id, "filer_id": filer_id})

        cand_formatted.append({
            "_id": id,
            "DDDBPid": DDDBPid,
            "first": first,
            "middle": middle,
            "last": last,
            "suffix": suffix,
            "title": title,
        })

with open("committee_candidate.json", "w") as f:
    json.dump(cand_formatted, f, indent=4)

    