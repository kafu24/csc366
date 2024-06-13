import sqlalchemy as sa
import db
import json

with db.engine.begin() as conn:
    # Only inserting the Candidates with Committee associated with them
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
        SELECT * FROM cte3;
    """)).all()

    cand_formatted = []
    for row in candidates:
        # Edge Cases
        # | BROWN           | EDMUND       | G. (JERRY)  |  Edmund   Jerry   Brown     or Jerry   Brown will work fine
        # | PEREZ           | V. MANUEL    |             |  V. Manuel   Perez
        # | REYES           | ELOISE GOMEZ |             |  Eloise   Gomez
        # | CAMPOS          | DAVID F.     |             |  David F.   Campos
        middle = row.CAN_NAMM.title().strip() if row.CAN_NAMM else None
        last = row.CAND_NAML.title().strip()
        first = row.CAND_NAMF.title().strip().split()
        title = row.CAND_NAMT.title().strip() if row.CAND_NAMT else None
        suffix = None  # I know this
        if len(first) > 1:
            if last == "Brown":
                middle = "Jerry"
                first = "Edmund"
            elif last == "Perez":
                first = "V. Manuel"
            elif last == "Reyes":
                middle = "Gomez"
                first = "Eloise"
            elif last == "Campos":
                first = "David F."
            
        else:
            first = first[0]
        
        dddb_search = conn.execute(sa.text("""
            WITH cte AS (
                SELECT pid, ROW_NUMBER() OVER (PARTITION BY first, middle, last ORDER BY pid DESC) AS "person_ord"
                FROM DDDB2016Aug.Person
                WHERE first = :first AND middle = :middle AND last = :last AND title = :title
            )
            SELECT * FORM cte WHERE person_ord = 1
        """), {"first": first, "middle": middle, "last": last, "title": title}).first()

        if dddb_search is not None:
            conn.execute(sa.text("""
                INSERT INTO PWDev.person (DDDBPid, first, middle, last, title)
                VALUES (:dddbpid, :first, :middle, :last, :title)
            """), {"dddbpid": dddb_search.pid, "first": first, "middle": middle, "last": last, "title": title})
            cand_formatted.append({
                "DDDBPid": dddb_search.pid,
                "first": first,
                "middle": middle,
                "last": last,
                "suffix": suffix,
                "title": title,
            })
        else:
            conn.execute(sa.text("""
                INSERT INTO PWDev.person (first, middle, last, title)
                VALUES (:first, :middle, :last, :title)
            """), {"first": first, "middle": middle, "last": last, "title": title})
            cand_formatted.append({
                "DDDBPid": None,
                "first": first,
                "middle": middle,
                "last": last,
                "suffix": suffix,
                "title": title,
            })

with open("comm_candidate.json", "w") as f:
    json.dump(cand_formatted, f)

    