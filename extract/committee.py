import sqlalchemy as sa
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
import db
from collections import defaultdict
import re
import json
from datetime import datetime

committee_orgs = []

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
                    INSERT INTO PWDev.filer_id (organization_id, filer_id)
                    VALUES (:org_id, :filer_id)
                """), {"org_id": organization_id, "filer_id": filer_id})
        else:
            organization_id = check_filer.organization_id

        filing_id = org.FILING_ID.strip()
        amend_id = org.AMEND_ID.strip()

        committee_insert = conn.execute(sa.text("""
            INSERT INTO PWDev.committee (organization_id, 410_filing_id, 410_amendment_id)
            VALUES (:org_id, :filing_id, :amend_id)
        """), {"org_id": organization_id, "filing_id": filing_id, "amend_id": amend_id})
        # if entity_cd == "CTL":
        #     controlled_committee_insert = conn.execute(sa.text("""
        #         INSERT INTO PWDev.controlled_committee ()
        #     """))
        if entity_cd == "CAO":
            # Candidate (not controlled? idk)
        sa.text("""
        WITH cte AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY FILING_ID ORDER BY CAST(AMEND_ID AS UNSIGNED) DESC) AS "ord"
            FROM `CVR_SO_CD`
            WHERE FORM_TYPE = 'F410'
        ), cte2 AS (
            SELECT *
            FROM cte
            WHERE ord = 1
        )
        SELECT * FROM cte2 INNER JOIN `F501_502_CD` ON cte2.FILER_ID = `COMMITTEE_ID`;
        """)

        committee_orgs.append({
            str((org_name, filer_id, entity_cd)): organization_id
        })

        


with open("commitee_orgs.json", 'w') as f:
    json.dump(committee_orgs, f)