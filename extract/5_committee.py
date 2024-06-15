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
    None: "N/A",
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
            FROM CalAccess.CVR_SO_CD
            WHERE FORM_TYPE = 'F410'
        )
        SELECT *,
            IF(SUBSTRING_INDEX(SUBSTRING_INDEX(from_date, '/', -1), ' ', 1) % 2 = 0,
                SUBSTRING_INDEX(SUBSTRING_INDEX(from_date, '/', -1), ' ', 1) - 1,
                SUBSTRING_INDEX(SUBSTRING_INDEX(from_date, '/', -1), ' ', 1)) ls
        FROM cte INNER JOIN CalAccess.CVR_CAMPAIGN_DISCLOSURE_CD ON cte.FILER_ID = CalAccess.CVR_CAMPAIGN_DISCLOSURE_CD.FILER_ID
        WHERE ord = 1
    """)).all()

    for org in all_orgs:
        filer_id = org.FILER_ID.strip()
        entity_cd = org.ENTITY_CD.strip()
        org_name = org.FILER_NAML.strip()
        check_filer = conn.execute(sa.text("""
            SELECT * FROM PWProd.filer_id WHERE filer_id = :filer_id
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
                SELECT filing_id, amend_id, filer_id, cand_naml last, cand_namf first, can_namm middle, cand_namt title,
                    cand_nams suffix, office_cd, offic_dscr office, agency_nam agency, juris_cd, juris_dscr jurisdiction,
                    yr_of_elec election_year, elec_type election_type, party_cd, party, district_cd, dist_no district_num,
                    ROW_NUMBER() OVER (PARTITION BY CalAccess.F501_502_CD.FILING_ID ORDER BY CAST(CalAccess.F501_502_CD.AMEND_ID AS UNSIGNED) DESC) AS "501_ord"
                FROM CalAccess.F501_502_CD
                WHERE FORM_TYPE = 'F501' AND COMMITTEE_ID = :filer_id
            )
            SELECT * FROM cte WHERE 501_ord = 1;
        """), {"filer_id": filer_id}).all()

        for row in candidates:
            filer_id = row.filer_id
            first = row.first.upper().strip() if row.first else None
            middle = row.middle.upper().strip() if row.middle else None
            last = row.last.upper().strip() if row.last else None
            title = row.title if row.title else None
            suffix = row.suffix if row.suffix else None
            party = row.party if row.party else None
            party_cd = row.party_cd if row.party_cd else None
            # convert party code to party
            if party is None:
                party = party_codes[party_cd]
            # fix for when they have their entire name in last field only
            if first is None:
                first = last.split()[0]
            # fix for when they have middle grouped with last/first
            if middle == '' or middle is None:
                match = re.match(r'^(.*\S)\s+(\S+)$', last)
                if match and last[-1] == '.':
                    last = match.group(1)
                    middle = match.group(2)
                else:
                    if first[-1] == '.':
                        match = re.match(r'^(.*\S)\s+(\S+)$', first)
                        if match:
                            first = match.group(1)
                            middle = match.group(2)
                if middle == 'JR.' or middle == 'SR.':
                    suffix = middle
                    middle = None 
            # check if the person has already been inserted, also check if more info provided
            person_id = None
            current_first = None
            current_middle = None
            current_last = None
            result = conn.execute(sa.text("""
                SELECT p._id, p.first, p.middle, p.last, c.party
                FROM PWProd.filer_id f
                JOIN PWProd.person p ON f.person_id = p._id
                JOIN PWProd.candidate c ON c.person_id = p._id
                WHERE filer_id = :filer_id AND (first = :first OR first = :last)
            """), {"filer_id": filer_id, "first": first, "last": last}).fetchall()
            if result:
                person_id, current_first, current_middle, current_last, current_parties = result[0]
                current_parties = []
                for row in result:
                    current_parties.append(row.party)
            found = False
            if person_id is None:
                raise Exception

            id = person_id

            cand_formatted.append({
                "_id": id,
                "DDDBPid": None,
                "first": first,
                "middle": middle,
                "last": last,
                "suffix": suffix,
                "title": title,
            })

            # Uh, this is a controlled committee, probably.
            if org.CMTTE_TYPE == "C" or org.CONTROL_YN == "Y":
                conn.execute(sa.text("""
                    INSERT IGNORE INTO PWProd.controlled_committee (committee_id, candidate_id) VALUES (:committee_id, :candidate_id)
                """), {"committee_id": organization_id, "candidate_id": id})
        # This is also a ballot-measure committee, probably.
        if org.CMTTE_TYPE == "B":
            # TODO: See if you want to add CVR2 source to help? Seems contradictory sometimes.
            if org.BAL_NUM and org.BAL_NAME and org.BAL_JURIS and len(org.BAL_NUM) <= 4:
                bal_num = org.BAL_NUM.strip()
                bal_name = org.BAL_NAME.strip()
                bal_juris = org.BAL_JURIS.strip()
                ls = org.ls
                pos = True if org.SUP_OPP_CD.upper() == "S" else False
                conn.execute(sa.text("""
                    INSERT IGNORE INTO PWProd.ballot (ballot_number, legislative_session, name, jurisdiction)
                    VALUES (:ballot_number, :legislative_session, :name, :jurisdiction)
                """), {"ballot_number": bal_num, "legislative_session": ls, "name": bal_name, "jurisdiction": bal_juris})
                conn.execute(sa.text("""
                    INSERT IGNORE INTO PWProd.ballot_committee (committee_id) VALUES (:committee_id)
                """), {"committee_id": organization_id})
                conn.execute(sa.text("""
                    INSERT IGNORE INTO PWProd.ballot_support (committee_id, ballot_number, legislative_session, position)
                    VALUES (:committee_id, :ballot_number, :legislative_session, :position)
                """), {"committee_id": organization_id, "ballot_number": bal_num, "legislative_session": ls, "position": pos})
        # This is also a general purpose committee, probably.
        if org.CMTTE_TYPE == "G":
            conn.execute(sa.text("""
                INSERT IGNORE INTO PWProd.general_committee (committee_id) VALUES (:committee_id)
            """), {"committee_id": organization_id})
        if org.CMTTE_TYPE == "P":
            conn.execute(sa.text("""
                INSERT IGNORE INTO PWProd.independent_committee (committee_id) VALUES (:committee_id)
            """), {"committee_id": organization_id})

        committee_orgs.append({
            str((org_name, filer_id, entity_cd)): organization_id
        })
        
with open("committee_candidate.json", "w") as f:
    json.dump(cand_formatted, f, indent=4)

with open("commitee_orgs.json", 'w') as f:
    json.dump(committee_orgs, f)