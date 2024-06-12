import sqlalchemy
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
import db
from collections import defaultdict
import re
from datetime import datetime
import json

added_orgs = defaultdict(str)
with db.engine.begin() as connection:
    # get the lobbyists and what orgs they've been employed by from calaccess
    # * one every 2 months for each employment relation
    # * TODO: some of the lobbyist reported org names and or start dates are wrong (7946/73485),
    #     so a match won't be found can probably go the other way from org -> lobbyist as well to
    #     double check, but will run into same problem with lobbyist names
    # * TODO: some lobbyists have same filer_id and first name, can match on more info,
    #     but difficult because it's not consistent at all; also rare anyway
    # - filing_id and amend_id are used to identify each employment relation
    # - form_type (F625, F635) are used to differentiate direct/permanent employment
    # - lb_filer_id, last, first, title, suffix are the lobbyist's
    # - ethics is the date if they completed the course for that legislative session
    # - org_filer_id, type (FRM, LEM, LCO), name, city, state are the organization's
    # - lby_actvty is filled for LEM that employ lobbyists
    # - start and end are the range of the period of employment (where the lobbying activity doesn't change)
    # - ls is the year of the beginning of the legislative session
    registrations = connection.execute(sqlalchemy.text("""
        WITH ranked_reg AS (
            SELECT filing_id, amend_id, form_type, filer_id, entity_cd, filer_naml, filer_namf, filer_namt, filer_nams,
                firm_name, firm_city, firm_st, lby_actvty, from_date start, thru_date end,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) amendment_rank
            FROM CalAccess.CVR_LOBBY_DISCLOSURE_CD
        ),
        lobbyist_reg AS (
            SELECT DISTINCT filer_id lb_filer_id, filer_naml last, filer_namf first, 
                filer_namt title, filer_nams suffix, firm_name org_name, start, end,
                IF(SUBSTRING_INDEX(SUBSTRING_INDEX(start, '/', -1), ' ', 1) % 2 = 0,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(start, '/', -1), ' ', 1) - 1,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(start, '/', -1), ' ', 1)) lb_ls
            FROM ranked_reg rr
            WHERE form_type = 'F615' AND entity_cd = 'LBY' AND amendment_rank = 1
        ),
        org_reg AS (
            SELECT filing_id filing_id, amend_id amend_id, form_type, filer_id org_filer_id,
                entity_cd type, filer_naml org_name, firm_city city, firm_st state, lby_actvty,
                start, end
            FROM ranked_reg
            WHERE form_type IN ('F625', 'F635') AND entity_cd IN ('FRM', 'LEM', 'LCO') AND amendment_rank = 1
        ),
        ethics AS (
            SELECT filer_id lb_filer_id, ls_beg_yr ls, MAX(complet_dt) ethics, filing_id reg_filing_id,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) amendment_rank
            FROM CalAccess.CVR_REGISTRATION_CD
            WHERE form_type = 'F604' AND entity_cd = 'LBY'
            GROUP BY lb_filer_id, ls
        )
        SELECT lb.*, e.ethics, org.*
        FROM lobbyist_reg lb
        JOIN org_reg org ON lb.org_name = org.org_name AND lb.start = org.start
        LEFT JOIN ethics e ON lb.lb_filer_id = e.lb_filer_id AND lb_ls = e.ls AND e.amendment_rank = 1
    """)).fetchall()
    for reg in registrations:
        # get information to fill person, lobbyist, and filerid
        filer_id = reg.lb_filer_id
        first = reg.first.upper().strip() if reg.first else None
        middle = None
        last = reg.last.upper().strip() if reg.last else None
        title = reg.title if reg.title else None
        suffix = reg.suffix if reg.suffix else None
        # fix for when they have their entire name in last field only
        if first is None:
            first = last.split()[0]
        # fix for when they have middle grouped with first
        match = re.match(r'^(.*\S)\s+(\S+)$', first)
        if match:
            first = match.group(1)
            middle = match.group(2)
        # check if the person has already been inserted, also check if more info provided
        person_id = None
        current_first = None
        current_middle = None
        current_last = None
        result = connection.execute(sqlalchemy.text("""
            SELECT p._id, p.first, p.middle, p.last
            FROM PWDev.filer_id f
            JOIN PWDev.person p ON f.person_id = p._id
            WHERE filer_id = :filer_id AND (first = :first OR first = :last)
        """), {"filer_id": filer_id, "first": first, "last": last}).first()
        if result is not None:
            person_id, current_first, current_middle, current_last = result
        found = False
        if person_id is None:
            # check if they exist in DDDB, insert into person table accordingly
            try:
                # exactly one match with middle
                middle_initial = None
                if middle:
                    middle_initial = middle.rstrip('.')
                dddb_pid = connection.execute(sqlalchemy.text("""
                    SELECT pid
                    FROM DDDB2016Aug.Person
                    WHERE first = :first
                        AND ((:middle IS NOT NULL AND middle LIKE CONCAT(:middle, '%')) OR
                            (:middle IS NULL AND (middle IS NULL OR middle = '')))
                        AND last = :last
                """), {"first": first, "middle": middle_initial, "last": last}).scalar_one()
                lobbyist_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO PWDev.person (DDDBPid, first, middle, last, title, suffix)
                    VALUES (:pid, :first, :middle, :last, :title, :suffix)
                """), {"pid": dddb_pid, "first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
                found = True
            except NoResultFound:
                try:
                    # if middle name exists in calaccess but not in dddb
                    dddb_pid = connection.execute(sqlalchemy.text("""
                        SELECT pid
                        FROM DDDB2016Aug.Person
                        WHERE first = :first
                            AND last = :last
                    """), {"first": first, "last": last}).scalar_one()
                    lobbyist_id = connection.execute(sqlalchemy.text("""
                        INSERT INTO PWDev.person (DDDBPid, first, middle, last, title, suffix)
                        VALUES (:pid, :first, :middle, :last, :title, :suffix)
                    """), {"pid": dddb_pid, "first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
                    found = True
                except NoResultFound:
                    # zero matches with/without middle in dddb, give up
                    pass
                except MultipleResultsFound:
                    # multiple matches with/without middle in dddb, attempt to filter by lobbyist source
                    try:
                        dddb_pid = connection.execute(sqlalchemy.text("""
                            SELECT pid
                            FROM DDDB2016Aug.Person
                            WHERE first = :first
                                AND last = :last
                                AND source = 'TT_Lobbyist'
                        """), {"first": first, "last": last}).scalar_one()
                        lobbyist_id = connection.execute(sqlalchemy.text("""
                            INSERT INTO PWDev.person (first, middle, last, title, suffix)
                            VALUES (:first, :middle, :last, :title, :suffix)
                        """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
                        found = True
                    except (NoResultFound, MultipleResultsFound):
                        # zero or multiple matches found, give up
                        pass
            except MultipleResultsFound:
                # multiple matches with middle name, give up
                pass
            if not found:
                print("inserted new. no people or multiple found in dddb for:", filer_id, first, last)
                lobbyist_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO PWDev.person (first, middle, last, title, suffix)
                    VALUES (:first, :middle, :last, :title, :suffix)
                """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
            # classify as lobbyist and associate with filer id
            connection.execute(sqlalchemy.text("""
                INSERT INTO PWDev.lobbyist (person_id)
                VALUES (:pid)
            """), {"pid": lobbyist_id})
            if filer_id != "":
                connection.execute(sqlalchemy.text("""
                    INSERT INTO PWDev.filer_id (person_id, filer_id)
                    VALUES (:person_id, :filer_id)
                """), {"person_id": lobbyist_id, "filer_id": filer_id})
        else:
            # TODO: resolve this, probably take most filled/latest
            if first != current_first or middle != current_middle or last != current_last:
                print("conflicting name:", first, middle, last, current_first, current_middle, current_last)
            # update name info if necessary
            if first is not None and current_first is None:
                # print("updated name:", current_first, first)
                connection.execute(sqlalchemy.text("""
                    UPDATE PWDev.person
                    SET first = :first
                    WHERE _id = :pid
                """), {"pid": person_id, "first": first})
            if middle is not None and current_middle is None:
                # print("updated name:", current_middle, middle)
                connection.execute(sqlalchemy.text("""
                    UPDATE PWDev.person
                    SET middle = :middle
                    WHERE _id = :pid
                """), {"pid": person_id, "middle": middle})
            if last is not None and current_last is None:
                # print("updated name:", current_last, last)
                connection.execute(sqlalchemy.text("""
                    UPDATE PWDev.person
                    SET last = :last
                    WHERE _id = :pid
                """), {"pid": person_id, "last": last})
        # get information to fill organization, filerid, firm/employer, employment, and employed lobbying
        org_name = reg.org_name.upper().strip()
        city = reg.city.upper() if reg.city else None
        state = reg.state.upper() if reg.state else None
        filer_id = reg.org_filer_id
        filing_id = reg.filing_id
        amend_id = reg.amend_id
        form_type = reg.form_type
        type = reg.type
        ethics = reg.ethics if reg.ethics else None
        start = reg.start
        end = reg.end
        activity = reg.lby_actvty
        ls = reg.lb_ls
        # fix for if they entered city and state together
        if city is not None:
            split_city = city.split(', ')
            if len(split_city) == 2:
                city, state = split_city
        # converting to string to datetime
        if ethics is not None:
            ethics = datetime.strptime(ethics, "%m/%d/%Y %I:%M:%S %p")
        start = datetime.strptime(start, "%m/%d/%Y %I:%M:%S %p")
        end = datetime.strptime(end, "%m/%d/%Y %I:%M:%S %p")
        # if already found
        if added_orgs[(org_name, filer_id, type)]:
            organization_id = added_orgs[(org_name, filer_id, type)]
        else:
            # check if it exists in DDDB, handle accordingly
            try:
                # exactly one match
                organization_id = connection.execute(sqlalchemy.text("""
                    SELECT DISTINCT oid
                    FROM DDDB2016Aug.Organizations
                    WHERE name = :name
                """), {"name": org_name}).scalar_one()
            except MultipleResultsFound:
                # multiple exact matches, more specific
                try:
                    organization_id = connection.execute(sqlalchemy.text("""
                        SELECT DISTINCT oid
                        FROM DDDB2016Aug.Organizations
                        WHERE name = :name AND (city = :city OR stateHeadquartered = :state)
                    """), {"name": org_name, "city": city, "state": state}).scalar_one()
                except (NoResultFound, MultipleResultsFound):
                    # still multiple matches, or none, insert new record
                    try:
                        organization_id = connection.execute(sqlalchemy.text("""
                            INSERT INTO DDDB2016Aug.Organizations (name, city, stateHeadquartered)
                            VALUES (:name, :city, :state)
                        """), {"name": org_name, "city": city, "state": state}).lastrowid
                    except:
                        organization_id = connection.execute(sqlalchemy.text("""
                            INSERT INTO DDDB2016Aug.Organizations (name)
                            VALUES (:name)
                        """), {"name": org_name}).lastrowid
                    print("inserted new. no org or multiple orgs in dddb:", org_name)
                    # organization_id = 0
            except NoResultFound:
                # zero matches, insert new record
                # TODO: not sure what other attributes to fill out
                try:
                    organization_id = connection.execute(sqlalchemy.text("""
                        INSERT INTO DDDB2016Aug.Organizations (name, city, stateHeadquartered)
                        VALUES (:name, :city, :state)
                    """), {"name": org_name, "city": city, "state": state}).lastrowid
                except:
                    organization_id = connection.execute(sqlalchemy.text("""
                        INSERT INTO DDDB2016Aug.Organizations (name)
                        VALUES (:name)
                    """), {"name": org_name}).lastrowid
                print("inserted new. no org in dddb:", org_name)
            # associate organization with filer id
            if filer_id != "":
                connection.execute(sqlalchemy.text("""
                    INSERT INTO PWDev.filer_id (organization_id, filer_id)
                    VALUES (:org_id, :filer_id)
                """), {"org_id": organization_id, "filer_id": filer_id})
            # check if it's a lobbying firm, insert if so
            if form_type == 'F625' and type == 'FRM':
                connection.execute(sqlalchemy.text("""
                    INSERT IGNORE INTO PWDev.lobbying_firm (organization_id)
                    VALUES (:oid)
                """), {"oid": organization_id})
            # check if it's a lobbyist employer, insert if so
            elif form_type == 'F635' and type in ['LEM', 'LCO']:
                connection.execute(sqlalchemy.text("""
                    INSERT IGNORE INTO PWDev.lobbyist_employer (organization_id)
                    VALUES (:oid)
                """), {"oid": organization_id})
            added_orgs[(org_name, filer_id, type)] = organization_id
        # check if it's a lobbying firm
        if form_type == 'F625' and type == 'FRM':
            if employed_id is None:
                # insert the permanent employment relation
                connection.execute(sqlalchemy.text("""
                    INSERT IGNORE INTO PWDev.permanent_employment (lobbyist_id, lobbying_firm_id, ethics_completion,
                        start, end, legislative_session)
                    VALUES (:lobbyist_id, :firm_id, :ethics, :start, :end, :ls)
                """), {"lobbyist_id": lobbyist_id, "firm_id": organization_id, "ethics": ethics, "start": start, "end": end, "ls": ls})
        # check if it's a lobbyist employer
        elif form_type == 'F635' and type in ['LEM', 'LCO']:
            # check if employment is already accounted for
            employed_id = connection.execute(sqlalchemy.text("""
                SELECT _id
                FROM PWDev.direct_employment
                WHERE lobbyist_id = :lobbyist_id AND lobbyist_employer_id = :firm_id AND start = :start
            """), {"lobbyist_id": lobbyist_id, "firm_id": organization_id, "start": start}).scalar_one_or_none()
            if employed_id is None:
                # insert the direct employment relation
                employed_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO PWDev.direct_employment (lobbyist_id, lobbyist_employer_id, ethics_completion,
                        start, end, legislative_session)
                    VALUES (:lobbyist_id, :firm_id, :ethics, :start, :end, :ls)
                """), {"lobbyist_id": lobbyist_id, "firm_id": organization_id, "ethics": ethics, "start": start, "end": end, "ls": ls}).lastrowid
            # TODO: parse the bills, way too many variations, I don't think it's possible
            # bills_split = activity.split(';')
            # for bill_type in bills_split:
            #     split = bill_type.strip().split(' ', 1)
            #     type = split[0].strip()
            #     # handle "NONE"
            #     try:
            #         numbers = split[1].split(',')
            #     except:
            #         continue
            #     for number in numbers:
            #         # attempt to find in dddb with type, number, and year
            #         bill_id = None
            #         try:
            #             bill_id = connection.execute(sqlalchemy.text("""
            #                 SELECT DISTINCT bid
            #                 FROM DDDB2016Aug.Bill
            #                 WHERE type = :type AND number = :number AND sessionYear = :ls AND state = 'CA'
            #             """), {"type": type, "number": number.strip(), "ls": ls}).scalar_one_or_none()
            #         except:
            #             continue
            #         # if exists in dddb
            #         if bill_id:
            #             # insert the employed lobbying relation
            #             connection.execute(sqlalchemy.text("""
            #                 INSERT INTO PWDev.employed_lobbying (employed_id, bill_id, 635_filing_id, 635_amendment_id)
            #                 VALUES (:employed_id, :bill_id, :filing_id, :amend_id)
            #             """), {"employed_id": employed_id, "bill_id": bill_id, "filing_id": filing_id, "amend_id": amend_id})
            activity_id = connection.execute(sqlalchemy.text("""
                SELECT _id
                FROM PWDev.activity
                WHERE filing_id = :filing_id
            """), {"filing_id": filing_id}).scalar_one_or_none()
            if activity_id is None:
                activity_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO PWDev.activity (activity,  filing_id, amendment_id)
                    VALUES (:activity, :filing_id, :amend_id)
                """), {"activity": activity, "filing_id": filing_id, "amend_id": amend_id}).lastrowid
            connection.execute(sqlalchemy.text("""
                INSERT IGNORE INTO PWDev.employed_lobbying (employed_id, activity_id)
                VALUES (:employed_id, :activity_id)
            """), {"employed_id": employed_id, "activity_id": activity_id})

added_orgs = {str(key): value for key, value in added_orgs.items()}
with open("lobbying_orgs.json", "w") as file:
    json.dump(added_orgs, file)
