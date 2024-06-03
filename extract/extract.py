import sqlalchemy
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
import db
from collections import defaultdict
import re


with db.engine.begin() as connection:
    # ——————————— Lobbying ———————————
    # get the lobbyists from calaccess
    # - filing_id, amend_id to get the form for the lobbyist reg (F604)
    # - filer_id is the lobbyist's
    # - sender_id is backup for if the firm_name is incorrect
    # - ethics is whether or not they completed the ethics course
    # - firm_name is the lobbying firm or lobbyist employer/coalitions name
    # - ls is the year the legislative session began
    added_people = defaultdict(bool)
    lobbyist_regs = connection.execute(sqlalchemy.text("""
        WITH ranked_records AS (
            SELECT filing_id, amend_id, filer_id, sender_id, complet_dt ethics, firm_name, ls_beg_yr ls,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) AS amendment_rank
            FROM `CalAccess`.`CVR_REGISTRATION_CD`
            WHERE form_type = 'F604' AND entity_cd = 'LBY'
            )
        SELECT * FROM ranked_records
        WHERE amendment_rank = 1
        LIMIT 50
        OFFSET 150
    """)).fetchall()
    for lobbyist_reg in lobbyist_regs:
        filer_id = lobbyist_reg.filer_id
        ethics = lobbyist_reg.ethics if lobbyist_reg.ethics else None
        # get more detailed information on the lobbyist from calaccess
        # - first, middle, last, title, suffix
        # - print first name to compare for middle
        # - can also get address, city, state, zip, phone, and email if necessary
        all_name_info = connection.execute(sqlalchemy.text("""
            SELECT DISTINCT CVR.filer_namf first, NAMES.namm middle, CVR.filer_naml last,
                CVR.filer_namt title, CVR.filer_nams suffix, CVR.sig_namf first_print
            FROM `CalAccess`.`CVR_REGISTRATION_CD` CVR
            JOIN `CalAccess`.`NAMES_CD` NAMES
            ON CVR.filer_namf = NAMES.namf AND CVR.filer_naml = NAMES.naml
            WHERE CVR.filer_id = :filer_id
        """), {"filer_id": filer_id}).fetchall()
        for name_info in all_name_info:
            lobbyist_id = None
            first = name_info.first.upper() if name_info.first else None
            first_print = name_info.first_print.upper() if name_info.first_print else None
            if first is None:
                first = first_print
            middle = name_info.middle.upper() if name_info.middle else None
            if middle is None:
                match = re.match(r'^(.*\S)\s+(\S+)$', first)
                if match:
                    first = match.group(1)
                    middle = match.group(2)
        last = name_info.last.upper() if name_info.last else None
        title = name_info.title if name_info.title else None
        suffix = name_info.suffix if name_info.suffix else None
        # check if they exist in DDDB, insert into person table accordingly
        try:
            # exactly one match with middle
            middle_initial = None
            if middle:
                middle_initial = middle.rstrip('.')
            if filer_id == 'L23476':
                print('hi')
            dddb_pid = connection.execute(sqlalchemy.text("""
                SELECT pid
                FROM `DDDB2016Aug`.`Person`
                WHERE first = :first
                    AND ((:middle IS NOT NULL AND middle LIKE CONCAT(:middle, '%')) OR (:middle IS NULL AND middle IS NULL))
                    AND last = :last
            """), {"first": first, "middle": middle_initial, "last": last}).scalar_one()
            if not added_people[filer_id]:
                lobbyist_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO `PWDev`.`person` (DDDBPid, first, middle, last, title, suffix)
                    VALUES (:pid, :first, :middle, :last, :title, :suffix)
                """), {"pid": dddb_pid, "first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
        except NoResultFound:
            try:
                # if middle name exists in calaccess but not in dddb
                dddb_pid = connection.execute(sqlalchemy.text("""
                    SELECT pid
                    FROM `DDDB2016Aug`.`Person`
                    WHERE first = :first
                        AND last = :last
                """), {"first": first, "last": last}).scalar_one()
                if not added_people[filer_id]:
                    lobbyist_id = connection.execute(sqlalchemy.text("""
                        INSERT INTO `PWDev`.`person` (DDDBPid, first, middle, last, title, suffix)
                        VALUES (:pid, :first, :middle, :last, :title, :suffix)
                    """), {"pid": dddb_pid, "first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
            except NoResultFound:
                # zero matches with/without middle in dddb, give up
                print("no people found in dddb for: ", filer_id, name_info)
                if not added_people[filer_id]:
                    lobbyist_id = connection.execute(sqlalchemy.text("""
                        INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                        VALUES (:first, :middle, :last, :title, :suffix)
                    """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
            except MultipleResultsFound:
                # multiple matches with/without middle in dddb, attempt to filter by lobbyist source
                try:
                    dddb_pid = connection.execute(sqlalchemy.text("""
                        SELECT pid
                        FROM `DDDB2016Aug`.`Person`
                        WHERE first = :first
                            AND last = :last
                            AND source = 'TT_Lobbyist'
                    """), {"first": first, "last": last}).scalar_one()
                    if not added_people[filer_id]:
                        lobbyist_id = connection.execute(sqlalchemy.text("""
                            INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                            VALUES (:first, :middle, :last, :title, :suffix)
                        """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
                except (NoResultFound, MultipleResultsFound):
                    # zero or multiple matches found, give up
                    print("no people or multiple found in dddb for: ", filer_id, name_info)
                    if not added_people[filer_id]:
                        lobbyist_id = connection.execute(sqlalchemy.text("""
                            INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                            VALUES (:first, :middle, :last, :title, :suffix)
                        """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
        except MultipleResultsFound:
            # multiple matches with middle name, give up
            if not added_people[filer_id]:
                lobbyist_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                    VALUES (:first, :middle, :last, :title, :suffix)
                """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
        # classify as lobbyist and associate with filer id
        if not added_people[filer_id]:
            connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`lobbyist` (person_id, completed_ethics_course)
                VALUES (:pid, :ethics)
            """), {"pid": lobbyist_id, "ethics": ethics})
            connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`filer` (person_id, filer_id)
                VALUES (:person_id, :filer_id)
            """), {"person_id": lobbyist_id, "filer_id": filer_id})
        # get the organization (firm/employer) they are registered with from calaccess
        # - filing_id, amend_id to get the form for the org (F601/603)
        # - form_type (F601/F603), entity_cd (FRM/LEM) to determine type
        # - filer_id, name, city, state
        added_org = defaultdict(bool)
        org_name = lobbyist_reg.firm_name if lobbyist_reg.firm_name else None
        # if they forgot the name on the form, look at sender_id for org if possible
        if not org_name:
            org_name = connection.execute(sqlalchemy.text("""
                WITH ranked_records AS (
                    SELECT FILER_NAML,
                        RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) AS amendment_rank
                    FROM `CalAccess`.`CVR_REGISTRATION_CD`
                    WHERE filer_id = :org_filer_id AND ls_beg_yr = :ls
                    )
                SELECT FILER_NAML FROM ranked_records
                WHERE amendment_rank = 1
            """), {"org_filer_id": lobbyist_reg.sender_id, "ls": lobbyist_reg.ls}).scalar()
        organization_regs = connection.execute(sqlalchemy.text("""
            WITH ranked_records AS (
                SELECT filing_id, amend_id, form_type, entity_cd, filer_id, filer_naml name, bus_city city, bus_st state,
                    RANK() OVER (PARTITION BY FILING_ID ORDER BY AMEND_ID DESC) AS amendment_rank
                FROM `CalAccess`.`CVR_REGISTRATION_CD`
                WHERE filer_naml = :org_name AND ls_beg_yr = :ls)
            SELECT * FROM ranked_records
            WHERE amendment_rank = 1
        """), {"org_name": org_name, "ls": lobbyist_reg.ls}).all()
        # if they misprinted the name on the form, look at sender_id for org if possible
        if not organization_regs:
            org_name = connection.execute(sqlalchemy.text("""
                WITH ranked_records AS (
                    SELECT FILER_NAML,
                        RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) AS amendment_rank
                    FROM `CalAccess`.`CVR_REGISTRATION_CD`
                    WHERE filer_id = :org_filer_id AND ls_beg_yr = :ls
                    )
                SELECT FILER_NAML FROM ranked_records
                WHERE amendment_rank = 1
            """), {"org_filer_id": lobbyist_reg.sender_id, "ls": lobbyist_reg.ls}).scalar()
            organization_regs = connection.execute(sqlalchemy.text("""
                WITH ranked_records AS (
                    SELECT filing_id, amend_id, form_type, entity_cd, filer_id, filer_naml name, bus_city city, bus_st state,
                        RANK() OVER (PARTITION BY FILING_ID ORDER BY AMEND_ID DESC) AS amendment_rank
                    FROM `CalAccess`.`CVR_REGISTRATION_CD`
                    WHERE filer_naml = :org_name AND ls_beg_yr = :ls)
                SELECT * FROM ranked_records
                WHERE amendment_rank = 1
            """), {"org_name": org_name, "ls": lobbyist_reg.ls}).all()
        organization_reg = organization_regs[0]
        filer_id = organization_reg.filer_id
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
            organization_id = connection.execute(sqlalchemy.text("""
                SELECT DISTINCT oid
                FROM DDDB2016Aug.Organizations
                WHERE name = :name AND (city = :city OR stateHeadquartered = :state)
            """), {"name": org_name, "city": organization_reg.city, "state": organization_reg.state}).scalar_one()
        except NoResultFound:
            # zero matches, insert new record
            # TODO: not sure what other attributes to fill out
            # organization_id = connection.execute(sqlalchemy.text("""
            #     INSERT INTO `DDDB2016Aug`.`Organizations` (name, city, stateHeadquartered)
            #     VALUES (:name, :city, :state)
            # """), {"name": org_name, "city": organization_reg.city, "state": organization_reg.state}).lastrowid
            print("noresultfound in dddb: ", org_name)
            organization_id = 0
        # check if it's a lobbying firm
        if organization_reg.form_type == 'F601' and organization_reg.entity_cd == 'FRM':
            # insert the organization as a lobbying firm
            connection.execute(sqlalchemy.text("""
                INSERT IGNORE INTO `PWDev`.`lobbying_firm` (organization_id)
                VALUES (:oid)
            """), {"oid": organization_id})
            # insert the permanent employment relation
            connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`permanent_employment` (lobbyist_id, lobbying_firm_id, 601_filing_id,
                    601_amendment_id, 604_filing_id, 604_amendment_id, legislative_session)
                VALUES (:lobbyist_id, :firm_id, :601_fid, :601_aid, :604_fid, :604_aid, :ls)
            """), {"lobbyist_id": lobbyist_id, "firm_id": organization_id, "601_fid": organization_reg.filing_id, "601_aid": organization_reg.amend_id,
                  "604_fid": lobbyist_reg.filing_id, "604_aid": lobbyist_reg.amend_id, "ls": lobbyist_reg.ls})
        # check if it's a lobbyist employer
        elif organization_reg.form_type in ['F602', 'F603'] and organization_reg.entity_cd in ['LEM', 'LCO']:
            # insert the organization as a lobbyist employer
            connection.execute(sqlalchemy.text("""
                INSERT IGNORE INTO `PWDev`.`lobbyist_employer` (organization_id)
                VALUES (:oid)
            """), {"oid": organization_id})
            # insert the direct employment relation
            connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`direct_employment` (lobbyist_id, lobbyist_employer_id, 603_filing_id,
                    603_amendment_id, 604_filing_id, 604_amendment_id, legislative_session)
                VALUES (:lobbyist_id, :firm_id, :603_fid, :603_aid, :604_fid, :604_aid, :ls)
            """), {"lobbyist_id": lobbyist_id, "firm_id": organization_id, "603_fid": organization_reg.filing_id, "603_aid": organization_reg.amend_id,
                  "604_fid": lobbyist_reg.filing_id, "604_aid": lobbyist_reg.amend_id, "ls": lobbyist_reg.ls})
        else:
            print("error: not a firm or employer", organization_reg)
        # associate organization with filer id(s)
        for organization_reg in organization_regs:
            filer_id = organization_reg.filer_id
            if not added_org[filer_id]:
                connection.execute(sqlalchemy.text("""
                    INSERT INTO `PWDev`.`filer` (organization_id, filer_id)
                    VALUES (:org_id, :filer_id)
                """), {"org_id": organization_id, "filer_id": filer_id})
                added_org[filer_id] = True
        



# get the orgs that arent already recorded (those that have no lobbyists?)
# select org reg where filerid not already in our db