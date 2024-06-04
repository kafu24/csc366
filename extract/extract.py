import sqlalchemy
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
import db
from collections import defaultdict
import re
import json
from datetime import datetime

with db.engine.begin() as connection:
    # ——————————— Lobbying ———————————
    # get the lobbyists from calaccess
    # - filing_id, amend_id to get the form for the lobbyist reg (F604)
    # - filer_id, first, last, title, and suffix are the lobbyist's
    # - sender_id is backup for if the firm_name is incorrect
    # - ethics is whether or not they completed the ethics course
    # - firm_name is the lobbying firm or lobbyist employer/coalitions name
    # - ls is the year the legislative session began
    lobbyist_regs = connection.execute(sqlalchemy.text("""
        WITH ranked_records AS (
            SELECT filing_id, amend_id, filer_id, filer_namf first, filer_naml last, filer_namt title,
                filer_nams suffix, sender_id, complet_dt ethics, firm_name, ls_beg_yr ls,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) AS amendment_rank
            FROM `CalAccess`.`CVR_REGISTRATION_CD`
            WHERE form_type = 'F604' AND entity_cd = 'LBY'
            )
        SELECT *
        FROM ranked_records
        WHERE amendment_rank = 1
    """)).fetchall()
    print(len(lobbyist_regs))
    for lobbyist_reg in lobbyist_regs:
        filer_id = lobbyist_reg.filer_id
        first = lobbyist_reg.first.upper() if lobbyist_reg.first else None
        middle = None
        last = lobbyist_reg.last.upper() if lobbyist_reg.last else None
        title = lobbyist_reg.title if lobbyist_reg.title else None
        suffix = lobbyist_reg.suffix if lobbyist_reg.suffix else None
        # fix for when they have their entire name in last field only
        if first is None:
            first = last.split()[0]
        # fix for when they have middle grouped with first
        match = re.match(r'^(.*\S)\s+(\S+)$', first)
        if match:
            first = match.group(1)
            middle = match.group(2)
        ethics = lobbyist_reg.ethics if lobbyist_reg.ethics else None
        if ethics is not None:
            ethics = datetime.strptime(ethics, "%m/%d/%Y %I:%M:%S %p")
        # check if the person has already been inserted, also check if more info provided
        person_id = None
        current_first = None
        current_middle = None
        current_last = None
        current_ethics = None
        result = connection.execute(sqlalchemy.text("""
            SELECT p._id, p.first, p.middle, p.last, l.completed_ethics_course
            FROM `PWDev`.`filer` f
            JOIN `PWDev`.`person` p ON f.person_id = p._id
            JOIN `PWDev`.`lobbyist` l ON l.person_id = p._id
            WHERE filer_id = :filer_id AND (first = :first OR first = :last)
        """), {"filer_id": filer_id, "first": first, "last": last}).first()
        if result is not None:
            person_id, current_first, current_middle, current_last, current_ethics = result
        found = False
        if person_id is None:
            # get all years of information on the lobbyist from calaccess to try and match
            # - first, last, title, suffix
            # - can also get address, city, state, zip, phone, and email if necessary
            all_name_info = connection.execute(sqlalchemy.text("""
                SELECT DISTINCT CVR.filer_namf first, CVR.filer_naml last,
                    CVR.filer_namt title, CVR.filer_nams suffix
                FROM `CalAccess`.`CVR_REGISTRATION_CD` CVR
                WHERE CVR.filer_id = :filer_id AND CVR.filer_namf LIKE CONCAT('%', :first, '%')
            """), {"filer_id": filer_id, "first": first}).fetchall()
            for name_info in all_name_info:
                first = name_info.first.upper() if name_info.first else None
                middle = None
                last = name_info.last.upper() if name_info.last else None
                title = name_info.title if name_info.title else None
                suffix = name_info.suffix if name_info.suffix else None
                match = re.match(r'^(.*\S)\s+(\S+)$', first)
                if match:
                    first = match.group(1)
                    middle = match.group(2)
                # check if they exist in DDDB, insert into person table accordingly
                try:
                    # exactly one match with middle
                    middle_initial = None
                    if middle:
                        middle_initial = middle.rstrip('.')
                    dddb_pid = connection.execute(sqlalchemy.text("""
                        SELECT pid
                        FROM `DDDB2016Aug`.`Person`
                        WHERE first = :first
                            AND ((:middle IS NOT NULL AND middle LIKE CONCAT(:middle, '%')) OR (:middle IS NULL AND (middle IS NULL OR middle = '')))
                            AND last = :last
                    """), {"first": first, "middle": middle_initial, "last": last}).scalar_one()
                    lobbyist_id = connection.execute(sqlalchemy.text("""
                        INSERT INTO `PWDev`.`person` (DDDBPid, first, middle, last, title, suffix)
                        VALUES (:pid, :first, :middle, :last, :title, :suffix)
                    """), {"pid": dddb_pid, "first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
                    found = True
                    break
                except NoResultFound:
                    try:
                        # if middle name exists in calaccess but not in dddb
                        dddb_pid = connection.execute(sqlalchemy.text("""
                            SELECT pid
                            FROM `DDDB2016Aug`.`Person`
                            WHERE first = :first
                                AND last = :last
                        """), {"first": first, "last": last}).scalar_one()
                        lobbyist_id = connection.execute(sqlalchemy.text("""
                            INSERT INTO `PWDev`.`person` (DDDBPid, first, middle, last, title, suffix)
                            VALUES (:pid, :first, :middle, :last, :title, :suffix)
                        """), {"pid": dddb_pid, "first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
                        found = True
                        break
                    except NoResultFound:
                        # zero matches with/without middle in dddb, give up
                        # print("no people found in dddb for: ", filer_id, name_info)
                        continue
                        # lobbyist_id = connection.execute(sqlalchemy.text("""
                        #     INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                        #     VALUES (:first, :middle, :last, :title, :suffix)
                        # """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
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
                            lobbyist_id = connection.execute(sqlalchemy.text("""
                                INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                                VALUES (:first, :middle, :last, :title, :suffix)
                            """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
                            found = True
                            break
                        except (NoResultFound, MultipleResultsFound):
                            # zero or multiple matches found, give up
                            # print("no people or multiple found in dddb for: ", filer_id, name_info)
                            continue
                            # lobbyist_id = connection.execute(sqlalchemy.text("""
                            #     INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                            #     VALUES (:first, :middle, :last, :title, :suffix)
                            # """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
                except MultipleResultsFound:
                    # multiple matches with middle name, give up
                    # print("multiple people in dddb: ", filer_id, name_info)
                    continue
                    # lobbyist_id = connection.execute(sqlalchemy.text("""
                    #     INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                    #     VALUES (:first, :middle, :last, :title, :suffix)
                    # """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
            if not found:
                print("gave up. no people or multiple found in dddb for: ", filer_id, first, last)
                lobbyist_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                    VALUES (:first, :middle, :last, :title, :suffix)
                """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
            # classify as lobbyist and associate with filer id
            connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`lobbyist` (person_id, completed_ethics_course)
                VALUES (:pid, :ethics)
            """), {"pid": lobbyist_id, "ethics": ethics})
            connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`filer` (person_id, filer_id)
                VALUES (:person_id, :filer_id)
            """), {"person_id": lobbyist_id, "filer_id": filer_id})
        else:
            # TODO: resolve this
            if first != current_first or middle != current_middle or last != current_last:
                print(first, middle, last)
                print(current_first, current_middle, current_last)
            # update name info if necessary
            if first is not None and current_first is None:
                print("updated name:", current_first, first)
                connection.execute(sqlalchemy.text("""
                    UPDATE `PWDev`.`person`
                    SET first = :first
                    WHERE _id = :pid
                """), {"pid": person_id, "first": first})
            if middle is not None and current_middle is None:
                print("updated name:", current_middle, middle)
                connection.execute(sqlalchemy.text("""
                    UPDATE `PWDev`.`person`
                    SET middle = :middle
                    WHERE _id = :pid
                """), {"pid": person_id, "middle": middle})
            if last is not None and current_last is None:
                print("updated name:", current_last, last)
                connection.execute(sqlalchemy.text("""
                    UPDATE `PWDev`.`person`
                    SET last = :last
                    WHERE _id = :pid
                """), {"pid": person_id, "last": last})
            # update ethics completion if necessary
            if ethics is not None and current_ethics is None:
                print("updated ethics:", ethics)
                connection.execute(sqlalchemy.text("""
                    UPDATE `PWDev`.`lobbyist`
                    SET completed_ethics_course = :ethics
                    WHERE person_id = :pid
                """), {"pid": person_id, "ethics": ethics})
        # get the organization (firm/employer) they are registered with from calaccess
        # - filing_id, amend_id to get the form for the org (F601/603)
        # - form_type (F601/F603), entity_cd (FRM/LEM) to determine type
        # - filer_id, name, city, state
        added_orgs = defaultdict(bool)
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
                WHERE filer_naml = :org_name AND ls_beg_yr = :ls
                AND ((form_type = 'F601' AND entity_cd = 'FRM') OR
                     (form_type IN ('F602', 'F603') AND entity_cd IN ('LEM', 'LCO'))))
            SELECT * FROM ranked_records
            WHERE amendment_rank = 1
        """), {"org_name": org_name, "ls": lobbyist_reg.ls}).all()
        # if they misprinted the name on the form and none found, look at sender_id for org if possible
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
                    WHERE filer_naml = :org_name AND ls_beg_yr = :ls
                    AND ((form_type = 'F601' AND entity_cd = 'FRM') OR
                         (form_type IN ('F602', 'F603') AND entity_cd IN ('LEM', 'LCO'))))
                SELECT * FROM ranked_records
                WHERE amendment_rank = 1
            """), {"org_name": org_name, "ls": lobbyist_reg.ls}).all()
        if not organization_regs:
            # try again with all years
            org_name = connection.execute(sqlalchemy.text("""
                WITH ranked_records AS (
                    SELECT FILER_NAML,
                        RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) AS amendment_rank
                    FROM `CalAccess`.`CVR_REGISTRATION_CD`
                    WHERE filer_id = :org_filer_id
                    )
                SELECT FILER_NAML FROM ranked_records
                WHERE amendment_rank = 1
            """), {"org_filer_id": lobbyist_reg.sender_id}).scalar()
            organization_regs = connection.execute(sqlalchemy.text("""
                WITH ranked_records AS (
                    SELECT filing_id, amend_id, form_type, entity_cd, filer_id, filer_naml name, bus_city city, bus_st state,
                        RANK() OVER (PARTITION BY FILING_ID ORDER BY AMEND_ID DESC) AS amendment_rank
                    FROM `CalAccess`.`CVR_REGISTRATION_CD`
                    WHERE filer_naml = :org_name
                    AND ((form_type = 'F601' AND entity_cd = 'FRM') OR
                         (form_type IN ('F602', 'F603') AND entity_cd IN ('LEM', 'LCO'))))
                SELECT * FROM ranked_records
                WHERE amendment_rank = 1
            """), {"org_name": org_name}).all()
        if not organization_regs:
            # give up
            print("no org in calaccess named: ", org_name, lobbyist_reg)
            continue
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
            try:
                organization_id = connection.execute(sqlalchemy.text("""
                    SELECT DISTINCT oid
                    FROM DDDB2016Aug.Organizations
                    WHERE name = :name AND (city = :city OR stateHeadquartered = :state)
                """), {"name": org_name, "city": organization_reg.city, "state": organization_reg.state}).scalar_one()
            except (NoResultFound, MultipleResultsFound):
                # still multiple matches, or none, insert new record
                # organization_id = connection.execute(sqlalchemy.text("""
                #     INSERT INTO `DDDB2016Aug`.`Organizations` (name, city, stateHeadquartered)
                #     VALUES (:name, :city, :state)
                # """), {"name": org_name, "city": organization_reg.city, "state": organization_reg.state}).lastrowid
                print("no org or mulitple orgs in dddb: ", org_name)
                organization_id = 0
        except NoResultFound:
            # zero matches, insert new record
            # TODO: not sure what other attributes to fill out
            # organization_id = connection.execute(sqlalchemy.text("""
            #     INSERT INTO `DDDB2016Aug`.`Organizations` (name, city, stateHeadquartered)
            #     VALUES (:name, :city, :state)
            # """), {"name": org_name, "city": organization_reg.city, "state": organization_reg.state}).lastrowid
            print("no org in dddb: ", org_name)
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
            if not added_orgs[filer_id]:
                connection.execute(sqlalchemy.text("""
                    INSERT INTO `PWDev`.`filer` (organization_id, filer_id)
                    VALUES (:org_id, :filer_id)
                """), {"org_id": organization_id, "filer_id": filer_id})
                added_orgs[filer_id] = True
    with open("added_orgs", 'w') as json_file:
        json.dump(added_orgs, json_file, indent=4)



# get the orgs that arent already recorded (those that have no lobbyists?)
# select org reg where filerid not already in our db