import sqlalchemy
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
import db


with db.engine.begin() as connection:
    # ——————————— Lobbying ———————————
    # get the lobbyists from calaccess
    # - filing_id, amend_id to get the form for the lobbyist reg (F604)
    # - filer_id is the lobbyist's
    # - ethics is whether or not they completed the ethics course
    # - firm_name is the lobbying firm or lobbyist employer/coalitions name
    # - ls is the year the legislative session began
    lobbyist_reg = connection.execute(sqlalchemy.text("""
        WITH ranked_records AS (
            SELECT filing_id, amend_id, filer_id, complet_dt ethics, firm_name, ls_beg_yr ls,
                RANK() OVER (PARTITION BY FILING_ID ORDER BY AMEND_ID DESC) AS amendment_rank
            FROM `CalAccess`.`CVR_REGISTRATION_CD`
            WHERE form_type = 'F604' AND entity_cd = 'LBY')
        SELECT * FROM ranked_records
        WHERE amendment_rank = 1
    """)).fetchall()
    for lobbyist in lobbyist_reg:
        # get more detailed information on the lobbyist from calaccess
        # - first, middle, last, title, suffix
        # - can also get address, city, state, zip, phone, and email if necessary
        name_info = connection.execute(sqlalchemy.text("""
            SELECT DISTINCT CVR.filer_namf first, NAMES.namm middle, CVR.filer_naml last,
                CVR.filer_namt title, CVR.filer_nams suffix
            FROM `CalAccess`.`CVR_REGISTRATION_CD` CVR
            JOIN `CalAccess`.`NAMES_CD` NAMES
            ON CVR.filer_namf = NAMES.namf AND CVR.filer_naml = NAMES.naml
            WHERE CVR.filer_id = :filer_id
        """), {"filer_id": lobbyist.filer_id}).one()
        first = name_info.first if name_info.first else None
        middle = name_info.middle if name_info.middle else None
        last = name_info.last if name_info.last else None
        title = name_info.suffix if name_info.title else None
        suffix = name_info.suffix if name_info.suffix else None
        # check if they exist in DDDB, insert into person table accordingly
        try:
            # exactly one match
            dddb_pid = connection.execute(sqlalchemy.text("""
                SELECT pid
                FROM `DDDB2016Aug`.`Person`
                WHERE first = :first
                  AND (middle = :middle OR (middle IS NULL AND :middle IS NULL))
                  AND last = :last
                  AND (suffix = :suffix OR (suffix IS NULL AND :suffix IS NULL))
            """), {"first": first, "middle": middle, "last": last, "suffix": suffix}).one()
            lobbyist_id = connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`person` (DDDBPid, first, middle, last, title, suffix)
                VALUES (:pid, :first, :middle, :last, :title, :suffix)
            """), {"pid": dddb_pid.pid, "first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
        except NoResultFound or MultipleResultsFound:
            # multiple or zero matches
            lobbyist_id = connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`person` (first, middle, last, title, suffix)
                VALUES (:first, :middle, :last, :title, :suffix)
            """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
        except Exception as e:
            # TODO: handle this, add logging
            print(e)
        # associate lobbyist with filer id
        connection.execute(sqlalchemy.text("""
            INSERT INTO `PWDev`.`individual_filer` (person_id, filer_id)
            VALUES (:person_id, :filer_id)
        """), {"person_id": lobbyist_id, "filer_id": lobbyist.filer_id})
        # get the organization (firm/employer) they are registered with from calaccess
        # - filing_id, amend_id to get the form for the org (F601/603)
        # - form_type (F601/F603), entity_cd (FRM/LEM) to determine type
        # - filer_id, name, city, state
        try:
            organization_reg = connection.execute(sqlalchemy.text("""
                WITH ranked_records AS (
                    SELECT filing_id, amend_id, form_type, entity_cd, filer_id, filer_naml name, bus_city city, bus_st state,
                        RANK() OVER (PARTITION BY FILING_ID ORDER BY AMEND_ID DESC) AS amendment_rank
                    FROM `CalAccess`.`CVR_REGISTRATION_CD`
                    WHERE filer_naml = :org_name AND ls_beg_yr = :ls)
                SELECT * FROM ranked_records
                WHERE amendment_rank = 1
            """), {"org_name": lobbyist.firm_name, "ls": lobbyist.ls}).one()
        # TODO: logging
        except MultipleResultsFound:
            print("error: multiple orgs in calaccess named ", lobbyist.firm_name)
        except NoResultFound:
            print("error: no orgs in calaccess named ", lobbyist.firm_name)
        except Exception as e:
            print(e)
        # 6. check if it exists in DDDB, handle accordingly
        try:
            # exactly one match
            organization_id = connection.execute(sqlalchemy.text("""
                SELECT DISTINCT oid
                FROM DDDB2016Aug.Organizations
                WHERE name = :name
            """), {"name": lobbyist.firm_name}).one().oid
        except MultipleResultsFound:
            # multiple exact matches, more specific
            organization_id = connection.execute(sqlalchemy.text("""
                SELECT DISTINCT oid
                FROM DDDB2016Aug.Organizations
                WHERE name = :name AND city = :city AND stateHeadquartered = :state
            """), {"name": lobbyist.firm_name, "city": organization_reg.city, "state": organization_reg.state}).one().oid
        except NoResultFound:
            # zero matches, insert new record
            # TODO: not sure what other attributes to fill out
            # organization_id = connection.execute(sqlalchemy.text("""
            #     INSERT INTO `DDDB2016Aug`.`Organizations` (name, city, stateHeadquartered)
            #     VALUES (:name, :city, :state)
            # """), {"name": lobbyist.firm_name, "city": organization_reg.city, "state": organization_reg.state}).lastrowid
            print("noresultfound error")
        except Exception as e:
            # TODO: handle this, add logging
            print(e)
        # 7. if it is a lobbying firm
        if organization_reg.form_type == 'F601' and organization_reg.entity_cd == 'FRM':
            # insert the lobbyist as a contract lobbyist
            lobbyist_id = connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`contract_lobbyist` (person_id, completed_ethics_course)
                VALUES (:pid, :ethics)
            """), {"pid": lobbyist_id, "ethics": lobbyist.ethics}).lastrowid
            # insert the organization as a lobbying firm
            firm_id = connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`lobbying_firm` (organization_id)
                VALUES (:oid)
            """), {"oid": organization_id}).lastrowid
            # insert the permanent employment relation
            connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`permanent_employment` (lobbyist_id, lobbying_firm_id, 601_filing_id,
                    601_amendment_id, 604_filing_id, 604_amendment_id, legislative_session)
                VALUES (:lobbyist_id, :firm_id, :601_fid, :601_aid, :604_fid, :604_aid, :ls)
            """), {"lobbyist_id": lobbyist_id, "firm_id": firm_id, "601_fid": organization_reg.filing_id, "601_aid": organization_reg.amendment_id,
                   "604_fid": lobbyist.filing_id, "604_aid": lobbyist.amendment_id, "ls": lobbyist.ls})
        elif organization_reg.form_type == 'F603' and organization_reg.entity_cd == 'LEM':
            # insert the lobbyist as an in-house lobbyist
            lobbyist_id = connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`in_house_lobbyist` (person_id, completed_ethics_course)
                VALUES (:pid, :ethics)
            """), {"pid": lobbyist_id, "ethics": lobbyist.ethics}).lastrowid
            # insert the organization as a lobbyist employer
            employer_id = connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`lobbyist_employer` (organization_id)
                VALUES (:oid)
            """), {"oid": organization_id}).lastrowid
            # insert the direct employment relation
            connection.execute(sqlalchemy.text("""
                INSERT INTO `PWDev`.`direct_employment` (lobbyist_id, lobbyist_employer_id, 603_filing_id,
                    603_amendment_id, 604_filing_id, 604_amendment_id, legislative_session)
                VALUES (:lobbyist_id, :firm_id, :603_fid, :603_aid, :604_fid, :604_aid, :ls)
            """), {"lobbyist_id": lobbyist_id, "firm_id": employer_id, "603_fid": organization_reg.filing_id, "603_aid": organization_reg.amendment_id,
                   "604_fid": lobbyist.filing_id, "604_aid": lobbyist.amendment_id, "ls": lobbyist.ls})
        else:
            # TODO add logging
            print("error: not a firm or employer")
        # question: for lobbying stuff, are there orgs that do not have any lobbyists under them?
            
        # print(organization)
        break
  