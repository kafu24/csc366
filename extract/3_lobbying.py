import sqlalchemy
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
import db
from collections import defaultdict
from datetime import datetime
import json

# restore the orgs from previous run
with open("lobbying_orgs.json", "r") as file:
    added_orgs = json.load(file)
added_orgs = {tuple(eval(key)): value for key, value in added_orgs.items()}
added_orgs = defaultdict(str, added_orgs)
with db.engine.begin() as connection:
    # TODO: again, missing a lot of matches
    # get firms and all of their subcontracts
    # * most firms did not double report in their firm registration form, so useless join
    # * we were able to get all of the subcontracts because no cross-form joins
    # * no filing_id for subfirm or employer; and no city and state for employer 
    # - filing_id, amend_id, and filing_start are the firm's lobbying disclosure
    # - reg_filing_id and reg_amend_id are the firm's registration
    # - firm_filer_id, firm_name, firm_city, firm_st, are the firm's
    # - subfirm_name, subfirm_city, subfirm_st, are the subfirm's
    # - emplr_name is the employer's
    # - ls is the year of the beginning of the legislative session
    subcontracts = connection.execute(sqlalchemy.text("""
        WITH firm AS (
            SELECT filing_id act_filing_id, amend_id act_amend_id, from_date filing_start,
                filer_id firm_filer_id, filer_naml firm_name, firm_city, firm_st,
                IF(SUBSTRING_INDEX(SUBSTRING_INDEX(from_date, '/', -1), ' ', 1) % 2 = 0,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(from_date, '/', -1), ' ', 1) - 1,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(from_date, '/', -1), ' ', 1)) ls,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) amendment_rank
            FROM CalAccess.CVR_LOBBY_DISCLOSURE_CD
            WHERE form_type = 'F625' AND entity_cd = 'FRM'
        ),
        ungrouped_firm_reg AS (
            SELECT filing_id reg_filing_id, amend_id reg_amend_id, filer_naml firm_name, ls_beg_yr ls,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) amendment_rank
            FROM CalAccess.CVR_REGISTRATION_CD
            WHERE form_type = 'F601' AND entity_cd = 'FRM'
        ),
        firm_reg AS (
            SELECT reg_filing_id, reg_amend_id, firm_name, ls,
                RANK() OVER (PARTITION BY firm_name, ls ORDER BY reg_filing_id DESC) occurrence_rank
            FROM ungrouped_firm_reg
            WHERE amendment_rank = 1
        ),
        contract AS (
            SELECT filing_id act_filing_id, amend_id act_amend_id, emplr_naml emplr_name, emplr_city, emplr_st, lby_actvty,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) amendment_rank
            FROM CalAccess.LPAY_CD
        ),
        employer AS (
            SELECT filing_id reg_filing_id, amend_id reg_amend_id,
                cli_naml cli_name, cli_city, cli_st, eff_date, con_period,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) amendment_rank
            FROM CalAccess.LEMP_CD
        ),
        subcontract AS (
            SELECT filing_id act_filing_id, amend_id act_amend_id, firm_name subfirm_name, firm_city subfirm_city,
                firm_st subfirm_st, subj_naml emplr_name,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) amendment_rank
            FROM CalAccess.LOTH_CD
        )
        SELECT *, f.*
        FROM employer e
        JOIN firm_reg fr ON e.reg_filing_id = fr.reg_filing_id AND e.amendment_rank = 1 AND fr.occurrence_rank = 1
        JOIN firm f ON f.firm_name = fr.firm_name AND f.ls = fr.ls AND f.amendment_rank = 1
        JOIN contract c ON e.cli_name = c.emplr_name AND c.act_filing_id = f.act_filing_id AND c.amendment_rank = 1
        JOIN subcontract sc ON f.act_filing_id = sc.act_filing_id AND c.emplr_name = sc.emplr_name AND sc.amendment_rank = 1
        JOIN firm sf ON sf.firm_name = sc.subfirm_name AND f.filing_start = sf.filing_start AND sf.amendment_rank = 1
        WHERE f.firm_name != '' AND e.cli_name != ''
    """)).fetchall()
    for cont in subcontracts:
        # get information to fill subcontract
        filing_id = cont.reg_filing_id
        amend_id = cont.reg_amend_id
        filing_start = cont.filing_start
        firm_filer_id = cont.firm_filer_id
        firm_name = cont.firm_name.upper().strip()
        firm_city = cont.firm_city.upper() if cont.firm_city else None
        firm_st = cont.firm_st.upper() if cont.firm_st else None
        subfirm_name = cont.subfirm_name.upper().strip()
        subfirm_city = cont.subfirm_city.upper() if cont.subfirm_city else None
        subfirm_st = cont.subfirm_st.upper() if cont.subfirm_st else None
        emplr_name = cont.emplr_name.upper().strip()
        activity = cont.lby_actvty
        eff_date = cont.eff_date
        con_period = cont.con_period
        ls = cont.ls
        # converting to string to datetime
        filing_start = datetime.strptime(filing_start, "%m/%d/%Y %I:%M:%S %p")
        try:
            eff_date = datetime.strptime(eff_date, "%m/%d/%Y %I:%M:%S %p")
        except:
            eff_date = None
        firm_id = None
        subfirm_id = None
        emplr_id = None
        for type, org_name, filer_id, city, state in zip(["FRM", "SFRM", "LEM"],
              [firm_name, subfirm_name, emplr_name], [firm_filer_id, None, None], 
              [firm_city, subfirm_city, None], [firm_st, subfirm_st, None]):
            # fix for if they entered city and state together
            if city is not None:
                split_city = city.split(', ')
                if len(split_city) == 2:
                    city, state = split_city
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
                # TODO: we are not associating the filer id for the subfirm nor the employer
                #   because we're starting from F625. it's possible to go the other way, both
                #   for confirmation and to get the filer ids, but that would result in the loss
                #   of relationships since it introduces possible inconsistencies of org names
                if filer_id is not None:
                    connection.execute(sqlalchemy.text("""
                        INSERT INTO PWDev.filer_id (organization_id, filer_id)
                        VALUES (:org_id, :filer_id)
                    """), {"org_id": organization_id, "filer_id": filer_id})
                # classify as firm/employer
                if type in ["FRM", "SFRM"]:
                    connection.execute(sqlalchemy.text("""
                        INSERT IGNORE INTO PWDev.lobbying_firm (organization_id)
                        VALUES (:oid)
                    """), {"oid": organization_id})
                else:
                    connection.execute(sqlalchemy.text("""
                        INSERT IGNORE INTO PWDev.lobbyist_employer (organization_id)
                        VALUES (:oid)
                    """), {"oid": organization_id})
                added_orgs[(org_name, filer_id, type)] = organization_id  
            if type == "FRM":
                firm_id = organization_id
            elif type == "SFRM":
                subfirm_id = organization_id
            else:
                emplr_id = organization_id
        # insert the subcontract
        try:
            subcontract_id = connection.execute(sqlalchemy.text("""
                INSERT INTO PWDev.subcontract (subcontracting_firm_id, subcontracted_firm_id, employer_id, 601_filing_id,
                    601_amendment_id, filing_date, effective_date, period_of_contract, legislative_session)
                VALUES (:firm_id, :subfirm_id, :emplr_id, :filing_id, :amend_id, :filing_start, :eff_date, :con_period, :ls)
            """), {"firm_id": firm_id, "subfirm_id": subfirm_id, "emplr_id": emplr_id, "filing_id": filing_id,
                  "amend_id": amend_id, "filing_start": filing_start, "eff_date": eff_date, "con_period": con_period, "ls": ls}).lastrowid
        except:
            continue
        # insert the activity
        filing_id = cont.act_filing_id
        amend_id = cont.act_amend_id
        activity_id = connection.execute(sqlalchemy.text("""
            INSERT INTO PWDev.activity (activity, filing_id, amendment_id)
            VALUES (:activity, :filing_id, :amend_id)
        """), {"activity": activity, "filing_id": filing_id, "amend_id": amend_id}).lastrowid
        # insert the lobbying relation
        connection.execute(sqlalchemy.text("""
            INSERT INTO PWDev.subcontracted_lobbying (subcontract_id, activity_id)
            VALUES (:subcontract_id, :activity_id)
        """), {"subcontract_id": subcontract_id, "activity_id": activity_id})
    # get all individual entities that spend more than $5000 on lobbying activities
    # - filing_id, amend_id, filing_start are the report of person form's
    # - filer_id, type, org_name, org_city, org_st are the org's
    # - lby_actvty is the activity
    # - ls is the legislative session
    individuals = connection.execute(sqlalchemy.text("""
        WITH individual AS (
            SELECT filing_id, amend_id, from_date filing_start, filer_id, entity_cd type,
                CONCAT(filer_namf, filer_naml) org_name, firm_city org_city, firm_st org_st, lby_actvty,
                IF(SUBSTRING_INDEX(SUBSTRING_INDEX(from_date, '/', -1), ' ', 1) % 2 = 0,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(from_date, '/', -1), ' ', 1) - 1,
                    SUBSTRING_INDEX(SUBSTRING_INDEX(from_date, '/', -1), ' ', 1)) ls,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) amendment_rank
            FROM CalAccess.CVR_LOBBY_DISCLOSURE_CD
            WHERE form_type = 'F645'
        )
        SELECT *
        FROM individual
        WHERE amendment_rank = 1
    """)).fetchall()
    for ind in individuals:
        # get information to fill individual lobbying
        filing_id = ind.filing_id
        amend_id = ind.amend_id
        filing_start = ind.filing_start
        filer_id = ind.filer_id
        org_name = ind.org_name.upper().strip()
        city = ind.org_city.upper() if ind.org_city else None
        state = ind.org_st.upper() if ind.org_st else None
        activity = ind.lby_actvty
        ls = ind.ls
        # fix for if they entered city and state together
        if city is not None:
            split_city = city.split(', ')
            if len(split_city) == 2:
                city, state = split_city
        # converting to string to datetime
        filing_start = datetime.strptime(filing_start, "%m/%d/%Y %I:%M:%S %p")
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
            if filer_id is not None:
                connection.execute(sqlalchemy.text("""
                    INSERT INTO PWDev.filer_id (organization_id, filer_id)
                    VALUES (:org_id, :filer_id)
                """), {"org_id": organization_id, "filer_id": filer_id})
            # check if it's a lobbying firm, insert if so
            if type == 'FRM':
                connection.execute(sqlalchemy.text("""
                    INSERT IGNORE INTO PWDev.lobbying_firm (organization_id)
                    VALUES (:oid)
                """), {"oid": organization_id})
            # check if it's a lobbyist employer, insert if so
            elif type in ['LEM', 'LCO']:
                connection.execute(sqlalchemy.text("""
                    INSERT IGNORE INTO PWDev.lobbyist_employer (organization_id)
                    VALUES (:oid)
                """), {"oid": organization_id})
            added_orgs[(org_name, filer_id, type)] = organization_id
        # insert the activity
        activity_id = connection.execute(sqlalchemy.text("""
            INSERT INTO PWDev.activity (activity, filing_id, amendment_id)
            VALUES (:activity, :filing_id, :amend_id)
        """), {"activity": activity, "filing_id": filing_id, "amend_id": amend_id}).lastrowid
        # insert the lobbying relation
        connection.execute(sqlalchemy.text("""
            INSERT INTO PWDev.individual_lobbying (organization_id, activity_id, filing_date, legislative_session)
            VALUES (:organization_id, :activity_id, :filing_start, :ls)
        """), {"organization_id": organization_id, "activity_id": activity_id, "filing_start": filing_start, "ls": ls})
added_orgs = {str(key): value for key, value in added_orgs.items()}
with open("lobbying_orgs.json", "w") as file:
    json.dump(added_orgs, file)