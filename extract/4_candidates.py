import sqlalchemy
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
import db
import re

district_codes = {
    None: "N/A",
    "0": "N/A",
    "17001": "01",
    "17002": "13",
    "17003": "24",
    "17004": "35",
    "17005": "46",
    "17006": "57",
    "17007": "68",
    "17008": "79",
    "17009": "02",
    "17010": "05",
    "17011": "04",
    "17013": "06",
    "17014": "07",
    "17015": "08",
    "17016": "19",
    "17017": "10",
    "17018": "11",
    "17019": "12",
    "17020": "14",
    "17021": "15",
    "17022": "16",
    "17023": "17",
    "17024": "18",
    "17026": "20",
    "17027": "21",
    "17028": "22",
    "17029": "23",
    "17030": "25",
    "17031": "26",
    "17032": "27",
    "17033": "28",
    "17034": "29",
    "17035": "30",
    "17036": "31",
    "17037": "32",
    "17038": "33",
    "17039": "34",
    "17040": "36",
    "17041": "37",
    "17042": "38",
    "17043": "39",
    "17044": "40",
    "17045": "41",
    "17046": "42",
    "17047": "43",
    "17048": "44",
    "17049": "45",
    "17050": "47",
    "17051": "48",
    "17052": "49",
    "17053": "50",
    "17054": "51",
    "17055": "52",
    "17056": "53",
    "17057": "54",
    "17058": "55",
    "17059": "56",
    "17060": "03",
    "17061": "59",
    "17062": "60",
    "17063": "61",
    "17064": "62",
    "17065": "63",
    "17066": "64",
    "17067": "65",
    "17068": "66",
    "17069": "67",
    "17070": "69",
    "17071": "70",
    "17072": "71",
    "17073": "72",
    "17074": "73",
    "17075": "74",
    "17076": "75",
    "17077": "76",
    "17078": "77",
    "17079": "78",
    "17080": "80",
    "17081": "09",
    "17090": "58",
    "17012": "N/A",
    "17082": "N/A",
    "17025": "N/A"
}
party_codes = {
    None: 'N/A',
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

office_codes = {
    None: "N/A",
    "0": "N/A",
    "30001": "PRESIDENT",
    "30002": "GOVERNOR",
    "30003": "LIEUTENANT GOVERNOR",
    "30004": "SECRETARY OF STATE",
    "30005": "CONTROLLER",
    "30006": "TREASURER",
    "30007": "ATTORNEY GENERAL",
    "30008": "SUPERINTENDENT OF PUBLIC INSTRUCTION",
    "30009": "MEMBER BOARD OF EQUALIZATION",
    "30010": "OXNARD HARBOR COMMISSIONER",
    "30011": "CITY CONTROLLER",
    "30012": "STATE SENATE",
    "30013": "ASSEMBLY",
    "30014": "INSURANCE COMMISSIONER",
    "30015": "JUDGE",
    "30016": "BOARD MEMBER",
    "30017": "TAX COLLECTOR",
    "30018": "TRUSTEE",
    "30019": "SUPERVISOR",
    "30020": "SHERIFF",
    "30021": "CORONER",
    "30022": "MARSHALL",
    "30023": "CITY CLERK",
    "30024": "SCHOOL BOARD",
    "30025": "HARBOR COMMISSIONER",
    "30026": "DISTRICT ATTORNEY",
    "30027": "COUNTY CLERK",
    "30028": "AUDITOR",
    "30029": "MAYOR",
    "30030": "CITY ATTORNEY",
    "30031": "DEMOCRATIC COUNTY CENTRAL COMMITTEE",
    "30032": "TOWN COUNCIL",
    "30033": "ASSESSOR",
    "30034": "CITY TREASURER",
    "30035": "CITY COUNCIL",
    "30036": "COMMISSIONER",
    "30037": "REPUBLICAN COUNTY CENTRAL COMMITTEE",
    "30038": "DIRECTOR",
    "30039": "DIRECTOR OF ZONE 7",
    "30040": "COMMUNITY COLLEGE BOARD",
    "30041": "POLICE CHIEF",
    "30042": "CHIEF OF POLICE",
    "30043": "CENTRAL COMMITTEE",
    "30044": "BOARD OF EDUCATION",
    "30045": "BOARD OF DIRECTORS",
    "30046": "COLLEGE BOARD",
    "30047": "BART BOARD DIRECTOR",
    "30048": "BOARD OF TRUSTEES",
    "30049": "IRRIGATION",
    "30050": "WATER BOARD",
    "30051": "COMMUNITY PLANNING GROUP",
    "30052": "BOARD OF SUPERVISORS",
    "30053": "SUPERIOR COURT JUDGE",
    "30054": "DISTRICT ATTORNEY/PUBLIC DEFENDER",
    "30055": "MEASURE",
    "30056": "CITY PROSECUTOR",
    "30057": "SUPREME COURT JUDGE",
    "30058": "PUBLIC EMPLOYEES RETIREMENT BOARD",
    "30059": "APPELLATE COURT JUDGE",
    "50001": "Ag",
    "50002": "Assembly",
    "50003": "Assessor",
    "50004": "Assessor/Clerk/Recorder",
    "50005": "Assessor/County Clerk/Recorder",
    "50006": "Assessor/Recorder",
    "50007": "Associate Justice",
    "50008": "Auditor",
    "50009": "Auditor/Controller",
    "50010": "Auditor/Controller/Clerk/Recorder",
    "50011": "Auditor/Controller/Recorder",
    "50012": "Auditor/Controller/Treasurer/Tax Collector",
    "50013": "Auditor/Recorder",
    "50014": "Board Member",
    "50015": "Board Of Director",
    "50016": "Board Of Supervisor",
    "50017": "Boe",
    "50018": "Chief Justice",
    "50019": "City",
    "50020": "City Attorney",
    "50021": "City Auditor",
    "50022": "City Clerk",
    "50023": "City Council",
    "50024": "City Of Los Angeles",
    "50025": "City Of South El Monte",
    "50026": "City Prosecutor",
    "50027": "City Treasurer",
    "50028": "Clerk/Auditor",
    "50029": "Clerk/Record/Public Admin",
    "50030": "Clerk/Recorder",
    "50031": "Clerk/Recorder/Registar",
    "50032": "Clerk/Recorder/Registrar",
    "50033": "Commissioner",
    "50034": "Controller",
    "50035": "Costa Mesa",
    "50036": "Council Member",
    "50037": "County Clerk",
    "50038": "County Clerk/Auditor",
    "50039": "County Clerk/Auditor/Controller",
    "50040": "County Clerk/Recorder",
    "50041": "County Clerk/Recorder/Assessor",
    "50042": "County Clerk/Recorder/Public Admin",
    "50043": "Democratic County Central Committee",
    "50044": "Director",
    "50045": "District Attorney",
    "50046": "District Attorney/Public Administrator",
    "50047": "Gccc",
    "50048": "Governor",
    "50049": "Harbor Commissioner",
    "50050": "Ic",
    "50051": "Irrigation Dist",
    "50052": "Judge",
    "50053": "Justice",
    "50054": "Legislature",
    "50055": "Lieutenant Governor",
    "50056": "Mayor",
    "50057": "N/A",
    "50058": "Placentia",
    "50059": "Public Administrator",
    "50060": "Public Administrator/Guardian",
    "50061": "Rent Stabilization Board",
    "50062": "Republican Central Committee",
    "50063": "San Francisco Dccc",
    "50064": "Sanger",
    "50065": "School Board",
    "50066": "Secretary Of State",
    "50067": "Senator",
    "50068": "Sheriff",
    "50069": "Sheriff/Coroner",
    "50070": "Sheriff/Coroner/Marshall",
    "50071": "Sheriff/Coroner/Public Administrator",
    "50072": "Solana Beach",
    "50073": "Superintendent",
    "50074": "Supervisor",
    "50075": "Supt Of Schools",
    "50076": "Tax Collector",
    "50077": "Town Council",
    "50078": "Treasurer",
    "50079": "Treasurer/Tax Collector",
    "50080": "Treasurer/Tax Collector/Clerk",
    "50081": "Treasurer/Tax Collector/Public Administrator",
    "50082": "Treasurer/Tax Collector/Public Administrator/County Clerk",
    "50083": "Treasurer/Tax Collector/Recorder",
    "50084": "Trustee",
    "50085": "Weed Recreation Board Member"
}

jurisdiction_codes = {
    None: "N/A",
    "0": "N/A",
    "40501": "LOCAL",
    "40502": "STATE",
    "40503": "COUNTY",
    "40504": "MULTI-COUNTY",
    "40505": "CITY",
    "40507": "SUPERIOR COURT JUDGE"
}

election_codes = {
    None: "N/A",
    "0": "N/A",
    "3001": "GENERAL",
    "3002": "PRIMARY",
    "3003": "RECALL",
    "3004": "SPECIAL ELECTION",
    "3005": "OFFICEHOLDER",
    "3006": "SPECIAL RUNOFF",
    "3007": "UNKNOWN"
}

with db.engine.begin() as connection:
    candidates = connection.execute(sqlalchemy.text("""
        WITH ranked_intentions AS (
            SELECT filing_id, amend_id, filer_id, cand_naml last, cand_namf first, can_namm middle, cand_namt title,
            cand_nams suffix, office_cd, offic_dscr office, agency_nam agency, juris_cd, juris_dscr jurisdiction,
            yr_of_elec election_year, elec_type election_type, party_cd, party, district_cd, dist_no district_num,
                RANK() OVER (PARTITION BY filing_id ORDER BY amend_id DESC) amendment_rank
            FROM CalAccess.F501_502_CD
        )
        SELECT *
        FROM ranked_intentions ri
        WHERE ri.amendment_rank = 1
                                                    LIMIT 100
    """)).fetchall()
    for cand in candidates:
        # get information to fill district, office, and election
        office = cand.office if cand.office else None
        office_cd = cand.office_cd if cand.office_cd else None
        # fix for if they don't fill office, but fill out type
        if office is None:
            office = office_codes[office]
        agency = cand.agency if cand.agency else None
        jurisdiction = cand.jurisdiction if cand.jurisdiction else None
        juris_cd = cand.juris_cd if cand.juris_cd else None
        if jurisdiction is None:
            jurisdiction = jurisdiction_codes[jurisdiction]
        election_year = cand.election_year if cand.election_year else None
        election_type = cand.election_type if cand.election_type else None
        election_type = election_codes[election_type]
        district_num = cand.district_num if cand.district_num else None
        district_cd = cand.district_cd if cand.district_cd else None
        if district_num is None:
            district_num = district_codes[district_cd]
        office_id = None
        district_id = None
        # insert district
        if district_num not in ["N/A", "Unknown"] and office in ['ASSEMBLY', 'STATE SENATE', 'MEMBER BOARD OF EQUALIZATION']:
            district_id = connection.execute(sqlalchemy.text("""
                SELECT _id
                FROM PWProd.district 
                WHERE chamber = :office AND number = :district_num
            """), {"office": office, "district_num": district_num}).scalar_one_or_none()
            if district_id is None:
                district_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO PWProd.district (chamber, number)
                    VALUES (:office, :district_num)
                """), {"office": office, "district_num": district_num}).lastrowid
            # insert office with district
            office_id = connection.execute(sqlalchemy.text("""
                SELECT _id
                FROM PWProd.office 
                WHERE title = :office AND agency = :agency AND jurisdiction = :jurisdiction
            """), {"office": office, "agency": agency, "jurisdiction": jurisdiction}).scalar_one_or_none()
            if office_id is None: 
                office_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO PWProd.office (title, agency, jurisdiction, district_id)
                    VALUES (:office, :agency, :jurisdiction, :district_id)
                """), {"office": office, "agency": agency, "jurisdiction": jurisdiction, "district_id": district_id}).lastrowid
            # insert election
            election_id = connection.execute(sqlalchemy.text("""
                INSERT IGNORE INTO PWProd.election (office_id, type, year)
                VALUES (:office_id, :election_type, :election_year)
            """), {"office_id": office_id, "election_type": election_type, "election_year": election_year}).lastrowid
        else:
            # insert office, no district
            office_id = connection.execute(sqlalchemy.text("""
                SELECT _id
                FROM PWProd.office 
                WHERE title = :office AND agency = :agency AND jurisdiction = :jurisdiction
            """), {"office": office, "agency": agency, "jurisdiction": jurisdiction}).scalar_one_or_none()
            if office_id is None: 
                office_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO PWProd.office (title, agency, jurisdiction)
                    VALUES (:office, :agency, :jurisdiction)
                """), {"office": office, "agency": agency, "jurisdiction": jurisdiction}).lastrowid
            # insert election
            election_id = connection.execute(sqlalchemy.text("""
                INSERT IGNORE INTO PWProd.election (office_id, type, year)
                VALUES (:office_id, :election_type, :election_year)
            """), {"office_id": office_id, "election_type": election_type, "election_year": election_year}).lastrowid
        # get information to fill person and candidate
        filer_id = cand.filer_id
        first = cand.first.upper().strip() if cand.first else None
        middle = cand.middle.upper().strip() if cand.middle else None
        last = cand.last.upper().strip() if cand.last else None
        title = cand.title if cand.title else None
        suffix = cand.suffix if cand.suffix else None
        party = cand.party if cand.party else None
        party_cd = cand.party_cd if cand.party_cd else None
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
        result = connection.execute(sqlalchemy.text("""
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
                candidate_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO PWProd.person (DDDBPid, first, middle, last, title, suffix)
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
                    candidate_id = connection.execute(sqlalchemy.text("""
                        INSERT INTO PWProd.person (DDDBPid, first, middle, last, title, suffix)
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
                        candidate_id = connection.execute(sqlalchemy.text("""
                            INSERT INTO PWProd.person (first, middle, last, title, suffix)
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
                candidate_id = connection.execute(sqlalchemy.text("""
                    INSERT INTO PWProd.person (first, middle, last, title, suffix)
                    VALUES (:first, :middle, :last, :title, :suffix)
                """), {"first": first, "middle": middle, "last": last, "title": title, "suffix": suffix}).lastrowid
            # classify as candidate and associate with filer id
            connection.execute(sqlalchemy.text("""
                INSERT IGNORE INTO PWProd.candidate (person_id, party)
                VALUES (:pid, :party)
            """), {"pid": candidate_id, "party": party})
            if filer_id != "":
                connection.execute(sqlalchemy.text("""
                    INSERT INTO PWProd.filer_id (person_id, filer_id)
                    VALUES (:person_id, :filer_id)
                """), {"person_id": candidate_id, "filer_id": filer_id})
        else:
            # TODO: resolve this, probably take most filled/latest
            if first != current_first or middle != current_middle or last != current_last:
                print("conflicting name:", first, middle, last, current_first, current_middle, current_last)
            # update name info if necessary
            if first is not None and current_first is None:
                # print("updated name:", current_first, first)
                connection.execute(sqlalchemy.text("""
                    UPDATE PWProd.person
                    SET first = :first
                    WHERE _id = :pid
                """), {"pid": person_id, "first": first})
            if middle is not None and current_middle is None:
                # print("updated name:", current_middle, middle)
                connection.execute(sqlalchemy.text("""
                    UPDATE PWProd.person
                    SET middle = :middle
                    WHERE _id = :pid
                """), {"pid": person_id, "middle": middle})
            if last is not None and current_last is None:
                # print("updated name:", current_last, last)
                connection.execute(sqlalchemy.text("""
                    UPDATE PWProd.person
                    SET last = :last
                    WHERE _id = :pid
                """), {"pid": person_id, "last": last})
            # if they filed with another party
            if party not in current_parties:
                connection.execute(sqlalchemy.text("""
                    INSERT IGNORE INTO PWProd.candidate (person_id, party)
                    VALUES (:pid, :party)
                """), {"pid": candidate_id, "party": party})
        filing_id = cand.filing_id if cand.filing_id else None
        amend_id = cand.amend_id if cand.amend_id else None
        # insert the running relation
        connection.execute(sqlalchemy.text("""
            INSERT IGNORE INTO PWProd.running (candidate_id, office_id, 501_filing_id, 501_amendment_id)
            VALUES (:candidate_id, :office_id, :filing_id, :amend_id)
        """), {"candidate_id": candidate_id, "office_id": office_id, "filing_id": filing_id, "amend_id": amend_id})
