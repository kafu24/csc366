# get lobbyists

# -- get registration for lobbyists
# SELECT *
# FROM (
#     SELECT *,
#            RANK() OVER (PARTITION BY FILING_ID ORDER BY AMEND_ID DESC) AS amendment_rank
#     FROM `CalAccess`.`CVR_REGISTRATION_CD`
#     WHERE FORM_TYPE = 'F604' AND ENTITY_CD = 'LBY'
# ) AS ranked_records
# WHERE amendment_rank = 1
# FIRM_NAME is the employer/firm
# FILING_ID, AMEND_ID is 604 form

# -- get the filer (ignore dupe with diff uid) to confirm match and no dupes
# SELECT DISTINCT XREF_FILER_ID, FILER_ID, FILER_TYPE, STATUS, EFFECT_DT, NAML, NAMF,
#     NAMT, NAMS, ADR1, ADR2, CITY, ST, ZIP4, PHON, FAX, EMAIL
# FROM `CalAccess`.`FILERNAME_CD` 
# WHERE FILER_ID = 'id' -- put id here

# check for duplicates, report error

# check DDDB2016Aug.Person table, set DDDBPid in Person table to reference found id

# -- get the filer (ignore dupe with diff uid) to confirm match and no dupes
# SELECT DISTINCT XREF_FILER_ID, FILER_ID, FILER_TYPE, STATUS, EFFECT_DT, NAML, NAMF,
#     NAMT, NAMS, ADR1, ADR2, CITY, ST, ZIP4, PHON, FAX, EMAIL
# FROM `CalAccess`.`FILERNAME_CD` 
# WHERE FILER_ID = 'id' -- put id here

# add id to FilerID table

# search DDDB2016.Organizations for FIRM_NAME
# -- get registration for firm

# SELECT * FROM `CalAccess`.`CVR_REGISTRATION_CD` 
# WHERE FORM_TYPE = 'F601'
#     AND FILER_NAML = 'org name' -- put order name here
# FILER_NAML is the employer/firm
# FILING_ID, AMEND_ID is 601/603 form (depends on lobbying firm/employer)

# -- get the filer (ignore dupe with diff uid) to confirm match and no dupes
# SELECT DISTINCT XREF_FILER_ID, FILER_ID, FILER_TYPE, STATUS, EFFECT_DT, NAML, NAMF,
#     NAMT, NAMS, ADR1, ADR2, CITY, ST, ZIP4, PHON, FAX, EMAIL
# FROM `CalAccess`.`FILERNAME_CD` 
# WHERE FILER_ID = 'id' -- put id here