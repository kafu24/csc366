import sqlalchemy as sa
import db
from datetime import datetime

engine = db.engine

with engine.begin() as conn:
    # join expenditure data from the CVR_CAMPAIGN_DISCLOSURE_CD and EXPN_CD tables
    print("Loading expenditure data...")
    expenditures = conn.execute(sa.text("""
        SELECT
            c.FILING_ID AS filing_id,
            c.AMEND_ID AS amendment_id,
            c.CMTTE_ID AS committee_id,
            e.EXPN_DATE AS transaction_date,
            e.AMOUNT AS monetary_amount,
            e.EXPN_CODE AS code,
            e.EXPN_DSCR AS description,
            e.EXPN_DSCR AS purpose
        FROM CalAccess.CVR_CAMPAIGN_DISCLOSURE_CD c
        JOIN CalAccess.EXPN_CD e ON c.FILING_ID = e.FILING_ID
        WHERE c.FORM_TYPE = 'F460'
    """)).fetchall()

    print("Expenditure data loaded.")

    insert_stmt = sa.text("""
        INSERT INTO expenditure (
            460_filing_id,
            460_amendment_id,
            committee_id,
            transaction_date,
            monetary_amount,
            code,
            description,
            purpose
        ) VALUES (
            :filing_id,
            :amendment_id,
            :committee_id,
            :transaction_date,
            :monetary_amount,
            :code,
            :description,
            :purpose
        )
    """)
    
    print("Inserting expenditure data...")

    for row in expenditures:
        # convert transaction_date to datetime object if it's not None
        transaction_date = datetime.strptime(row.transaction_date, "%Y-%m-%d") if row.transaction_date else None

        conn.execute(insert_stmt, {
            "filing_id": row.filing_id,
            "amendment_id": row.amendment_id,
            "committee_id": row.committee_id,
            "transaction_date": transaction_date,
            "monetary_amount": row.monetary_amount,
            "code": row.code,
            "description": row.description,
            "purpose": row.purpose,
        })
        print(f"Inserted expenditure {row.filing_id}, {row.amendment_id}, {row.committee_id}, {transaction_date}, {row.monetary_amount}, {row.code}, {row.description}, {row.purpose}")

    print("Expenditure data inserted.")
