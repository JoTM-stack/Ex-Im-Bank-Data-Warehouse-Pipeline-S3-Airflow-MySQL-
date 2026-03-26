import pymysql
import pandas as pd
import math
from src.utils.logger import get_logger
from src.config.settings import DB_CONFIG

logger = get_logger()

# -----------------------------
# CONNECTIONS
# -----------------------------
def get_conn():
    return pymysql.connect(**DB_CONFIG)


def read_dim(query):
    conn = get_conn()
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# -----------------------------
# GENERIC INSERT FUNCTION
# -----------------------------
def insert_df(df, table_name):
    if df.empty:
        logger.warning(f"{table_name} received empty dataframe — skipping")
        return

    conn = None
    cursor = None

    try:
        conn = get_conn()
        cursor = conn.cursor()

        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})"

        data = [
            tuple(None if (isinstance(v, float) and math.isnan(v)) else v for v in row)
            for row in df.itertuples(index=False)
        ]

        cursor.executemany(sql, data)
        conn.commit()
        logger.info(f"{len(data)} rows inserted into {table_name}")

    except Exception as e:
        logger.error(f"Insert failed for {table_name}: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -----------------------------
# LOAD STAGING
# -----------------------------
def load_to_staging(df):
    logger.info(f"Loading {len(df)} rows into staging_export_data")
    insert_df(df, "staging_export_data")


# -----------------------------
# DIMENSION ATTACH FUNCTIONS
# -----------------------------
def attach_country_id(df):
    dim_df = read_dim("SELECT country_id, country_name FROM dim_country")
    dim_df["country_name"] = dim_df["country_name"].str.upper().str.strip()
    df = df.merge(dim_df, left_on="country", right_on="country_name", how="left")
    logger.info(f"Attached country_id — {df['country_id'].notna().sum()} matches")
    return df


def attach_program_id(df):
    dim_df = read_dim("SELECT program_id, program_name FROM dim_program")
    df = df.merge(dim_df, left_on="program", right_on="program_name", how="left")
    logger.info(f"Attached program_id — {df['program_id'].notna().sum()} matches")
    return df


def attach_exporter_id(df):
    dim_df = read_dim("SELECT exporter_id, exporter_name FROM dim_exporter")
    df = df.merge(dim_df, left_on="primary_exporter", right_on="exporter_name", how="left")
    logger.info(f"Attached exporter_id — {df['exporter_id'].notna().sum()} matches")
    return df


# -----------------------------
# LOAD FACT TABLES
# -----------------------------
def load_fact_tables(df):
    logger.info("Attaching dimension keys")
    df = attach_country_id(df)
    df = attach_program_id(df)
    df = attach_exporter_id(df)

    # Approved deals
    approved = df[df["decision"] == "Approved"]
    if not approved.empty:
        approved_df = approved[[
            "unique_identifier", "decision_date", "country_id",
            "program_id", "exporter_id", "approved_declined_amount",
            "disbursed_shipped_amount", "outstanding_exposure_amount",
            "loan_interest_rate"
        ]].rename(columns={
            "unique_identifier": "deal_id",
            "decision_date": "date_id",
            "approved_declined_amount": "approved_amount",
            "disbursed_shipped_amount": "disbursed_amount",
            "outstanding_exposure_amount": "outstanding_amount",
            "loan_interest_rate": "interest_rate"
        })
        approved_df = approved_df.dropna(subset=["country_id", "program_id"])
        approved_df = approved_df.drop_duplicates(subset=["deal_id"])
        logger.info(f"Loading {len(approved_df)} approved deals")
        insert_df(approved_df, "fact_approved_deals")

    # Declined deals
    declined = df[df["decision"] == "Declined"]
    if not declined.empty:
        declined_df = declined[[
            "unique_identifier", "decision_date", "country_id",
            "program_id", "approved_declined_amount"
        ]].rename(columns={
            "unique_identifier": "deal_id",
            "decision_date": "date_id",
            "approved_declined_amount": "requested_amount"
        })
        declined_df = declined_df.dropna(subset=["country_id", "program_id"])
        declined_df = declined_df.drop_duplicates(subset=["deal_id"])
        logger.info(f"Loading {len(declined_df)} declined deals")
        insert_df(declined_df, "fact_declined_deals")

#DB_URL = "mysql+pymysql://root:root123@127.0.0.1:3306/govdata"
#
#
#def load_to_staging(df):
#    from sqlalchemy import create_engine
#    engine = create_engine(DB_URL)
#    df.to_sql(
#        "staging_export_data",
#        con=engine,
#        if_exists="append",
#        index=False,
#        method="multi",
#        chunksize=1000
#    )
#    engine.dispose()
#
#
#def load_fact_tables(df):
#    from sqlalchemy import create_engine
#    engine = create_engine(DB_URL)
#
#    approved = df[df["decision"] == "Approved"]
#    approved_df = approved[[
#        "unique_identifier",
#        "decision_date",
#        "approved_declined_amount",
#        "disbursed_shipped_amount",
#        "loan_interest_rate"
#    ]].rename(columns={
#        "unique_identifier": "deal_id",
#        "approved_declined_amount": "approved_amount",
#        "disbursed_shipped_amount": "disbursed_amount",
#        "loan_interest_rate": "interest_rate"
#    })
#    approved_df.to_sql("fact_approved_deals", con=engine, if_exists="append", index=False, method="multi")
#
#    declined = df[df["decision"] == "Declined"]
#    declined_df = declined[[
#        "unique_identifier",
#        "decision_date",
#        "approved_declined_amount"
#    ]].rename(columns={
#        "unique_identifier": "deal_id",
#        "approved_declined_amount": "requested_amount"
#    })
#    declined_df.to_sql("fact_declined_deals", con=engine, if_exists="append", index=False, method="multi")
#    engine.dispose()
#