import pandas as pd
from src.load.load import insert_df
import pycountry
import re



def generate_country_code(name):
    if not name:
        return None

    name = name.upper().strip()
    name = re.sub(r"\(.*?\)", "", name).strip()

    try:
        country = pycountry.countries.search_fuzzy(name)[0]
        return country.alpha_3   # ISO code (USA, ZAF, BRA)
    except:
        return name[:3]  # fallback

# -----------------------------
# COUNTRY DIMENSION
# -----------------------------
def load_dim_country(df):
    country_df = df[["country_clean"]].dropna().drop_duplicates()

    # normalize
    country_df["country_clean"] = country_df["country_clean"].str.upper().str.strip()

    # generate country codes
    country_df["country_id"] = country_df["country_clean"].apply(generate_country_code)

    # remove failed mappings
    country_df = country_df.dropna(subset=["country_id"])

    country_df = country_df.rename(columns={
        "country_clean": "country_name"
    })

    country_df = country_df[["country_id", "country_name"]]

    insert_df(country_df, "dim_country")


# -----------------------------
# PROGRAM DIMENSION
# -----------------------------
def load_dim_program(df):
    program_df = df[[
        "program",
        "policy_type"
    ]].dropna().drop_duplicates()

    program_df = program_df.rename(columns={
        "program": "program_name"
    })

    insert_df(program_df, "dim_program")


# -----------------------------
# EXPORTER DIMENSION
# -----------------------------
def load_dim_exporter(df):
    exporter_df = df[[
        "primary_exporter",
        "primary_exporter_city",
        "primary_exporter_state_code",
        "primary_exporter_state_name"
    ]].dropna().drop_duplicates()

    exporter_df = exporter_df.rename(columns={
        "primary_exporter": "exporter_name",
        "primary_exporter_city": "city",
        "primary_exporter_state_code": "state_code",
        "primary_exporter_state_name": "state_name"
    })

    insert_df(exporter_df, "dim_exporter")


# -----------------------------
# DATE DIMENSION
# -----------------------------
def load_dim_date(df):
    date_df = df[["decision_date"]].dropna().drop_duplicates()

    # ensure datetime
    date_df["decision_date"] = pd.to_datetime(date_df["decision_date"])

    date_df["year"] = date_df["decision_date"].dt.year
    date_df["month"] = date_df["decision_date"].dt.month
    date_df["day"] = date_df["decision_date"].dt.day

    date_df = date_df.rename(columns={
        "decision_date": "date_id"
    })

    insert_df(date_df, "dim_date")


# -----------------------------
# MASTER FUNCTION
# -----------------------------
def load_all_dimensions(df):
    load_dim_country(df)
    load_dim_program(df)
    load_dim_exporter(df)
    load_dim_date(df)