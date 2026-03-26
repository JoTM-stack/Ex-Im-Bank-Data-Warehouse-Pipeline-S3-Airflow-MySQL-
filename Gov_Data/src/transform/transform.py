import pandas as pd

COLUMN_MAPPING = {
    "fiscal_year": "fiscal_year",
    "unique_identifier": "unique_identifier",
    "deal_number": "deal_number",
    "decision": "decision",
    "decision_date": "decision_date",
    "effective_date": "effective_date",
    "expiration_date": "expiration_date",
    "brokered": "brokered",
    "deal_cancelled": "deal_cancelled",
    "country": "country",
    "program": "program",
    "policy_type": "policy_type",
    "decision_authority": "decision_authority",

    "primary_export_product_naics_sic_code": "naics_code",
    "product_description": "product_description",
    "term": "term",

    "primary_applicant": "primary_applicant",
    "primary_lender": "primary_lender",
    "primary_exporter": "primary_exporter",
    "primary_exporter_city": "primary_exporter_city",
    "primary_exporter_state_code": "primary_exporter_state_code",
    "primary_exporter_state_name": "primary_exporter_state_name",

    "primary_borrower": "primary_borrower",
    "primary_source_of_repayment_psor": "primary_source_of_repayment",

    "working_capital_delegated_authority": "working_capital_delegated_authority",

    "approved_declined_amount": "approved_declined_amount",
    "disbursed_shipped_amount": "disbursed_shipped_amount",
    "undisbursed_exposure_amount": "undisbursed_exposure_amount",
    "outstanding_exposure_amount": "outstanding_exposure_amount",

    "small_business_authorized_amount": "small_business_authorized_amount",
    "woman_owned_authorized_amount": "woman_owned_authorized_amount",
    "minority_owned_authorized_amount": "minority_owned_authorized_amount",

    "loan_interest_rate": "loan_interest_rate",
    "multiyear_working_capital_extension": "multiyear_working_capital_extension"
}



def clean_column_names(df):
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("-", "_")
        .str.replace("(", "")
        .str.replace(")", "")
    )
    return df


def convert_types(df):
    date_columns = [
        "decision_date",
        "effective_date",
        "expiration_date"
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    numeric_columns = [
        "approved_declined_amount",
        "disbursed_shipped_amount",
        "undisbursed_exposure_amount",
        "outstanding_exposure_amount",
        "loan_interest_rate"
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def handle_missing(df):
    # Drop rows with no identifier
    df = df.dropna(subset=["unique_identifier"])

    # Fill numeric nulls
    df = df.fillna({
        "loan_interest_rate": 0
    })

    return df

def clean_country(df):
        df["country_clean"] = (
            df["country"]
            .str.upper()
            .str.strip()
            .str.replace(r"\(.*\)", "", regex=True)
        )
        return df

def transform_chunk(df):
    df = clean_column_names(df)

    # Apply mapping
    df = df.rename(columns=COLUMN_MAPPING)

    # Keep ONLY columns that exist in DB
    df = df[list(COLUMN_MAPPING.values())]

    df = convert_types(df)
    df = handle_missing(df)

    df = clean_country(df)

    # ADD THIS (FIXES YOUR ERROR)
    df["country"] = (
        df["country"]
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(r"\(.*?\)", "", regex=True)
        .str.strip()
    )

    return df