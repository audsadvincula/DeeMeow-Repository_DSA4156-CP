import pandas as pd
from datetime import datetime
import re

# ==========================================
#          CLEANING FUNCTIONS
# ==========================================

# ============ GENERAL =================

def convert_columns_to_uppercase(df):
    """Converts all column headers to uppercase."""
    df.columns = [col.upper() for col in df.columns]
    return df

def drop_index_column(df):
    """Drops the index column if it exists."""
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    return df

def remove_duplicates(df):
    if df.duplicated().sum() > 0:
        df = df.drop_duplicates()
    return df

def convert_to_datetime(df):
    """Converts any column with 'date' in the name to datetime objects."""
    for col in df.columns:
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def handle_missing_values(df):
    for col in df.columns:
        if df[col].isnull().any():
            df[col] = df[col].fillna('UNKNOWN')
    return df

# =============== ENTERPRISE ===============

def clean_phone_numbers(df):
    """Removes non-numeric characters from contact_number column."""
    target_col = 'contact_number'
    if target_col in df.columns:
        # Convert to string, replace non-digits, then back to numeric
        df[target_col] = df[target_col].astype(str).str.replace(r'[^0-9]', '', regex=True)
        df[target_col] = pd.to_numeric(df[target_col], errors='coerce')
    return df

def convert_to_datetime(df):
    """Converts any column with 'date' in the name to datetime objects."""
    for col in df.columns:
        if 'date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def clean_country_column(df):
    """Standardizes country names using a predefined mapping."""
    country_corrections = {
        "RÃ©union": "Réunion", 
        "Ã\u0085land Islands": "Åland Islands",
        "Saint BarthÃ©lemy": "Saint Barthélemy", 
        "CuraÃ§ao": "Curaçao",
        "CÃ´te d'Ivoire": "Côte d'Ivoire",
        "Viet Nam": "Vietnam"
    }
    for col in df.columns:
        if 'country' in col.lower():
            df[col] = df[col].replace(country_corrections)
    return df

# =============== BUSINESS ===============

def clean_product_type_column(df):
    product_corrections = {"toolss": "tools"}
    for col in df.columns:
        if 'product_type' in col.lower():
            df[col] = df[col].replace(product_corrections)
    return df

# =============== MARKETING ===============

import pandas as pd
import re

def clean_campaign_df(df):
    """
    Cleans a dataframe whose first column contains messy campaign data
    and returns a dataframe with separated columns:
    campaign_id, campaign_name, campaign_description, discount
    """

    data = df.iloc[:, 0].astype(str)

    # remove leading row numbers
    data = data.str.replace(r"^\d+", "", regex=True)

    # extract campaign_id and discount
    campaign_id = data.str.extract(r"(CAMPAIGN\d+)")
    discount = data.str.extract(r"(\d+\s*(?:%|pct|percent))", flags=re.IGNORECASE)

    # remove extracted parts from text
    cleaned = (
        data
        .str.replace(r"CAMPAIGN\d+", "", regex=True)
        .str.replace(r"\d+\s*(?:%|pct|percent)", "", regex=True)
        .str.strip()
    )

    # split campaign name and description
    campaign_name = (
        cleaned
        .str.extract(r'^([^"]+)')[0]
        .str.replace('"', '', regex=False)
        .str.strip()
    )

    campaign_description = cleaned.str.extract(r'"(.+)"')[0].str.strip()

    # return cleaned dataframe
    return pd.DataFrame({
        "campaign_id": campaign_id[0],
        "campaign_name": campaign_name,
        "campaign_description": campaign_description,
        "discount": discount[0]
    })


def clean_campaign_discount(df):
    """
    Cleans the discount column (e.g. '1%', '10pct', '1percent')
    and converts it to a numeric rate in the SAME column.
    """

    # find discount column (case-insensitive)
    discount_col = None
    for col in df.columns:
        if "discount" in col.lower():
            discount_col = col
            break

    if discount_col is None:
        return df

    df[discount_col] = (
        df[discount_col]
        .astype(str)
        .str.lower()
        .str.replace(r"(pct|percent|%)", "", regex=True)
        .str.strip()
    )

    # convert to numeric rate
    df[discount_col] = pd.to_numeric(df[discount_col], errors="coerce") / 100

    return df


def convert_transaction_date_column(df):
    """
    Generalized: Finds columns with 'date' in the name and converts them.
    (Handles 'transaction_date')
    """
    for col in df.columns:
        if 'transaction_date' in col.lower():
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    return df

def clean_estimated_arrival(df):
    """
    Specific: Removes 'days' from estimated arrival columns and converts to int.
    Handles variations like 'estimated arrival' or 'estimated_arrival'.
    """
    # Identify the column (it might have spaces or underscores)
    target_col = None
    if 'estimated arrival' in df.columns:
        target_col = 'estimated arrival'
    elif 'estimated_arrival' in df.columns:
        target_col = 'estimated_arrival'
        
    if target_col:
        # Create new clean column
        new_col = 'estimated_arrival_days'
        
        # Remove 'days', convert to numeric, fill NaNs with 0, convert to int
        df[new_col] = df[target_col].astype(str).str.replace('days', '', regex=False)
        df[new_col] = pd.to_numeric(df[new_col], errors='coerce').fillna(0).astype(int)
        
        # Drop the original dirty column
        df = df.drop(columns=[target_col])
        
    return df


# =============== CUSTOMER MANAGEMENT ===============
 
def clean_bank_names(df):
    """
    Finds a 'bank' column, inserts space before 'Bank' if missing, and uppercases.
    """
    # 1. Find target column (one-liner)
    target_col = next((col for col in df.columns if 'bank' in col.lower()), None)
    
    if target_col:
        # Insert space using regex, then convert to uppercase
        # Regex: Look for a letter (\w) followed immediately by 'bank', add space between.
        df[target_col] = (
            df[target_col]
            .astype(str)
            .str.replace(r'(\w)(bank)', r'\1 \2', flags=re.IGNORECASE, regex=True)
            .str.upper()
        )
    else:
        print("No column with 'bank' found.")
        
    return df

































