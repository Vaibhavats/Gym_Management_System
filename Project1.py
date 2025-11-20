import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import streamlit as st
from db import insert_member, insert_payment, fetch_members, fetch_membership_types, fetch_trainers


# ------------------- Helper Functions -------------------

def capitalize_strings(df):
    """Capitalize all string/object columns and replace 'Nan' with None."""
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip().str.title()
        df[col] = df[col].replace({'Nan': None})
    return df


def format_dates(df, date_cols):
    """Format datetime columns to SQL-friendly YYYY-MM-DD."""
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
    return df


def fill_non_critical(df, critical_cols):
    """Fill non-critical missing columns with None for SQL insertion."""
    non_critical = set(df.columns) - set(critical_cols)
    for col in non_critical:
        df[col] = df[col].apply(lambda x: x if pd.notna(x) else None)
    return df


def drop_missing_critical(df, critical_cols):
    """Drop rows where critical columns (PK/FK) are missing."""
    df = df.dropna(subset=critical_cols)
    df = df.reset_index(drop=True)
    return df


def remove_duplicates(df, subset_cols):
    """Drop duplicate rows based on subset of columns (e.g., primary key)."""
    df = df.drop_duplicates(subset=subset_cols, keep='first')
    df = df.reset_index(drop=True)
    return df


def integrity_check(df, column, valid_values_set):
    """
    Remove rows where 'column' value is not in valid_values_set (for FK constraints).
    Example: remove members whose trainer_id is not in trainers table.
    """
    if column in df.columns:
        df = df[df[column].isin(valid_values_set) | df[column].isna()]  # Keep NaN
        df = df.reset_index(drop=True)
    return df

# ------------------- Clean Functions -------------------

def clean_members(file, valid_trainers=None, valid_memberships=None):
    df = pd.read_csv(file).drop_duplicates()

    # Add 'name' as critical
    critical = ['member_id', 'name', 'membership_type', 'start_date']
    if 'trainer_id' in df.columns:
        critical.append('trainer_id')

    df = drop_missing_critical(df, critical)
    df = fill_non_critical(df, critical)
    df = capitalize_strings(df)
    df = format_dates(df, ['start_date', 'end_date'])

    # Fix gender to single character
    if 'gender' in df.columns:
        df['gender'] = df['gender'].astype(str).str.strip().str[0].str.upper()

    # Fix numeric types
    if 'contact' in df.columns:
        df['contact'] = pd.to_numeric(df['contact'], errors='coerce')
    if 'trainer_id' in df.columns:
        df['trainer_id'] = pd.to_numeric(df['trainer_id'], errors='coerce')

    # Remove duplicates
    df = remove_duplicates(df, ['member_id'])

    # Integrity checks
    if valid_trainers is not None:
        df = df[df['trainer_id'].isin(valid_trainers)]
    if valid_memberships is not None:
        df = df[df['membership_type'].isin(valid_memberships)]

    df = df.reset_index(drop=True)
    return df

    # Remove duplicates
    df = remove_duplicates(df, ['member_id'])

    # Integrity checks
    if valid_trainers is not None:
        df = df[df['trainer_id'].isin(valid_trainers)]
    if valid_memberships is not None:
        df = df[df['membership_type'].isin(valid_memberships)]

    df = df.reset_index(drop=True)
    return df

def clean_trainers(file):
    df = pd.read_csv(file).drop_duplicates()
    df.columns = [col.lower() for col in df.columns]

    if 'trainer_id' in df.columns:
        df['trainer_id'] = pd.to_numeric(df['trainer_id'], errors='coerce').astype('Int64')

    critical = ['trainer_id', 'name', 'specialization']
    df = drop_missing_critical(df, critical)
    df = capitalize_strings(df)
    df = remove_duplicates(df, ['trainer_id'])
    df = fill_non_critical(df, critical)
    df = df.reset_index(drop=True)
    return df

def clean_membership_types(file):
    df = pd.read_csv(file).drop_duplicates()
    critical = ['membership_type', 'price', 'validity_months']
    df = drop_missing_critical(df, critical)
    df = fill_non_critical(df, critical)
    df = capitalize_strings(df)
    df = remove_duplicates(df, ['membership_type'])
    df = df.reset_index(drop=True)
    return df

def clean_payments(file, valid_members=None):
    df = pd.read_csv(file).drop_duplicates()
    critical = ['payment_id']
    df = drop_missing_critical(df, critical)
    df = fill_non_critical(df, critical)
    df = capitalize_strings(df)

    if 'amount' in df.columns:
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    df = format_dates(df, ['payment_date'])
    df = remove_duplicates(df, ['payment_id'])

    # Integrity check for member_id
    if valid_members is not None:
        df = df[df['member_id'].isin(valid_members)]

    df = df.reset_index(drop=True)
    return df

# ------------------- Call Cleaning Functions -------------------

trainers_df = clean_trainers(r"F:\DBMS_PROJECT\trainer_dirty.csv")
membership_df = clean_membership_types(r"F:\DBMS_PROJECT\membership_types_dirty.csv")
members_df = clean_members(
    r"F:\DBMS_PROJECT\members_dirty.csv",
    valid_trainers=set(trainers_df['trainer_id']),
    valid_memberships=set(membership_df['membership_type'])
)
payments_df = clean_payments(
    r"F:\DBMS_PROJECT\payments_dirty.csv",
    valid_members=set(members_df['member_id'])
)

# ------------------- Preview Cleaned Data -------------------
print("Members Cleaned:\n", members_df.head(), "\n")
print("Trainers Cleaned:\n", trainers_df.head(), "\n")
print("Membership Types Cleaned:\n", membership_df.head(), "\n")
print("Payments Cleaned:\n", payments_df.head(), "\n")


user = 'root'
password = 'Kartik10@'
host = 'localhost'
database = 'gym_db'

# Connect to MySQL
conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
cursor = conn.cursor()
print("✅ Connected to MySQL!")

# Create SQLAlchemy engine
password_encoded = quote_plus(password)
engine = create_engine(f"mysql+pymysql://{user}:{password_encoded}@{host}/{database}")
print("✅ SQLAlchemy engine ready!")

# ------------------- Create Tables -------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS Membership_Types (
    membership_type VARCHAR(50) PRIMARY KEY,
    price DECIMAL(10,2),
    validity_months INT
)
""")
conn.commit()

cursor.execute("""
CREATE TABLE IF NOT EXISTS Trainers (
    trainer_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    specialization VARCHAR(50)
)
""")
conn.commit()

cursor.execute("""
CREATE TABLE IF NOT EXISTS Members (
    member_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    age INT,
    gender CHAR(1),
    contact BIGINT,
    membership_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    trainer_id INT,
    FOREIGN KEY (membership_type) REFERENCES Membership_Types(membership_type),
    FOREIGN KEY (trainer_id) REFERENCES Trainers(trainer_id)
)
""")
conn.commit()

cursor.execute("""
CREATE TABLE IF NOT EXISTS Payments (
    payment_id VARCHAR(10) PRIMARY KEY,
    member_id INT,
    amount DECIMAL(10,2),
    payment_date DATE,
    mode VARCHAR(50),
    status VARCHAR(20),
    FOREIGN KEY (member_id) REFERENCES Members(member_id)
)
""")
conn.commit()

print("✅ Tables created successfully!")

# 1️⃣ Membership Types (parent table)
membership_df.to_sql('Membership_Types', con=engine, if_exists='append', index=False)

# 2️⃣ Trainers (parent table)
trainers_df.to_sql('Trainers', con=engine, if_exists='append', index=False)

# 3️⃣ Members (child table)
members_df.to_sql('Members', con=engine, if_exists='append', index=False)

# 4️⃣ Payments (child table)
payments_df.to_sql('Payments', con=engine, if_exists='append', index=False)
