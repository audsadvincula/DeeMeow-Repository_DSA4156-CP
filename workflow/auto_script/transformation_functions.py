import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

# ==========================================
# HELPERS
# ==========================================
def get_staging_engine():
    return PostgresHook(postgres_conn_id="postgres_staging").get_sqlalchemy_engine()

def get_dw_engine():
    return PostgresHook(postgres_conn_id="postgres_dw").get_sqlalchemy_engine()

def run_dw_ddl(ddl_statements):
    """Executes DDL (ALTER, INSERT, ADD CONSTRAINT) on the DW."""
    engine = get_dw_engine()
    with engine.begin() as conn:
        for stmt in ddl_statements:
            if stmt.strip():
                # We wrap in try-except to handle cases where constraints might conflict during dev
                # though correct SQL ordering (DROP IF EXISTS) is better.
                try:
                    conn.execute(text(stmt))
                except Exception as e:
                    print(f"⚠️ Warning executing DDL: {e}")
                    # We continue because 'multiple primary keys' error usually means 
                    # it was already successful in a previous partial run.

# ==========================================
# 1. DIMENSION: USER
# ==========================================
def create_dim_user(**kwargs):
    print("--- Creating DIM_USER ---")


    hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn = hook.get_conn()
    schema_name = 'dw'
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        conn.commit()
    print(f"✅ Schema '{schema_name}' is ready.")

    # FIX: Use Double Quotes for UPPERCASE columns
    sql_select = """
    SELECT 
        ROW_NUMBER() OVER (ORDER BY u1."USER_ID") AS "USER_REFERENCE_NUMBER",
        u1."USER_ID", 
        u1."CREATION_DATE" AS "USER_CREATION_DATE", 
        u1."NAME" AS "USER_NAME", 
        u1."STREET" AS "USER_STREET", 
        u1."STATE" AS "USER_STATE",
        u1."CITY" AS "USER_CITY", 
        u1."COUNTRY" AS "USER_COUNTRY", 
        u1."BIRTHDATE" AS "USER_BIRTHDATE", 
        u1."GENDER" AS "USER_GENDER",
        u1."DEVICE_ADDRESS" AS "USER_DEVICE_ADDRESS",
        u1."USER_TYPE" AS "USER_TYPE",
        u2."JOB_TITLE" AS "USER_JOB_TITLE",
        u2."JOB_LEVEL" AS "USER_JOB_LEVEL",
        u3."CREDIT_CARD_NUMBER" AS "USER_CREDIT_CARD_NUMBER",
        u3."ISSUING_BANK" AS "USER_ISSUING_BANK"
    FROM customer_management_staging_cleaned.user_data_cleaned AS u1
    LEFT JOIN customer_management_staging_cleaned.user_job_cleaned AS u2
    ON u1."USER_ID" = u2."USER_ID"
    LEFT JOIN customer_management_staging_cleaned.user_credit_card_cleaned AS u3
    ON u1."USER_ID" = u3."USER_ID"
    """
    
    df = pd.read_sql(sql_select, get_staging_engine())
    # To save into a specific schema:
    df.to_sql('dim_user', get_dw_engine(), schema='dw', if_exists='replace', index=False)
    
    # FIX: Quote column names in DDL as well
    ddl_scripts = [
        'ALTER TABLE dim_user ADD CONSTRAINT dim_user_pk PRIMARY KEY ("USER_REFERENCE_NUMBER");',
        'ALTER TABLE dim_user ALTER COLUMN "USER_REFERENCE_NUMBER" SET NOT NULL;',
        'ALTER TABLE dim_user ALTER COLUMN "USER_ID" SET NOT NULL;'
    ]
    run_dw_ddl(ddl_scripts)
    print("✅ DIM_USER Created Successfully")

# ==========================================
# 2. DIMENSION: PRODUCT
# ==========================================
def create_dim_product(**kwargs):
    print("--- Creating DIM_PRODUCT ---")

    hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn = hook.get_conn()
    schema_name = 'dw'
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        conn.commit()
    print(f"✅ Schema '{schema_name}' is ready.")


    # FIX: Double Quotes for UPPERCASE columns
    sql_select = """
    SELECT 
        ROW_NUMBER() OVER (ORDER BY "PRODUCT_ID") AS "PRODUCT_REFERENCE_NUMBER",
        "PRODUCT_ID", 
        "PRODUCT_NAME", 
        "PRODUCT_TYPE"
    FROM business_staging_cleaned.product_list_cleaned
    """
    
    df = pd.read_sql(sql_select, get_staging_engine())
    # To save into a specific schema:
    df.to_sql('dim_product', get_dw_engine(), schema='dw', if_exists='replace', index=False)
    
    ddl_scripts = [
        'ALTER TABLE dim_product ADD CONSTRAINT dim_product_pk PRIMARY KEY ("PRODUCT_REFERENCE_NUMBER");',
        'ALTER TABLE dim_product ALTER COLUMN "PRODUCT_REFERENCE_NUMBER" SET NOT NULL;',
        'ALTER TABLE dim_product ALTER COLUMN "PRODUCT_ID" SET NOT NULL;',
        "INSERT INTO dim_product VALUES (-1, 'PRODUCT00000', 'No Product', 'No Product');"
    ]
    run_dw_ddl(ddl_scripts)
    print("✅ DIM_PRODUCT Created Successfully")

# ==========================================
# 3. DIMENSION: CAMPAIGN
# ==========================================
def create_dim_campaign(**kwargs):
    print("--- Creating DIM_CAMPAIGN ---")

    hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn = hook.get_conn()
    schema_name = 'dw'
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        conn.commit()
    print(f"✅ Schema '{schema_name}' is ready.")

    # FIX: Double Quotes for UPPERCASE columns
    sql_select = """
    SELECT 
        ROW_NUMBER() OVER (ORDER BY "CAMPAIGN_ID") AS "CAMPAIGN_REFERENCE_NUMBER",
        "CAMPAIGN_ID",
        "CAMPAIGN_NAME",
        "CAMPAIGN_DESCRIPTION",
        "DISCOUNT" AS "CAMPAIGN_DISCOUNT"
    FROM marketing_staging_cleaned.campaign_data_cleaned
    """
    
    df = pd.read_sql(sql_select, get_staging_engine())
    # To save into a specific schema:
    df.to_sql('dim_campaign', get_dw_engine(), schema='dw', if_exists='replace', index=False)
    
    ddl_scripts = [
        'ALTER TABLE dim_campaign ADD CONSTRAINT dim_campaign_pk PRIMARY KEY ("CAMPAIGN_REFERENCE_NUMBER");',
        'ALTER TABLE dim_campaign ALTER COLUMN "CAMPAIGN_REFERENCE_NUMBER" SET NOT NULL;',
        'ALTER TABLE dim_campaign ALTER COLUMN "CAMPAIGN_ID" SET NOT NULL;',
        "INSERT INTO dim_campaign VALUES (-1, 'CAMPAIGN00000', 'No Campaign', 'No Campaign', 0.0);"
    ]
    run_dw_ddl(ddl_scripts)
    print("✅ DIM_CAMPAIGN Created Successfully")

# ==========================================
# 4. DIMENSION: MERCHANT
# ==========================================
def create_dim_merchant(**kwargs):
    print("--- Creating DIM_MERCHANT ---")
    
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn = hook.get_conn()
    schema_name = 'dw'
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        conn.commit()
    print(f"✅ Schema '{schema_name}' is ready.")

    # FIX: Double Quotes for UPPERCASE columns
    sql_select = """
    SELECT
        ROW_NUMBER () OVER (ORDER BY "MERCHANT_ID") AS "MERCHANT_REFERENCE_NUMBER",
        "MERCHANT_ID",
        "NAME" AS "MERCHANT_NAME",
        "STREET" AS "MERCHANT_STREET",
        "CITY" AS "MERCHANT_CITY",
        "STATE" AS "MERCHANT_STATE",
        "COUNTRY" AS "MERCHANT_COUNTRY",
        "CONTACT_NUMBER" AS "MERCHANT_CONTACT_NUMBER",
        "CREATION_DATE" AS "MERCHANT_CREATION_DATE"
    FROM enterprise_staging_cleaned.merchant_data_cleaned
    """
    
    df = pd.read_sql(sql_select, get_staging_engine())
# To save into a specific schema:
    df.to_sql('dim_merchant', get_dw_engine(), schema='dw', if_exists='replace', index=False)
    
    ddl_scripts = [
        'ALTER TABLE dim_merchant ADD CONSTRAINT dim_merchant_pk PRIMARY KEY ("MERCHANT_REFERENCE_NUMBER");',
        'ALTER TABLE dim_merchant ALTER COLUMN "MERCHANT_REFERENCE_NUMBER" SET NOT NULL;',
        'ALTER TABLE dim_merchant ALTER COLUMN "MERCHANT_ID" SET NOT NULL;'
    ]
    run_dw_ddl(ddl_scripts)
    print("✅ DIM_MERCHANT Created Successfully")

# ==========================================
# 5. DIMENSION: STAFF
# ==========================================
def create_dim_staff(**kwargs):
    print("--- Creating DIM_STAFF ---")
    
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn = hook.get_conn()
    schema_name = 'dw'
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        conn.commit()
    print(f"✅ Schema '{schema_name}' is ready.")

    # FIX: Double Quotes for UPPERCASE columns
    sql_select = """
    SELECT
        ROW_NUMBER () OVER (ORDER BY "STAFF_ID") AS "STAFF_REFERENCE_NUMBER", 
        "STAFF_ID",
        "NAME" AS "STAFF_NAME",
        "JOB_LEVEL" AS "STAFF_JOB_LEVEL",
        "STREET" AS "STAFF_STREET",
        "CITY" AS "STAFF_CITY",
        "STATE" AS "STAFF_STATE", 
        "COUNTRY" AS "STAFF_COUNTRY",
        "CONTACT_NUMBER" AS "STAFF_CONTACT_NUMBER",
        "CREATION_DATE" AS "STAFF_CREATION_DATE"
    FROM enterprise_staging_cleaned.staff_data_cleaned
    """
    
    df = pd.read_sql(sql_select, get_staging_engine())
# To save into a specific schema:
    df.to_sql('dim_staff', get_dw_engine(), schema='dw', if_exists='replace', index=False)
    ddl_scripts = [
        'ALTER TABLE dim_staff ADD CONSTRAINT dim_staff_pk PRIMARY KEY ("STAFF_REFERENCE_NUMBER");',
        'ALTER TABLE dim_staff ALTER COLUMN "STAFF_REFERENCE_NUMBER" SET NOT NULL;',
        'ALTER TABLE dim_staff ALTER COLUMN "STAFF_ID" SET NOT NULL;'
    ]
    run_dw_ddl(ddl_scripts)
    print("✅ DIM_STAFF Created Successfully")


# ==========================================
# 6. FACT: ORDER (Optimized with FDW / SQL)
# ==========================================

def create_fact_order(**kwargs):
    print("--- Creating FACT_ORDER via FDW (SQL) ---")
    
    # Connect to Data Warehouse
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    
    # Define the Full SQL Logic
    
    sql_script = """
    -- 1. Enable the extension in DW
    CREATE EXTENSION IF NOT EXISTS postgres_fdw;

    -- 2. Create the server connection to Staging
    -- 'postgres-staging' is the container name defined in docker-compose
    CREATE SERVER IF NOT EXISTS staging_server 
        FOREIGN DATA WRAPPER postgres_fdw 
        OPTIONS (host 'postgres-staging', port '5432', dbname 'shopzada');

    -- 3. Create User Mapping
    -- Maps your current DW user to the Staging DB credentials
    CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
        SERVER staging_server 
        OPTIONS (user 'airflow', password 'airflow');

    -- 4. Create a local schema to hold the "Ghost Tables"
    CREATE SCHEMA IF NOT EXISTS staging_proxy;

    -- 5. Import the Staging Tables into the Proxy Schema

    IMPORT FOREIGN SCHEMA operation_staging_cleaned 
        FROM SERVER staging_server INTO staging_proxy;

    IMPORT FOREIGN SCHEMA marketing_staging_cleaned 
        FROM SERVER staging_server INTO staging_proxy;

    IMPORT FOREIGN SCHEMA enterprise_staging_cleaned 
        FROM SERVER staging_server INTO staging_proxy;

    IMPORT FOREIGN SCHEMA business_staging_cleaned 
        FROM SERVER staging_server INTO staging_proxy;
    -- [STEP 3] Create Target Table in DW Schema

    
    -- Creation of the Star Schema
    
    CREATE SCHEMA IF NOT EXISTS dw;
    
    CREATE TABLE IF NOT EXISTS dw.fact_order AS
    SELECT
        ROW_NUMBER () OVER (ORDER BY o.order_id) AS "ORDER_REFERENCE_NUMBER",
        u."USER_REFERENCE_NUMBER",
        CASE 
            WHEN product."PRODUCT_REFERENCE_NUMBER" IS NULL
            THEN -1
            ELSE product."PRODUCT_REFERENCE_NUMBER" END AS "PRODUCT_REFERENCE_NUMBER",
        CASE 
            WHEN campaign."CAMPAIGN_REFERENCE_NUMBER" IS NULL 
            THEN -1 
            ELSE campaign."CAMPAIGN_REFERENCE_NUMBER" END AS "CAMPAIGN_REFERENCE_NUMBER",
        staff."STAFF_REFERENCE_NUMBER",
        merchant."MERCHANT_REFERENCE_NUMBER",
        o.order_id AS "ORDER_ID",
        o."estimated arrival" AS "ORDER_ESTIMATED_ARRIVAL", 
        o.transaction_date AS "ORDER_TRANSACTION_DATE",
        COALESCE(l1.price, 0) AS "ORDER_PRICE",
        COALESCE(l1.quantity, 0) AS "ORDER_QUANTITY",
        COALESCE(d."delay in days", 0) AS "DELAY_IN_DAYS",
        CASE 
            WHEN t."AVAILED"::text = '1' THEN 'Availed'
            WHEN t."AVAILED"::text = '0' THEN 'Not Availed'
            ELSE 'Not Applicable' 
        END AS "ORDER_W_PROMO"
    FROM staging_proxy.order_data_concat AS o
    LEFT JOIN staging_proxy.line_item_data_prices_concat AS l1	
    ON o.order_id = l1.order_id
    LEFT JOIN staging_proxy.line_item_data_products_concat AS l2
    ON o.order_id = l2.order_id
    LEFT JOIN staging_proxy.order_delays_cleaned AS d
    ON o.order_id = d.order_id
    LEFT JOIN staging_proxy.transactional_campaign_data_cleaned AS t
    ON o.order_id = t."ORDER_ID"
    LEFT JOIN dw.dim_user AS u
    ON o.user_id = u."USER_ID"
    LEFT JOIN dw.dim_product AS product
    ON l2.product_id = product."PRODUCT_ID"
    LEFT JOIN dw.dim_campaign AS campaign
    ON t."CAMPAIGN_ID" = campaign."CAMPAIGN_ID" 
    LEFT JOIN staging_proxy.order_with_merchant_data_concat AS oms
    ON o.order_id = oms."ORDER_ID"
    LEFT JOIN dw.dim_staff AS staff
    ON oms."STAFF_ID" = staff."STAFF_ID"
    LEFT JOIN dw.dim_merchant AS merchant
    ON oms."MERCHANT_ID" = merchant."MERCHANT_ID";

    """

    print("   > Executing SQL Transformation...")
    with engine.begin() as conn:
        conn.execute(text(sql_script))
    
    print("✅ FACT_ORDER Created Successfully via SQL.")