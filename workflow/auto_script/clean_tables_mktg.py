from auto_script.total_cleanfuncs import (
    convert_columns_to_uppercase,
    drop_index_column,
    remove_duplicates,
    convert_to_datetime,
    handle_missing_values,
    # UNIQUE TO BUSINESS
    clean_campaign_discount,
    clean_campaign_df,
    convert_transaction_date_column,
    clean_estimated_arrival  
)
from auto_script.auto_concat import consolidate_tables

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from datetime import datetime
from sqlalchemy import inspect  

# ==========================================
# MAIN ORCHESTRATOR (SINGLE FUNCTION)
# ==========================================

def clean_all_tables_marketing(schema_name='marketing_staging', table_suffix='_cleaned',**kwargs):
    """
    Single function to handle connection, loading, cleaning, and saving 
    for Product Dataset.
    """
    
    # 1. Setup Database Connection (Same pattern as your basis)
    hook = PostgresHook(postgres_conn_id="postgres_staging")
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    schema_nm = f"{schema_name}{table_suffix}"
    
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_nm};")
        conn.commit()
    print(f"✅ Schema '{schema_nm}' is ready.")

    try:
    
        inspector = inspect(engine)
        all_tables = inspector.get_table_names(schema=schema_name)

        tables_to_process = []
        for t in all_tables:
            if t.startswith('pg_') or t.startswith('airflow'):
                continue
            if t.endswith(table_suffix):
                continue
            
            tables_to_process.append(t)
            
    except Exception as e:
        print(f" Failed to fetch tables from schema '{schema_name}': {e}")
        return


    for table_name in tables_to_process:
        try:
            print(f"🔄 Processing raw table: {table_name}")
            
    
            chunksize = 100_000 
            chunk_iter = pd.read_sql(
                f'SELECT * FROM "{schema_name}"."{table_name}"',
                engine, 
                chunksize=chunksize
            )
            
            cleaned_chunks = []

            if table_name == 'campaign_data':
            
                for chunk in chunk_iter:
                        df_c = clean_campaign_df(chunk)
                        df_c = drop_index_column(df_c)
                        df_c = remove_duplicates(df_c)
                        df_c = handle_missing_values(df_c)
                        df_c = clean_campaign_discount(df_c)
                        df_c = convert_columns_to_uppercase(df_c)

                        cleaned_chunks.append(df_c)

            elif table_name == 'transactional_campaign_data':
            
                for chunk in chunk_iter:
                        df_tc = drop_index_column(chunk)
                        df_tc = remove_duplicates(df_tc)
                        df_tc = handle_missing_values(df_tc)
                        df_tc = convert_transaction_date_column(df_tc)
                        df_tc = clean_estimated_arrival(df_tc)
                        df_tc = convert_columns_to_uppercase(df_tc)

                        cleaned_chunks.append(df_tc)
            else:
                # Default cleaning for other tables (optional)
                for chunk in chunk_iter:
                    df_def = drop_index_column(chunk)
                    df_def = remove_duplicates(df_def)
                    df_def = convert_to_datetime(df_def)
                    df_def = handle_missing_values(df_def)
                    df_def = convert_columns_to_uppercase(df_def)

                    cleaned_chunks.append(df_def)
                    
            if cleaned_chunks:

                df_final = pd.concat(cleaned_chunks, ignore_index=True)
            
                cleaned_table_name = f"{table_name}{table_suffix}"
                
                df_final.to_sql(
                    name=cleaned_table_name,
                    con=engine,
                    schema=schema_nm,  
                    if_exists="replace",
                    index=False
                )
                print(f" Created: {schema_nm}.{cleaned_table_name}")

                # Delete Original (Use raw connection for this simple command)
                with conn.cursor() as cursor:
                    cursor.execute(f'DROP TABLE IF EXISTS "{schema_nm}"."{table_name}"')
                    conn.commit()
                    print(f"🗑️ Deleted raw table: {schema_nm}.{table_name}")

        except Exception as e:
            print(f" Error processing '{table_name}': {type(e).__name__}: {e}")


    # 3. CALL CONSOLIDATION (The "Fan-In" Step)
    # ==========================================
    # We call this AFTER the loop finishes, so all '_cleaned' tables are ready to be merged.
    print("\n--- Cleaning Complete. Starting Consolidation... ---")
    consolidate_tables(engine, schema_nm, table_suffix)