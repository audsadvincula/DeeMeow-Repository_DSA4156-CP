from auto_script.total_cleanfuncs import (
    drop_index_column,
    handle_missing_values,
    convert_to_datetime,
    remove_duplicates,
    convert_columns_to_uppercase,
    # Unique Enterpise
    clean_phone_numbers,
    clean_country_column
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

def clean_all_tables_enterprise(schema_name='enterprise_staging', table_suffix='_cleaned',**kwargs):
    """
    Single function to handle connection, loading, cleaning, and saving 
    for Merchant, Staff, and Order datasets.
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
            
            if table_name == 'merchant_data':
            
                for chunk in chunk_iter:
                        df_m = drop_index_column(chunk)
                        df_m = remove_duplicates(df_m)
                        df_m = handle_missing_values(df_m)
                        df_m = clean_phone_numbers(df_m)
                        df_m = convert_to_datetime(df_m)
                        df_m = convert_columns_to_uppercase(df_m)
                        
                        cleaned_chunks.append(df_m)
                
            elif table_name == 'staff_data':
                
                for chunk in chunk_iter:
                    df_s = drop_index_column(chunk)
                    df_s = remove_duplicates(df_s) 
                    df_s = handle_missing_values(df_s)
                    df_s = clean_phone_numbers(df_s) 
                    df_s = convert_to_datetime(df_s)
                    df_s = clean_country_column(df_s)
                    df_s = convert_columns_to_uppercase(df_s)

                    cleaned_chunks.append(df_s)
                        
            elif "order_with_merchant" in table_name.lower():
                
                for chunk in chunk_iter:
                    df_o = drop_index_column(chunk)
                    df_o = remove_duplicates(df_o)
                    df_o = handle_missing_values(df_o)
                    df_o = convert_columns_to_uppercase(df_o)

                    cleaned_chunks.append(df_o)

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