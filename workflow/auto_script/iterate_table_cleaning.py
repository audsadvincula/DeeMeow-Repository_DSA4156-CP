from auto_script.auto_cleaned import detect_currency_columns, find_duplicated_rows, clean_quantity
from auto_script.auto_concat import consolidate_tables

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect  

def clean_all_tables_operations(schema_name='operation_staging', table_suffix='_cleaned', **kwargs):
    
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
            for chunk in chunk_iter:
                
                df_clean = detect_currency_columns(chunk)
                df_clean = find_duplicated_rows(df_clean)
                
                if 'quantity' in df_clean.columns:
                    df_clean = clean_quantity(df_clean, 'quantity')
                
                cleaned_chunks.append(df_clean)

            
            df_final = pd.concat(cleaned_chunks, ignore_index=True)
            
            if "Unnamed: 0" in df_final.columns:
                df_final = df_final.drop(columns=["Unnamed: 0"])

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
