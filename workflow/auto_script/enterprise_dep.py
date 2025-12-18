from sqlalchemy import text
import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


def enterprise_department(path, schema_name='enterprise_staging'):
    hook = PostgresHook(postgres_conn_id="postgres_staging")
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Ensure the schema exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    conn.commit()
    cursor.close()
    print(f"✅ Schema '{schema_name}' is ready.")

    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)

        if os.path.isfile(file_path):
            df = None

            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(file_path)
                elif filename.endswith('.parquet'):
                    df = pd.read_parquet(file_path)
                elif filename.endswith('.xlsx'):
                    df = pd.read_excel(file_path)
                elif filename.endswith('.json'):
                    df = pd.read_json(file_path)
                elif filename.endswith('.pkl') or filename.endswith('.pickle'):
                    df = pd.read_pickle(file_path)
                elif filename.endswith('.html'):
                    df = pd.read_html(file_path)
                    df = df[0] 
                else:
                    print(f"Skipping unsupported file: {filename}")
                    continue
            except Exception as e:
                print(f"Error reading {filename}: {e}")
                continue

    
            df["source_file"] = filename

            
            table_name = (
                filename.split(".")[0]
                .replace(" ", "_")
                .replace("-", "_")
                .replace(".", "_")
                .lower()
            )


            try:
                df.to_sql(
                    name=table_name,
                    con=engine,
                    schema=schema_name,      
                    if_exists="replace",     
                    index=False
                )
                print(f"Uploaded to staging table: {schema_name}.{table_name}")

            except Exception as e:
                print(f"Error uploading {schema_name}.{table_name}: {e}")

