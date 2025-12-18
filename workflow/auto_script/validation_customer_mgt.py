import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import SQLAlchemyError
from auto_script.auto_validation import auto_validation
from sqlalchemy import inspect
import warnings


warnings.filterwarnings('ignore', category=UserWarning)

def validate_cleaned_tables_customer_management(schema_name='customer_management_staging_cleaned', table_suffix='_cleaned', **kwargs):
    print(f"--- Starting Validation Task for Schema: {schema_name} ---")
    
    hook = PostgresHook(postgres_conn_id="postgres_staging")
    engine = hook.get_sqlalchemy_engine()
    
    try:
       
        inspector = inspect(engine)
        all_tables = inspector.get_table_names(schema=schema_name)
        
        
        tables = [t for t in all_tables if t.endswith(table_suffix)]
        
        
    except Exception as e:
        print(f" Failed to fetch tables: {e}")
        return

    if not tables:
        print(f" No tables found with suffix '{table_suffix}'.")
        return

    for table_name in tables:
        print(f"\n Validating Table: {table_name}")
        
        try:

            df = pd.read_sql(f'SELECT * FROM "{schema_name}"."{table_name}"', engine)
            
            report = auto_validation(df)
            
         
            print(f"   >  Shape: {report['data_shape']}")
            
            if report['missing_values']:
                print(f"   >  Missing Values: {report['missing_values']}")
            else:
                print(f"   >  No Missing Values")

            if report.get('date_validation'):
                print(f"   > 📅 Date Issues: {report['date_validation']}")

            print(f"   >  Data Types: {report['data_types']}")
            print(f"   >  Unique Values: {report['unique_values']}")
            print(f"   >  Duplicate Rows: {report['duplicate_rows']}")
         
            # --------------------

        except Exception as e:
            print(f"   >  Error validating {table_name}: {e}")

    print("\n--- Validation Task Completed ---")