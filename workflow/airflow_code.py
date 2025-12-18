from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import os
from datetime import datetime

# ======= IMPORTS =======
# Operations Imports
from auto_script.iterate_table_cleaning import clean_all_tables_operations
from auto_script.validation_operations import validate_cleaned_tables_operations

# Enterprise Imports
from auto_script.clean_tables_enter import clean_all_tables_enterprise
from auto_script.validation_enterprise import validate_cleaned_tables_enterprise

# Business Imports
from auto_script.clean_tables_bus import clean_all_tables_business
from auto_script.validation_business import validate_cleaned_tables_business

# Marketing Imports
from auto_script.clean_tables_mktg import clean_all_tables_marketing    
from auto_script.validation_marketing import validate_cleaned_tables_marketing

# Customer Management Imports
from auto_script.clean_tables_customer_mgt import clean_all_tables_customer_mgt     
from auto_script.validation_customer_mgt import validate_cleaned_tables_customer_management
# ==========================================
# 1. INGESTION FUNCTIONS
# ==========================================

def generic_ingestion(path, schema_name):
    """
    Generic function to handle ingestion for any department.
    """
    hook = PostgresHook(postgres_conn_id="postgres_staging")
    engine = hook.get_sqlalchemy_engine()
    conn = hook.get_conn()
    
    # Ensure Schema Exists
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        conn.commit()
    print(f"✅ Schema '{schema_name}' is ready.")

    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)
        if os.path.isfile(file_path):
            df = None
            try:
                if filename.endswith('.csv'): df = pd.read_csv(file_path)
                elif filename.endswith('.parquet'): df = pd.read_parquet(file_path)
                elif filename.endswith('.xlsx'): df = pd.read_excel(file_path)
                elif filename.endswith('.json'): df = pd.read_json(file_path)
                elif filename.endswith('.pickle'): df = pd.read_pickle(file_path)
                elif filename.endswith('.html'): 
                    dfs = pd.read_html(file_path)
                    df = dfs[0] if dfs else None
                else:
                    print(f"Skipping unsupported file: {filename}")
                    continue
            except Exception as e:
                print(f"❌ Error reading {filename}: {e}")
                raise e

            if df is not None:
                df["source_file"] = filename
                # Sanitize table name
                table_name = filename.split(".")[0].replace(" ", "_").replace("-", "_").replace(".", "_").lower()

                try:
                    df.to_sql(
                        name=table_name,
                        con=engine,
                        schema=schema_name,      
                        if_exists="replace",     
                        index=False
                    )
                    print(f"✅ Uploaded to {schema_name}.{table_name}")
                except Exception as e:
                    print(f"❌ Error uploading {schema_name}.{table_name}: {e}")
                    raise e

# Specific wrappers for clarity in DAGs
def operation_department(path): generic_ingestion(path, 'operation_staging')
def enterprise_department(path): generic_ingestion(path, 'enterprise_staging')
def business_department(path): generic_ingestion(path, 'business_staging')
def marketing_department(path): generic_ingestion(path, 'marketing_staging')
def customer_management_department(path): generic_ingestion(path, 'customer_management_staging')


# ==========================================
# 2. DAG DEFINITIONS
# ==========================================

START_DATE = datetime(2023, 1, 1)

# --- 1. OPERATIONS (Full Pipeline) ---
with DAG('operation_department_dag', start_date=START_DATE, schedule_interval=None, catchup=False) as dag_op:
    t_op1 = PythonOperator(
        task_id='process_operation_department',
        python_callable=operation_department,
        op_kwargs={'path': '/opt/airflow/operations_department'}
    )
    t_op2 = PythonOperator(
        task_id='clean_all_tables_operations',
        python_callable=clean_all_tables_operations,
        op_kwargs={'schema_name': 'operation_staging', 'table_suffix': '_cleaned'}
    )
    t_op3 = PythonOperator(
        task_id='validate_cleaned_data_operation',
        python_callable=validate_cleaned_tables_operations,
        op_kwargs={'schema_name': 'operation_staging_cleaned', 'table_suffix': '_cleaned'}
    )
    t_op1 >> t_op2 >> t_op3

# --- 2. ENTERPRISE  ---
with DAG('enterprise_department_dag', start_date=START_DATE, schedule_interval=None, catchup=False) as dag_ent:
    t_ent1 = PythonOperator(
        task_id='process_enterprise_department',
        python_callable=enterprise_department,
        op_kwargs={'path': '/opt/airflow/enterprise_department'}
    )
    t_ent2 = PythonOperator(
        task_id='clean_all_tables_enterprise',
        python_callable=clean_all_tables_enterprise,
        op_kwargs={'schema_name': 'enterprise_staging', 'table_suffix': '_cleaned'}
    )
    t_ent3 = PythonOperator(
        task_id='validate_cleaned_data_enterprise',
        python_callable=validate_cleaned_tables_enterprise,
        op_kwargs={'schema_name': 'enterprise_staging_cleaned', 'table_suffix': '_cleaned'}
    )
    t_ent1 >> t_ent2 >> t_ent3

# --- 3. BUSINESS  ---
with DAG('business_department_dag', start_date=START_DATE, schedule_interval=None, catchup=False) as dag_bus:
    t_bus1 = PythonOperator(
        task_id='process_business_department',
        python_callable=business_department,
        op_kwargs={'path': '/opt/airflow/business_department'}
    )
    t_bus2 = PythonOperator(
        task_id='clean_all_tables_business',
        python_callable=clean_all_tables_business,
        op_kwargs={'schema_name': 'business_staging', 'table_suffix': '_cleaned'}
    )
    t_bus3 = PythonOperator(
        task_id='validate_cleaned_data_operation',
        python_callable=validate_cleaned_tables_business,
        op_kwargs={
            'schema_name': 'business_staging_cleaned',
            'table_suffix': '_cleaned'
        }
    )
    t_bus1 >> t_bus2 >> t_bus3

# --- 4. MARKETING ---
with DAG('marketing_department_dag', start_date=START_DATE, schedule_interval=None, catchup=False) as dag_mktg:
    t_mktg1 = PythonOperator(
        task_id='process_marketing_department',
        python_callable=marketing_department,
        op_kwargs={'path': '/opt/airflow/marketing_department'}
    )
    t_mktg2 = PythonOperator(
        task_id='clean_all_tables_marketing',
        python_callable=clean_all_tables_marketing,
        op_kwargs={'schema_name': 'marketing_staging', 'table_suffix': '_cleaned'}
    )
    t_mktg3 = PythonOperator(
        task_id='validate_cleaned_data_marketing',
        python_callable=validate_cleaned_tables_marketing,
        op_kwargs={
            'schema_name': 'marketing_staging_cleaned',
            'table_suffix': '_cleaned'
        }
    )
    t_mktg1 >> t_mktg2 >> t_mktg3
# --- 5. CUSTOMER MANAGEMENT ---
with DAG('customer_management_department_dag', start_date=START_DATE, schedule_interval=None, catchup=False) as dag_cust:
    t_cust1 = PythonOperator(
        task_id='process_customer_management_department',
        python_callable=customer_management_department,
        op_kwargs={'path': '/opt/airflow/customer_management_department'}
    )
    t_cust2 = PythonOperator(
        task_id='clean_all_tables_customer',
        python_callable=clean_all_tables_customer_mgt,
        op_kwargs={'schema_name': 'customer_management_staging', 'table_suffix': '_cleaned'}
    )

    t_cust3 = PythonOperator(
        task_id='validate_cleaned_data_customer',
        python_callable=validate_cleaned_tables_customer_management,
        op_kwargs={
            'schema_name': 'customer_management_staging',
            'table_suffix': '_cleaned'
        }
    )
    t_cust1 >> t_cust2 >> t_cust3




