from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import the transformation functions we defined in the previous step
from auto_script.transformation_functions import (
    create_dim_user,
    create_dim_product,
    create_dim_campaign,
    create_dim_merchant,
    create_dim_staff,
    create_fact_order
)

# ==========================================
# DAG CONFIGURATION
# ==========================================
# This DAG is responsible for the "T" (Transform) and "L" (Load to DW)
# It is typically triggered by the Master Orchestrator after all Staging/Cleaning is done.

START_DATE = datetime(2023, 1, 1)

with DAG(
    'unified_transformation_dag',
    start_date=START_DATE,
    schedule_interval=None, # Triggered externally (by Master DAG)
    catchup=False,
    tags=['dw', 'transformation', 'star_schema', 'merging']
) as dag:

    # ==========================================
    # PHASE 1: DIMENSION TABLES (Parallel)
    # ==========================================
    
    t_dim_user = PythonOperator(
        task_id='create_dim_user',
        python_callable=create_dim_user
    )

    t_dim_prod = PythonOperator(
        task_id='create_dim_product',
        python_callable=create_dim_product
    )

    t_dim_camp = PythonOperator(
        task_id='create_dim_campaign',
        python_callable=create_dim_campaign
    )

    t_dim_merch = PythonOperator(
        task_id='create_dim_merchant',
        python_callable=create_dim_merchant
    )

    t_dim_staff = PythonOperator(
        task_id='create_dim_staff',
        python_callable=create_dim_staff
    )

    # ==========================================
    # PHASE 2: FACT TABLE (Dependent)
    # ==========================================
    
    t_fact_order = PythonOperator(
        task_id='create_fact_order',
        python_callable=create_fact_order
    )

    # ==========================================
    # DEPENDENCIES
    # ==========================================
    # We must create all Dimensions BEFORE creating the Fact table.
    # This ensures the Foreign Keys (e.g., product_reference_number) exist for lookup.
    
    [t_dim_user, t_dim_prod, t_dim_camp, t_dim_merch, t_dim_staff] >> t_fact_order