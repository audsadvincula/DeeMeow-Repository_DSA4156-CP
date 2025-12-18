from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# ==========================================
# MASTER CONTROLLER DAG
# ==========================================
# This DAG connects your existing Department DAGs with your new Transformation DAG.
# Flow:
# 1. Trigger all 5 Department DAGs (Operations, Enterprise, Business, Marketing, Customer)
# 2. Wait for ALL to finish.
# 3. Trigger the Unified Transformation DAG.

START_DATE = datetime(2023, 1, 1)

with DAG(
    dag_id='master_pipeline_controller',
    start_date=START_DATE,
    schedule_interval='@daily',  # Run everything once a day
    catchup=False,
    tags=['master', 'orchestrator']
) as dag:

    # --- PHASE 1: TRIGGER DEPARTMENTS (Parallel) ---
    
    # Operations
    trig_ops = TriggerDagRunOperator(
        task_id='trigger_operations',
        trigger_dag_id='operation_department_dag', # Matches DAG ID in main_pipeline.py
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=['failed']
    )

    # Enterprise
    trig_ent = TriggerDagRunOperator(
        task_id='trigger_enterprise',
        trigger_dag_id='enterprise_department_dag', # Matches DAG ID in main_pipeline.py
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=['failed']
    )

    # Business
    trig_bus = TriggerDagRunOperator(
        task_id='trigger_business',
        trigger_dag_id='business_department_dag', # Matches DAG ID in main_pipeline.py
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=['failed']
    )

    # Marketing
    trig_mktg = TriggerDagRunOperator(
        task_id='trigger_marketing',
        trigger_dag_id='marketing_department_dag', # Matches DAG ID in main_pipeline.py
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=['failed']
    )

    # Customer Management
    trig_cust = TriggerDagRunOperator(
        task_id='trigger_customer',
        trigger_dag_id='customer_management_department_dag', # Matches DAG ID in main_pipeline.py
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=['failed']
    )

    # --- PHASE 2: TRIGGER TRANSFORMATION (Sequential) ---
    
    # This triggers the DAG defined in 'unified_transformation_dag.py'
    trig_transform = TriggerDagRunOperator(
        task_id='trigger_unified_transformation',
        trigger_dag_id='unified_transformation_dag', 
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=['failed']
    )

    # ==========================================
    # DEPENDENCIES
    # ==========================================
    # Run all departments in parallel, then run transformation
    [trig_ops, trig_ent, trig_bus, trig_mktg, trig_cust] >> trig_transform