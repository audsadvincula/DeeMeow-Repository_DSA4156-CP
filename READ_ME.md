## DEEMEOW PROJECT

The DeeMeow Project is an end-to-end Data Warehousing pipeline created for the fictional e-commerce platform: ShopZada.



## PROJECT OVERVIEW

This project is designed to support effective operational and strategic decision-making, including the integration of various datasets through a structured ETL pipeline, ensuring that data remains consistent and reliable across sources.

## FEATURES
- Multi-Layer Data Warehouse Architecture (staging → warehouse → presentation)
- Automated ETL Pipelines (Data ingestion, transformation, and loading)
- Kimball-Based Dimensional Modeling
- Business Intelligence & Dashboards
- Sales Performance Analysis
- Financial Metrics & Profitability Analysis
- Sales Forecasting & Trend Analysis
- Operational Impact Analysis

## TECHNOLOGIES USED
- Python
- Pandas
- Apache Airflow
- PostgreSQL
- Tableau

#Installation and Instructions

1. Download dataset “ShopZada 2.0 – Enterprise DWH Dataset”
2. Set up repository according to what is below.


```python
#REPOSITORY SCHEME

DeeMeow Project Data Scheme

┌──[00] DeeMeow Project				            # ← Folder for the Entire Project
│	└── 01. Docker Compose File
│	└── 02. Docker Files
│	└── 03. DAGs Folder                     # ← Folder for the Orchestrator
│	    └── 03.1 auto_script                # ← Folder for the automated scripts
│	    └── 03.2 airflow_code
│	    └── 03.3 Main Orchestrator Script
└─────└── 03.4 transform_pipeline
```

3. Build your docker in bash.


```python
docker build
```


```python
docker up
```

4. Access Postgres.
- via `localhost` on port `8080`.


```python
#on web browser
localhost:8080
```

5. Access PgAdmin.
- via `localhost` on port `5050`.
- Trigger master pipeline controller.
- This activates the 5 departments to go through transformation.


```python
#on web browser
localhost:5050
```

6. Connect to Tableau
- access the data from the Star Schema inside the Data Warehouse.

## OUTCOME
The DeeMeow Project includes a fully finctional and automated ETL pipeline that successfully ingests, cleans, validates, and integrates data from various ShopZada departments into a centralized data warehouse.

The pipeline produces structured dimension tables and a granular fact table, ensuring consistent and high-quality data for analysis.

The project also provides validated analytical views and interactive visualizations, providing clear insights into sales performance, profitability, trends, and operational patterns. This confirms the effectiveness of the end-to-end data engineering and analytics proposal.

## LIMITATIONS
<br>
1.  <b>Scalability Constraints of the Kimball Approach</b> - The Kimball dimensional model is optimized for analytical performance but can become difficult to scale as data volume and business complexity grow. Adding new dimensions or fact tables may require significant redesign and maintenance. This can limit flexibility as ShopZada expands globally.
<br><br>
2. <b> Dependency on Data Quality from Source Systems </b> - The data warehouse relies heavily on the accuracy and consistency of source system data. Even with validation and cleaning in the ETL pipeline, poor-quality input data can still affect downstream analytics. This dependency increases the risk of inaccurate insights if source data issues persist.
<br><br>
3. <b> Access and Governance Constraints </b> - Restricting access to the physical data model helps maintain data integrity and security. However, it may limit flexibility for non-technical users who need direct data exploration. Additional governance layers or self-service tools may be required to balance control and accessibility.
<br><br>

## AUTHORS:
From: The University of Santo Tomas, Manila, Philippines

3rd Year Students in B.S. Data Science and Analytics
```
Advincula, Audrey Johannah
Arago, Katrina Mae
Dumpit, Milaine Antonelle
Guanlao, Cianne Paulette
Melgar, Christian Miguel
Orilla, Zane
Vitor, Jeanne Margarette
```

