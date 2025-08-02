### Charity Data Pipeline

A scalable data pipeline for processing charity data (donations, projects, volunteers) using Azure Databricks, Unity Catalog, Azure Data Factory, Azure Synapse Analytics, Azure Machine Learning, and Power BI. The pipeline ingests raw data from Azure Blob Storage, processes it through bronze, silver, and gold layers, trains ML models, and delivers insights via Power BI dashboards.

### Features
Data Ingestion: Azure Data Factory ingests data from Dynamics 365 and CSVs into bronze/ using Auto Loader.
Data Processing: Databricks notebooks transform data (bronze_to_silver.ipynb, silver_to_gold.ipynb, validate_data.ipynb) with Unity Catalog governance.
Machine Learning: Trains and deploys a LogisticRegression model (train_aml_model.ipynb) to predict high-value donors.
Analytics: Azure Synapse Analytics stores gold tables for Power BI reporting.
Governance: Unity Catalog manages metadata and access control.
Orchestration: Databricks Workflows and ADF automate the pipeline.

charity-data-pipeline/
├── notebooks/                      # Databricks notebooks
├── scripts/                        # Data generation and upload scripts
├── docs/                           # Documentation
├── .env.example                    # Environment variable template
├── requirements.txt                 # Dependencies
├── README.md                       # This file
├── LICENSE                         # MIT License
├── .gitignore                      # Git ignore

### Usage
Generate sample data:
python scripts/generate_data.py
python scripts/upload_to_blob.py
Run ADF pipeline to ingest data into bronze/.
Trigger Databricks Workflow (charity_data_pipeline).
Verify outputs in Unity Catalog (charity_catalog.gold.<table>) and Synapse SQL pool.
View dashboards in Power BI Service.