
# E-commerce ETL Pipeline

This project demonstrates an end-to-end ETL pipeline for e-commerce sales data.

## Components
- Ingestion: Python + Pandas
- Storage: PostgreSQL
- Transformation: dbt
- Orchestration: Airflow

## Structure
- `data/` - CSV input files
- `Python scripts to load and clean data
- `dags/` - Airflow DAGs
- `sql/` - SQL scripts
- `dbt_project/` - dbt transformation models

## How to Run
1. Populate the `data/` folder with CSVs (orders, customers, products).
2. Run `load_data.py` to clean data.
3. Set up PostgreSQL and dbt project.
4. Use Airflow to schedule and run ETL.
