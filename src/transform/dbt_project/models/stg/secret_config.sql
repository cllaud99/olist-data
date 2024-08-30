{{ config(materialized='external', location='s3://raw-olist/raw_olist_customers.parquet') }}

FROM read_csv_auto('s3://raw-olist/raw_olist_customers.csv')