{{ config(
    materialized='table'
) }}

WITH raw_data AS (
    SELECT *
    FROM read_csv_auto('s3://raw-olist/raw_olist_customers.csv')
)

SELECT *
FROM raw_data