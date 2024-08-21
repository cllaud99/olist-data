import os
import sys

import duckdb
from duck import get_csv_data
from minio import create_bucket, upload_file
from models import OlistCustomer, OlistGeolocation, OlistOrder, OlistOrderItem, OlistOrderPayment, OlistOrderReview, OlistProduct, OlistSeller, validate_table

# Adiciona o diretório raiz do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.ingestion.kaggle import load_kaggle_credentials, main_extract


def run_ingestion_pipeline():
    """
    Executa o pipeline de ingestão de dados do Kaggle.
    """

    bucket_name = "raw-olist"

    create_bucket(bucket_name)

    load_kaggle_credentials()

    download_path = "data/olist_lz"

    #main_extract(download_path)

    mapeamento = {
        'olist_customers': OlistCustomer,
        'olist_geolocation': OlistGeolocation,
        'olist_order_items' : OlistOrderItem,
        'olist_order_payments' : OlistOrderPayment,
        'olist_order_reviews' : OlistOrderReview,
        'olist_orders' : OlistOrder,
        'olist_products' : OlistProduct,
        'olist_sellers' : OlistSeller
    }

    for item in mapeamento:
        class_reference = mapeamento[item]
        csv_file_path =  f"{download_path}/{item}_dataset.csv"
        print(f"Classe: {class_reference}, Caminho do CSV: {csv_file_path}")
        table_name = item
        conn, table = get_csv_data(csv_file_path, table_name)
        validate_table(conn, table, mapeamento[item])
        upload_file(bucket_name, csv_file_path, f'lz_{table_name}' )


if __name__ == "__main__":
    run_ingestion_pipeline()
