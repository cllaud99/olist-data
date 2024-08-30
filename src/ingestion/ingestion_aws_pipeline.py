import os
import sys
from awss3 import upload_files_to_s3, list_files, delete_local_files
from duck import get_csv_data
from models import (
    OlistCustomer,
    OlistGeolocation,
    OlistOrder,
    OlistOrderItem,
    OlistOrderPayment,
    OlistOrderReview,
    OlistProduct,
    OlistSeller,
    validate_table,
)
from kaggle import main_extract

# Adiciona o diretório raiz do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.ingestion.kaggle import load_kaggle_credentials, main_extract


def run_ingestion_pipeline():
    """
    Executa o pipeline de ingestão de dados do Kaggle.
    """

    load_kaggle_credentials()

    download_path = "data"

    main_extract(download_path)


    mapeamento = {
        "olist_customers": OlistCustomer,
        "olist_geolocation": OlistGeolocation,
        "olist_order_items": OlistOrderItem,
        "olist_order_payments": OlistOrderPayment,
        "olist_order_reviews": OlistOrderReview,
        "olist_orders": OlistOrder,
        "olist_products": OlistProduct,
        "olist_sellers": OlistSeller,
    }

    for item in mapeamento:
        class_reference = mapeamento[item]
        csv_file_path = f"{download_path}/{item}_dataset.csv"
        print(f"Classe: {class_reference}, Caminho do CSV: {csv_file_path}")
        table_name = item
        conn, table = get_csv_data(csv_file_path, table_name)
        validate_table(conn, table, mapeamento[item])

    files = list_files('data')
    upload_files_to_s3(files=files)
    delete_local_files(files)

if __name__ == "__main__":
    run_ingestion_pipeline()
