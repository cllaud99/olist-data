import os
import sys
from duck import get_csv_data
import duckdb
from models import validate_table, OlistCustomer
# Adiciona o diretório raiz do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.ingestion.kaggle import load_kaggle_credentials, main_extract


def run_ingestion_pipeline():
    """
    Executa o pipeline de ingestão de dados do Kaggle.
    """
    load_kaggle_credentials()
    download_path = "data/olist_lz"
    csv_file_path = 'data/olist_lz/olist_customers_dataset.csv'
    main_extract(download_path)
    conn, table = get_csv_data(csv_file_path, 'olist_customers')
    validate_table(conn, table, OlistCustomer)


if __name__ == "__main__":
    run_ingestion_pipeline()
