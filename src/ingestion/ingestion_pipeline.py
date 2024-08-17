import sys
import os

# Adiciona o diretório raiz do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.ingestion.kaggle import load_kaggle_credentials, main_extract

def run_ingestion_pipeline():
    """
    Executa o pipeline de ingestão de dados do Kaggle.
    """
    load_kaggle_credentials()
    download_path = 'data/olist_lz'
    main_extract(download_path)

if __name__ == "__main__":
    run_ingestion_pipeline()