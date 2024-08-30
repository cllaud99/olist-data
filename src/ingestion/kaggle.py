import os
import subprocess
import sys
import time
import zipfile

from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.logger_config import logger


def load_kaggle_credentials():
    """
    Carrega as credenciais do Kaggle a partir de variáveis de ambiente.
    """
    load_dotenv()
    os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME")
    os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY")


def download_kaggle_dataset(destination_dir, dataset="olistbr/brazilian-ecommerce"):
    """
    Baixa um dataset do Kaggle para o diretório especificado.

    Args:
        destination_dir (str): O diretório onde o dataset será salvo.
        dataset (str): O identificador do dataset no Kaggle.
    """
    logger.info("Baixando o dataset do Kaggle...")
    try:
        subprocess.run(
            ["kaggle", "datasets", "download", "-d", dataset, "-p", destination_dir],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao baixar o dataset: {e}")
        raise


def extract_zip_file(zip_file_path, extract_to):
    """
    Extrai o conteúdo de um arquivo ZIP para o diretório especificado.

    Args:
        zip_file_path (str): O caminho do arquivo ZIP a ser extraído.
        extract_to (str): O diretório onde os arquivos serão extraídos.
    """
    logger.info("Extraindo o arquivo ZIP...")
    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
    except zipfile.BadZipFile as e:
        logger.error(f"Erro ao extrair o arquivo ZIP: {e}")
        raise


def remove_file(file_path):
    """
    Remove o arquivo especificado do sistema.

    Args:
        file_path (str): O caminho do arquivo a ser removido.
    """
    logger.info("Removendo o arquivo ZIP...")
    try:
        os.remove(file_path)
    except OSError as e:
        logger.error(f"Erro ao remover o arquivo: {e}")
        raise


def main_extract(destination_dir):
    """
    Realiza o processo de download, extração e limpeza do dataset.

    Args:
        destination_dir (str): O diretório de destino para os arquivos extraídos.
    """
    os.makedirs(destination_dir, exist_ok=True)

    start_time = time.time()  # Registrar o tempo de início

    try:
        download_kaggle_dataset(destination_dir)
        zip_file_path = os.path.join(destination_dir, "brazilian-ecommerce.zip")
        extract_zip_file(zip_file_path, destination_dir)
        remove_file(zip_file_path)
        logger.success("Download, extração e limpeza concluídos com sucesso!")
    except Exception as e:
        logger.error(f"Ocorreu um erro: {e}")

    end_time = time.time()  # Registrar o tempo de término
    duration = end_time - start_time  # Calcular a duração
    logger.info(f"Tempo total da pipeline: {duration:.2f} segundos")


if __name__ == "__main__":
    destination_dir = "data/olist_lz"
    load_kaggle_credentials()
    main_extract(destination_dir)
