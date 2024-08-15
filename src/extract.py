import os
from dotenv import load_dotenv
import zipfile
from logger_config import logger  # Importa a configuração do logger

load_dotenv()

os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')


def main(destination_dir):
    os.makedirs(destination_dir, exist_ok=True)

    try:
        # Baixa o dataset
        logger.info("Baixando o dataset do Kaggle...")
        os.system(f'kaggle datasets download -d olistbr/brazilian-ecommerce -p {destination_dir}')

        # Caminho do arquivo ZIP baixado
        zip_file_path = os.path.join(destination_dir, "brazilian-ecommerce.zip")

        # Extrai o arquivo .zip
        logger.info("Extraindo o arquivo ZIP...")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(destination_dir)

        # Remove o arquivo ZIP após a extração
        logger.info("Removendo o arquivo ZIP...")
        os.remove(zip_file_path)

        logger.success("Download, extração e limpeza concluídos com sucesso!")

    except Exception as e:
        logger.error(f"Ocorreu um erro: {e}")
    
if __name__ == "__main__":
    destination_dir = "dbt_project/seeds"
    main(destination_dir)
