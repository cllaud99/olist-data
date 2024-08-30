import os
from typing import List
import duckdb

import boto3
from dotenv import load_dotenv

load_dotenv()

# Carregar as credenciais da AWS e outras variáveis do ambiente
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION: str = os.getenv("AWS_REGION")
BUCKET_NAME: str = os.getenv("BUCKET_NAME")

# Verificar se as variáveis foram carregadas corretamente
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET_NAME]):
    raise EnvironmentError(
        "Uma ou mais variáveis de ambiente necessárias não foram definidas corretamente."
    )

# Criar o cliente S3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def convert_csv_to_parquet_duckdb(csv_file: str) -> str:
    """
    Converte um arquivo CSV para o formato Parquet usando DuckDB.

    Args:
        csv_file (str): Caminho completo do arquivo CSV.

    Returns:
        str: Caminho completo do arquivo Parquet gerado.
    """
    # Define o caminho do arquivo Parquet
    parquet_file = csv_file.replace('.csv', '.parquet')

    # Executa a conversão usando DuckDB
    duckdb.query(f"COPY (SELECT * FROM read_csv_auto('{csv_file}')) TO '{parquet_file}' (FORMAT PARQUET);")

    return parquet_file


def list_files(folder: str) -> List[str]:
    """
    Lista todos os arquivos de uma pasta local.

    Args:
        folder (str): Caminho da pasta local onde os arquivos estão localizados.

    Returns:
        List[str]: Uma lista contendo os caminhos completos dos arquivos na pasta.
    """
    files: List[str] = []
    for file_name in os.listdir(folder):
        full_path = os.path.join(folder, file_name)
        if os.path.isfile(full_path):
            files.append(full_path)
    return files


def upload_files_to_s3(files: List[str]) -> List[str]:
    """
    Converte arquivos CSV para Parquet usando DuckDB e faz upload dos arquivos especificados para um bucket do S3.

    Args:
        files (List[str]): Lista dos caminhos completos dos arquivos CSV a serem convertidos e enviados para o S3.

    Returns:
        List[str]: Lista dos caminhos completos dos arquivos Parquet que foram enviados com sucesso.

    Raises:
        Exception: Se houver um erro durante o upload de um arquivo específico, ele será capturado e exibido.
    """
    uploaded_files: List[str] = []
    for file in files:
        # Converte o arquivo CSV para Parquet
        parquet_file = convert_csv_to_parquet_duckdb(file)
        file_name: str = os.path.basename(parquet_file)
        try:
            # Faz o upload do arquivo Parquet para o S3
            s3_client.upload_file(parquet_file, BUCKET_NAME, file_name)
            print(f"{file_name} foi enviado ao S3")
            uploaded_files.append(parquet_file)  # Adiciona à lista se o upload foi bem-sucedido
        except Exception as e:
            print(f"Erro ao fazer upload de {file_name}: {e}")
    return uploaded_files


def delete_local_files(files: List[str]) -> None:
    """
    Deleta os arquivos locais após o upload para o S3.

    Args:
        files (List[str]): Lista dos caminhos completos dos arquivos a serem deletados.

    Raises:
        Exception: Se houver um erro durante a exclusão de um arquivo específico, ele será capturado e exibido.
    """
    for file in files:
        try:
            os.remove(file)
            print(f"{file} deletado com sucesso do local")
        except Exception as e:
            print(f"Erro ao deletar {file}: {e}")


def main_load(folder: str) -> None:
    """
    Função principal que orquestra a listagem, upload e exclusão de arquivos.

    Args:
        folder (str): Caminho da pasta local onde os arquivos estão localizados.

    Funcionamento:
        1. Lista todos os arquivos na pasta especificada.
        2. Faz upload dos arquivos listados para o bucket S3.
        3. Deleta os arquivos locais que foram enviados com sucesso.
    """
    files: List[str] = list_files(folder)
    if files:
        uploaded_files = upload_files_to_s3(files)
        if uploaded_files:
            delete_local_files(uploaded_files)
        else:
            print("Nenhum arquivo foi enviado com sucesso. Nenhum arquivo local será deletado.")
    else:
        print("Nenhum arquivo encontrado para upload")


if __name__ == "__main__":
    FOLDER = "data/olist_lz"
    main_load(FOLDER)
