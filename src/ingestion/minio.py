import os

import boto3
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurar variáveis a partir do .env
minio_root_user = os.getenv("MINIO_ROOT_USER")
minio_root_password = os.getenv("MINIO_ROOT_PASSWORD")
minio_endpoint_url = os.getenv("MINIO_ENDPOINT_URL")

# Configuração das credenciais e endpoint do MinIO
minio_client = boto3.client(
    "s3",
    endpoint_url=minio_endpoint_url,
    aws_access_key_id=minio_root_user,
    aws_secret_access_key=minio_root_password,
    region_name="us-east-1",
    use_ssl=False,
)


def create_bucket(bucket_name):
    """
    Cria um bucket no MinIO.

    Parameters:
    bucket_name (str): Nome do bucket a ser criado.

    Returns:
    None
    """
    try:
        minio_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' criado com sucesso.")
    except Exception as e:
        print(f"Erro ao criar o bucket: {e}")


def upload_file(bucket_name, file_name, object_name):
    """
    Faz o upload de um arquivo para um bucket no MinIO.

    Parameters:
    bucket_name (str): Nome do bucket onde o arquivo será armazenado.
    file_name (str): Caminho local do arquivo a ser enviado.
    object_name (str): Nome do objeto (incluindo o caminho) no bucket.

    Returns:
    None
    """
    try:
        minio_client.upload_file(file_name, bucket_name, object_name)
        print(
            f"Arquivo '{file_name}' enviado com sucesso para '{bucket_name}/{object_name}'."
        )
    except Exception as e:
        print(f"Erro ao enviar o arquivo: {e}")


def list_objects(bucket_name):
    """
    Lista todos os objetos em um bucket no MinIO.

    Parameters:
    bucket_name (str): Nome do bucket do qual listar os objetos.

    Returns:
    None
    """
    try:
        response = minio_client.list_objects_v2(Bucket=bucket_name)
        print("Objetos no bucket:")
        for obj in response.get("Contents", []):
            print(obj["Key"])
    except Exception as e:
        print(f"Erro ao listar objetos: {e}")


def download_file(bucket_name, object_name, download_path):
    """
    Faz o download de um arquivo de um bucket no MinIO.

    Parameters:
    bucket_name (str): Nome do bucket de onde o arquivo será baixado.
    object_name (str): Nome do objeto a ser baixado (incluindo o caminho).
    download_path (str): Caminho local onde o arquivo será salvo.

    Returns:
    None
    """
    try:
        minio_client.download_file(bucket_name, object_name, download_path)
        print(f"Arquivo baixado com sucesso para '{download_path}'.")
    except Exception as e:
        print(f"Erro ao baixar o arquivo: {e}")
