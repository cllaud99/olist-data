import os
import sys
from typing import List

import duckdb

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.logger_config import logger


def load_aws_credentials(duckdb_con, profile: str):
    # Função ainda não implementada
    pass


def write_to_s3_from_duckdb(
    duckdb_con, table: str, s3_path: str, timestamp_column: str
):
    logger.info(f"Escrevendo dados para S3 {s3_path} / {table}")
    duckdb_con.execute(
        f"""
        COPY (
            SELECT *,
                EXTRACT(YEAR FROM {timestamp_column}) AS year,
                EXTRACT(MONTH FROM {timestamp_column}) AS month
            FROM {table}
        )
        TO '{s3_path}/{table}'
        (FORMAT PARQUET, PARTITION BY (year, month), OVERWRITE OR IGNORE 1, COMPRESSION 'ZSTD')
        """
    )


def get_csv_data(csv_file_path: str, table: str):
    logger.info(f"Criando tabela a partir do CSV {csv_file_path}")
    conn = None
    try:
        # Conectar ao DuckDB (em memória para simplicidade)
        conn = duckdb.connect()

        # Criar uma tabela temporária a partir do CSV
        conn.execute(
            f"""
            CREATE TABLE {table} AS
            SELECT * FROM read_csv_auto('{csv_file_path}')
            """
        )

        # Consultar e imprimir os dados da tabela
        result = conn.execute(f"SELECT * FROM {table} LIMIT 10").fetchall()
        print("Dados do CSV:")
        for row in result:
            print(row)

        return conn, table  # Retorne a conexão e o nome da tabela

    except Exception as e:
        print(f"Erro: {e}")
        if conn is not None:
            conn.close()
        raise  # Re-raise the exception after logging


if __name__ == "__main__":
    # Substitua pelo caminho real do seu arquivo CSV
    csv_file_path = "data/olist_lz/olist_customers_dataset.csv"
    get_csv_data(csv_file_path, "olist_customers")
