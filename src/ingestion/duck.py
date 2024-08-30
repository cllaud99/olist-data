import os
import sys
from typing import List

import duckdb

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.logger_config import logger


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
    csv_file_path = "data/olist_lz/olist_customers_dataset.csv"
    get_csv_data(csv_file_path, "olist_customers")
