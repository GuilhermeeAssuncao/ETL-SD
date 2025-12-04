# ETL-SD
import pandas as pd
import sqlite3
import logging
from datetime import datetime

# ---------------------------------------------------------
# 📝 CONFIGURAÇÃO DE LOGS E PARÂMETROS
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

CONFIG = {
    "input_file": "dados.csv",
    "db_path": "meu_banco.db",
    "table_name": "vendas"
}

# ---------------------------------------------------------
# 🔍 ETAPA 1 — EXTRACT
# ---------------------------------------------------------
def extract(path: str) -> pd.DataFrame:
    logging.info("▶️ Iniciando extração de dados...")

    try:
        df = pd.read_csv(path)
        logging.info(f"📄 {len(df)} registros extraídos.")
    except Exception as e:
        logging.error(f"❌ Erro ao extrair dados: {e}")
        raise

    return df


# ---------------------------------------------------------
# 🔧 ETAPA 2 — TRANSFORM
# ---------------------------------------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("🔧 Iniciando transformação...")

    # Padronização
    df.columns = [c.lower().strip() for c in df.columns]

    # Remoção de duplicados
    before = len(df)
    df = df.drop_duplicates()
    logging.info(f"🧹 Removidos {before - len(df)} registros duplicados.")

    # Limpeza de nulos
    df = df.dropna(how="any")

    # Conversões inteligentes
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")

    # Novas métricas
    if "preco" in df.columns and "quantidade" in df.columns:
        df["valor_total"] = df["preco"] * df["quantidade"]

    # Validação simples
    if df.isnull().sum().sum() > 0:
        logging.warning("⚠️ Ainda existem valores nulos após transformação.")

    logging.info("✔️ Transformação concluída.")
    return df


# ---------------------------------------------------------
# 📦 ETAPA 3 — LOAD (UPERT: UPDATE + INSERT)
# ---------------------------------------------------------
def load(df: pd.DataFrame, db_path: str, table_name: str):
    logging.info("📦 Iniciando carga dos dados...")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Criar tabela se não existir
        df.to_sql(table_name, conn, if_exists="append", index=False)

        logging.info(f"✔️ {len(df)} registros carregados em '{table_name}'.")
    except Exception as e:
        logging.error(f"❌ Erro na carga: {e}")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------
# 🚀 PIPELINE COMPLETO
# ---------------------------------------------------------
def run_etl(config=CONFIG):
    logging.info("🚀 Pipeline ETL iniciado...")

    df = extract(config["input_file"])
    df = transform(df)
    load(df, config["db_path"], config["table_name"])

    logging.info("🏁 Pipeline ETL finalizado com sucesso.")


# Executar
if __name__ == "__main__":
    run_etl()
