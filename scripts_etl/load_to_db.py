import pandas as pd
from sqlalchemy import create_engine, text
import requests
import io
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin

DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME = "postgres", "password123", "127.0.0.1", "5432", "intuitive_care"
AGREGADO_CSV = "data/processed/despesas_agregadas.csv"
CADASTRO_DIR_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"


def fix_encoding(series):
    """
    Corrige textos que foram lidos como ISO-8859-1 (latin-1)
    mas originalmente estavam em UTF-8 (ex: SAÃšDE -> SAÚDE).
    """
    return series.apply(
        lambda x: x.encode("latin1").decode("utf-8") if isinstance(x, str) else x
    )


def get_engine(db="postgres"):
    """Cria conexão com o banco especificado usando o driver psycopg[cite: 13]."""
    conn_str = f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{db}"
    return create_engine(conn_str)

def setup_database():
    """Garante que o banco de dados exista[cite: 94]."""
    engine_init = get_engine("postgres")
    with engine_init.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        res = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}'"))
        if not res.fetchone():
            print(f"Criando banco '{DB_NAME}'...")
            conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
    engine_init.dispose()

def create_schema(engine):
    """Cria a estrutura das tabelas conforme o Teste 3.2[cite: 101, 104]."""
    queries = [
        """
        CREATE TABLE IF NOT EXISTS operadoras (
            registro_ans VARCHAR(20) PRIMARY KEY,
            cnpj VARCHAR(14),
            razao_social VARCHAR(255),
            uf CHAR(2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS despesas_estatisticas (
            id SERIAL PRIMARY KEY,
            razao_social VARCHAR(255),
            uf CHAR(2),
            total_despesas DECIMAL(18, 2),
            media_trimestral DECIMAL(18, 2),
            desvio_padrao DECIMAL(18, 2)
        );
        """
    ]
    with engine.connect() as conn:
        for q in queries:
            conn.execute(text(q))
        conn.commit()

def load_data():
    setup_database()
    engine = get_engine(DB_NAME)
    create_schema(engine)
    
    try:
        print("Obtendo dados cadastrais da ANS...")
        res = requests.get(CADASTRO_DIR_URL)
        soup = BeautifulSoup(res.text, 'html.parser')
        csv_url = urljoin(CADASTRO_DIR_URL, [a.get('href') for a in soup.find_all('a') if a.get('href').endswith('.csv')][0])
         
        response = requests.get(csv_url)
        df_cad = pd.read_csv(io.BytesIO(response.content), sep=';', encoding='latin-1', dtype=str)
        
        df_cad.columns = [c.strip() for c in df_cad.columns]
        col_reg = 'REGISTRO_OPERADORA' if 'REGISTRO_OPERADORA' in df_cad.columns else 'Registro_ANS'
        
        df_to_db = df_cad[[col_reg, 'CNPJ', 'Razao_Social', 'UF']].copy()
        df_to_db.columns = ['registro_ans', 'cnpj', 'razao_social', 'uf']
        df_to_db['razao_social'] = fix_encoding(df_to_db['razao_social'])
        df_to_db['razao_social'] = df_to_db['razao_social'].str.strip()
        
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE operadoras CASCADE;"))
            conn.commit()
            
        df_to_db.to_sql('operadoras', engine, if_exists='append', index=False)
        print("Tabela 'operadoras' carregada com sucesso.")
        
    except Exception as e:
        print(f"Erro no cadastro: {e}")

    if os.path.exists(AGREGADO_CSV):
        print("Carregando despesas agregadas...")
        df_agg = pd.read_csv(AGREGADO_CSV, encoding='utf-8-sig')
        
        df_agg.columns = ['razao_social', 'uf', 'total_despesas', 'media_trimestral', 'desvio_padrao']
        df_agg['razao_social'] = df_agg['razao_social'].str.strip()
        
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE despesas_estatisticas;"))
            conn.commit()
            
        df_agg.to_sql('despesas_estatisticas', engine, if_exists='append', index=False)
        print("Tabela 'despesas_estatisticas' carregada com sucesso.")

if __name__ == "__main__":
    load_data()