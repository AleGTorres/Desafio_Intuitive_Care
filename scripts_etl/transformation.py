import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
import os
import re
from urllib.parse import urljoin

CONSOLIDATED_CSV = "data/processed/consolidado_despesas.csv"
CADASTRO_DIR_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
FINAL_AGREGADO_CSV = "data/processed/despesas_agregadas.csv"

def get_dynamic_csv_url(directory_url):
    """Varre o diretório para encontrar o link do CSV de cadastro."""
    print(f"Buscando link do cadastro em: {directory_url}")
    response = requests.get(directory_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.lower().endswith('.csv'):
            return urljoin(directory_url, href)
    raise Exception("Não foi possível encontrar nenhum arquivo CSV no diretório informado.")

def validate_cnpj(cnpj):
    """Implementa validação de dígitos verificadores do CNPJ."""
    cnpj = re.sub(r'\D', '', str(cnpj))
    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False
    def calc(c, w):
        s = sum(int(d) * v for d, v in zip(c, w))
        r = s % 11
        return 0 if r < 2 else 11 - r
    if int(cnpj[12]) != calc(cnpj[:12], [5,4,3,2,9,8,7,6,5,4,3,2]): return False
    if int(cnpj[13]) != calc(cnpj[:13], [6,5,4,3,2,9,8,7,6,5,4,3,2]): return False
    return True

def run_transformation():
    print("--- Iniciando Transformação e Validação Dinâmica ---")
    
    df_despesas = pd.read_csv(CONSOLIDATED_CSV, dtype={'REG_ANS': str})
    
    # localizar e baixar cadastro dinamicamente
    try:
        csv_url = get_dynamic_csv_url(CADASTRO_DIR_URL)
        print(f"Arquivo identificado: {csv_url}")
        res = requests.get(csv_url)
        df_cadastro = pd.read_csv(io.BytesIO(res.content), sep=';', encoding='latin-1', dtype=str)
    except Exception as e:
        print(f"Erro ao obter cadastro: {e}")
        return

    # limpeza de nomes de colunas
    df_cadastro.columns = [c.strip() for c in df_cadastro.columns]

    # identificar a coluna de Registro 
    col_reg = 'REGISTRO_OPERADORA' if 'REGISTRO_OPERADORA' in df_cadastro.columns else 'Registro_ANS'

    # join e enriquecimento
    df_merged = pd.merge(
        df_despesas, 
        df_cadastro[[col_reg, 'CNPJ', 'Razao_Social', 'UF']], 
        left_on='REG_ANS', 
        right_on=col_reg, 
        how='inner'
    )

    # validação de CNPJ
    df_merged['CNPJ_Valido'] = df_merged['CNPJ'].apply(validate_cnpj)
    df_clean = df_merged[df_merged['CNPJ_Valido'] == True].copy()
    
    # agregação e estatísticas
    agg_df = df_clean.groupby(['Razao_Social', 'UF']).agg(
        Total_Despesas=('Valor Despesas', 'sum'),
        Media_Trimestral=('Valor Despesas', 'mean'),
        Desvio_Padrao=('Valor Despesas', 'std')
    ).reset_index()

    agg_df['Desvio_Padrao'] = agg_df['Desvio_Padrao'].fillna(0)
    agg_df.sort_values(by='Total_Despesas', ascending=False, inplace=True)
    
    os.makedirs(os.path.dirname(FINAL_AGREGADO_CSV), exist_ok=True)
    agg_df.to_csv(FINAL_AGREGADO_CSV, index=False, encoding='utf-8')
    print(f"Sucesso! Módulo 2 finalizado. Arquivo: {FINAL_AGREGADO_CSV}")

if __name__ == "__main__":
    run_transformation()