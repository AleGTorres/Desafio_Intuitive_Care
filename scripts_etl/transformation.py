import pandas as pd
import requests
from bs4 import BeautifulSoup
import io
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
    """Validação dos dígitos verificadores do CNPJ."""
    cnpj = re.sub(r'\D', '', str(cnpj))

    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False

    def calc(c, w):
        s = sum(int(d) * v for d, v in zip(c, w))
        r = s % 11
        return 0 if r < 2 else 11 - r

    if int(cnpj[12]) != calc(cnpj[:12], [5,4,3,2,9,8,7,6,5,4,3,2]):
        return False
    if int(cnpj[13]) != calc(cnpj[:13], [6,5,4,3,2,9,8,7,6,5,4,3,2]):
        return False

    return True


def fix_encoding(series: pd.Series) -> pd.Series:
    """
    Corrige textos que foram lidos como ISO-8859-1
    mas originalmente estavam em UTF-8 (ex: SAÃšDE).
    """
    return series.apply(
        lambda x: x.encode("latin1").decode("utf-8") if isinstance(x, str) else x
    )


def run_transformation():
    print("--- Iniciando Transformação e Validação ---")

    df_despesas = pd.read_csv(
        CONSOLIDATED_CSV,
        dtype={'REG_ANS': str},
        encoding='iso-8859-1'
    )

    try:
        csv_url = get_dynamic_csv_url(CADASTRO_DIR_URL)
        res = requests.get(csv_url)

        df_cadastro = pd.read_csv(
            io.BytesIO(res.content),
            sep=';',
            encoding='iso-8859-1',
            dtype=str
        )
    except Exception as e:
        print(f"Erro ao obter cadastro: {e}")
        return

    df_cadastro.columns = [c.strip() for c in df_cadastro.columns]

    col_reg = (
        'REGISTRO_OPERADORA'
        if 'REGISTRO_OPERADORA' in df_cadastro.columns
        else 'Registro_ANS'
    )

    df_merged = pd.merge(
        df_despesas,
        df_cadastro[[col_reg, 'CNPJ', 'Razao_Social', 'UF']],
        left_on='REG_ANS',
        right_on=col_reg,
        how='inner'
    )

    df_merged['Razao_Social'] = fix_encoding(df_merged['Razao_Social'])

    df_merged['CNPJ_Valido'] = df_merged['CNPJ'].apply(validate_cnpj)
    df_clean = df_merged[df_merged['CNPJ_Valido']].copy()

    agg_df = (
        df_clean
        .groupby(['Razao_Social', 'UF'])
        .agg(
            Total_Despesas=('Valor Despesas', 'sum'),
            Media_Trimestral=('Valor Despesas', 'mean'),
            Desvio_Padrao=('Valor Despesas', 'std')
        )
        .reset_index()
    )

    agg_df['Desvio_Padrao'] = agg_df['Desvio_Padrao'].fillna(0)
    agg_df.sort_values(by='Total_Despesas', ascending=False, inplace=True)

    agg_df.to_csv(
        FINAL_AGREGADO_CSV,
        index=False,
        encoding='utf-8-sig'
    )

    print(f"Sucesso! Arquivo gerado corretamente: {FINAL_AGREGADO_CSV}")

if __name__ == "__main__":
    run_transformation()