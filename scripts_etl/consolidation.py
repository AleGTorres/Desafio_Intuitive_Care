import pandas as pd
import os
import glob

OUTPUT_DIR = "data/processed"
FINAL_CSV = os.path.join(OUTPUT_DIR, "consolidado_despesas.csv")
FINAL_COLUMNS = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'Valor Despesas']

def extract_date_info(date_str):
    """Extrai Ano e Trimestre (Requisito 1.3)."""
    try:
        date = pd.to_datetime(date_str)
        trimestre = (date.month - 1) // 3 + 1
        return date.year, trimestre
    except:
        return None, None

def run_consolidation():
    """Consolida os dados tratando as inconsistências reais encontradas."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    raw_files = glob.glob("data/raw/*.csv")
    
    all_data = []

    for file in raw_files:
        print(f"Processando: {os.path.basename(file)}")
        # Processamento incremental
        reader = pd.read_csv(file, sep=';', encoding='latin-1', chunksize=50000, dtype=str)
        
        for chunk in reader:
            # filtro de Despesas com Eventos/Sinistros
            mask = chunk['DESCRICAO'].str.contains('EVENTOS|SINISTROS', case=False, na=False)
            filtered = chunk[mask].copy()
            
            if not filtered.empty:
                # substitui vírgula por ponto e limpa negativos
                filtered['Valor Despesas'] = pd.to_numeric(
                    filtered['VL_SALDO_FINAL'].str.replace(',', '.'), errors='coerce'
                ).fillna(0)
                filtered.loc[filtered['Valor Despesas'] < 0, 'Valor Despesas'] = 0
                
                filtered['Ano'], filtered['Trimestre'] = zip(*filtered['DATA'].map(extract_date_info))
                
                # manter REG_ANS para o JOIN com o CNPJ
                all_data.append(filtered[['REG_ANS', 'Ano', 'Trimestre', 'Valor Despesas']])

    if not all_data:
        print("Nenhum dado de despesa encontrado.")
        return

    df_final = pd.concat(all_data, ignore_index=True)
    
    df_consolidado = df_final.groupby(['REG_ANS', 'Ano', 'Trimestre'], as_index=False).sum()

    df_consolidado.to_csv(FINAL_CSV, index=False, encoding='utf-8')
    print(f"Sucesso! Arquivo gerado: {FINAL_CSV}")

if __name__ == "__main__":
    run_consolidation()