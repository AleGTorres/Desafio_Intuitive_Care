import requests
from bs4 import BeautifulSoup
import os
import re
import zipfile
import io

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
OUTPUT_DIR = "data/raw"

def get_latest_quarters_and_files(url, count=3):
    """
    Navega na ANS e identifica os arquivos dos últimos 3 trimestres.
    Resiliência [1.1]: Lida com arquivos ZIP direto na pasta do ano ou em subpastas.
    """
    print(f"Buscando diretórios em: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar a ANS: {response.status_code}")

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Identifica pastas de anos
    years = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and re.match(r'^\d{4}/$', href):
            years.append(href)
    
    years.sort(reverse=True)
    
    zip_files_to_download = []
    
    for year in years:
        if len(zip_files_to_download) >= count:
            break
            
        year_url = url + year
        print(f"Verificando ano: {year.strip('/')}")
        res_year = requests.get(year_url)
        soup_year = BeautifulSoup(res_year.text, 'html.parser')
        
        # Busca ZIPs diretamente na pasta do ano
        current_year_zips = []
        for link in soup_year.find_all('a'):
            href = link.get('href')
            if href and href.lower().endswith('.zip'):
                current_year_zips.append((year_url + href, href))
        
        if current_year_zips:
            # Ordena para para pegar os últimos trimestres primeiro
            current_year_zips.sort(key=lambda x: x[1], reverse=True)
            zip_files_to_download.extend(current_year_zips)
        else:
            # Busca subpastas de trimestres
            sub_quarters = []
            for link in soup_year.find_all('a'):
                href = link.get('href')
                if href and re.match(r'^[1-4]/$', href):
                    sub_quarters.append(href)
            
            sub_quarters.sort(reverse=True)
            for q_folder in sub_quarters:
                if len(zip_files_to_download) >= count:
                    break
                q_url = year_url + q_folder
                res_q = requests.get(q_url)
                soup_q = BeautifulSoup(res_q.text, 'html.parser')
                for q_link in soup_q.find_all('a'):
                    q_href = q_link.get('href')
                    if q_href and q_href.lower().endswith('.zip'):
                        zip_files_to_download.append((q_url + q_href, q_href))

    return zip_files_to_download[:count]

def download_and_extract(zip_list):
    """
    Baixa e extrai arquivos ZIP automaticamente.
    Requisito [1.2]: Extração automática e persistência em diretório.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    extracted_paths = []
    
    for file_url, file_name in zip_list:
        print(f"Baixando e extraindo: {file_name}")
        try:
            res = requests.get(file_url)
            res.raise_for_status()
            
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                z.extractall(OUTPUT_DIR)
                for name in z.namelist():
                    full_path = os.path.join(OUTPUT_DIR, name)
                    extracted_paths.append(full_path)
        except Exception as e:
            print(f"Erro ao processar {file_name}: {e}")
            
    return extracted_paths

def filter_relevant_files(file_list):
    """
    Identifica apenas arquivos de 'Despesas com Eventos/Sinistros'.
    Requisito [1.2]: Identificação automática entre formatos variados.
    """
    keywords = ["despesas", "sinistros", "eventos"]
    relevant = [
        f for f in file_list 
        if any(key in f.lower() for key in keywords) 
        and f.lower().endswith(('.csv', '.txt', '.xlsx', '.csv'))
    ]
    print(f"Total de arquivos extraídos: {len(file_list)}")
    print(f"Arquivos relevantes para despesas: {len(relevant)}")
    return relevant

def run_ingestion():
    """Coordena a execução do Teste 1.1 e 1.2."""
    print("--- Iniciando Ingestão de Dados ANS ---")
    
    targets = get_latest_quarters_and_files(BASE_URL)
    
    all_extracted = download_and_extract(targets)

    relevant_files = filter_relevant_files(all_extracted)
    
    print("--- Ingestão Finalizada com Sucesso ---")
    return relevant_files

if __name__ == "__main__":
    run_ingestion()