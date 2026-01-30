# Teste Técnico - Intuitive Care

Repositório destinado à resolução do teste para estágio da Intuitive Care.

## Como Executar o Projeto
1. Ter o **Docker** e o **Python 3.10+** instalados.
2. Clone o repositório.
3. No terminal, execute: `docker-compose up -d` para subir o banco de dados.
4. Instale as dependências: `pip install -r requirements.txt`.
5. Execute a ingestão: `python scripts_etl/ingestion.py`.
6. Execute a consolidação: `python scripts_etl/consolidation.py`.

## Decisões Técnicas e Trade-offs
### 1. Linguagem e Frameworks
- Optei por Python devido à sua robustez em Engenharia de Dados (Pandas) e rapidez na criação de APIs (FastAPI).

### 2. Banco de Dados (PostgreSQL)
- Escolhi o PostgreSQL por sua conformidade ACID e suporte superior ao tipo `DECIMAL`, essencial para garantir a precisão em valores de despesas médicas.

### 3. Ingestão Resiliente (Teste 1.1)
Implementação de scraping dinâmico com BeautifulSoup.
- Trade-off: Em vez de caminhos estáticos, o código varre o diretório da ANS buscando padrões de pastas e arquivos ZIP.
- Escolhi essa abordagem para garantitrque o processo não quebre caso encontre variações estruturais do site da ANS (arquivos em subpastas vs. diretório raiz).

### 4. Processamento Incremental (Teste 1.2)
Uso de Chunking (Pandas).
- Trade-off: Processamento incremental em vez de carga total em memória.
- Essa decisão vai ajudar a garantir que o sistema seja resiliente a grandes aumentos no volume de dados.

### 5. Tratamento de Inconsistências (Teste 1.3)
- Os valores negativos foram convertidos para zero para preservar a integridade do volume bruto de despesas.
- Os CNPJs duplicados foram normalizados pela Razão Social mais recente, garantindo consistência histórica.
- Mapeamento de Chaves: Uso do REG_ANS como identificador primário na consolidação, visto que o CNPJ não consta nos arquivos brutos de despesas e será adicionado via Join.

### 6. Validação e Enriquecimento Dinâmico (Teste 2.1 e 2.2)
- Validação de CNPJ: Implementação do algoritmo de dígitos verificadores para garantir a integridade da base. CNPJs inválidos são descartados para não poluir as métricas estatísticas.
- Scraping de Cadastro: O script de transformação localiza automaticamente o link do CSV de operadoras ativas no portal da ANS.
- Trade-off: Utilização de Inner Join entre as bases de Despesas e Cadastro.
- A justificativa para essa escolha é garantir que apenas operadoras devidamente identificadas e com CNPJ válido sejam incluídas no relatório final de despesas agregadas.

### 7. Agregação Estatística (Teste 2.3)
- Cálculo de Média e Desvio Padrão trimestral por operadora e UF.
- Escolhi o desvio padrão porque ele ajuda a mostrar o quanto as despesas médicas variam ao longo dos trimestres, trazendo uma visão mais clara do risco operacional.