# Teste Técnico - Intuitive Care

Repositório destinado à resolução do teste para estágio da Intuitive Care.

## Como Executar o Projeto
1. Ter o **Docker** e o **Python 3.10+** instalados.
2. Clone o repositório.
3. No terminal, execute: `docker-compose up -d` para subir o banco de dados.
4. Instale as dependências: `pip install -r requirements.txt`.
5. Execute a ingestão: `python scripts_etl/ingestion.py`.
6. Execute a consolidação: `python scripts_etl/consolidation.py`.
7. Execute a transformação: `python scripts_etl/transformation.py`.
8. Execute a carga no banco: `python scripts_etl/load_to_db.py`.
9. Para iniciar o servidor da API, navegue até a pasta `/backend` (`cd backend`) e execute `uvicorn main:app --reload`.
10. Para visualizar a interface, abra o arquivo `frontend/index.html` no seu navegador de preferência.

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

### 8. Modelagem e Persistência de Dados (Teste 3)
- Estruturação de Schema (DDL): Criação explícita de tabelas com chaves primárias e tipos de dados rigorosos antes da ingestão.
- Trade-off: Em vez de deixar o Pandas criar as tabelas automaticamente (o que geraria tipos genéricos), optei por definir o Schema via SQL para garantir a integridade referencial e performance em futuras consultas.
- Conexão Resiliente: Implementação de lógica de "Auto-healing" no script de carga que utiliza o banco padrão postgres para verificar e criar o banco intuitive_care caso ele não exista.
- Trade-off: Escolha do driver psycopg (v3) em conjunto com SQLAlchemy para operar em modo `AUTOCOMMIT` durante a criação do database. Utilizei essa abordagem para evitar erros de transações ativas do PostgreSQL e isolar o processo de carga de instabilidades do ambiente Docker no Windows.
- Padronização de Encoding: Configuração do banco com `UTF8` e `locale C` no Docker Compose.
- Justificativa: Essa decisão visa mitigar erros de decodificação de caracteres especiais comuns em sistemas Windows.

### 9. Interface de Consulta e API
#### 9.1 - Backend com FastAPI
- Escolhi o FastAPI ao invés do Flask.
- Trade-off (4.2.1): Optei pelo FastAPI devido à sua performance assíncrona superior e à geração automática de documentação via Swagger/OpenAPI, o que facilita imensamente a fase de testes e integração com o frontend.
- Trade-off (4.2.2 e 4.2.4): Utilizei a "Opção A: Offset-based" pois, dado o volume de dados e a baixa frequência de atualizações em tempo real, ela oferece a melhor experiência de navegação (UX) com custo de implementação reduzido. A resposta inclui Dados + Metadados (Opção B) para que o frontend tenha ciência do total de registros e possa renderizar os controles de página corretamente.
- Trade-off (4.2.3): Para a rota de estatísticas, utilizei Queries Diretas (Opção A). Como o banco de dados é atualizado via processo ETL pontual e o volume de operadoras ativas permite cálculos agregados em milissegundos, o cache adicionaria complexidade desnecessária sem ganho perceptível de performance neste momento.

#### 9.2 - Frontend com Vue.js
- Busca realizada no servidor.
- Trade-off (4.3.1): Em vez de filtrar os dados no cliente, a interface envia o termo de busca para a API. Isso garante que a aplicação permaneça rápida mesmo que a base de dados cresça para milhares de registros.
- Para visualizar os dados, utilizei a biblioteca Chart.js para renderizar o gráfico de distribuição de despesas.
- Oferece uma representação visual clara das maiores operadoras por despesa, atendendo ao requisito de análise de dados do desafio.
- Trade-ff (4.3.2): No Vue.js, utilizei Props/Events simples (Opção A). Justifico essa escolha pela baixa complexidade da aplicação (Single Page Dashboard); o uso de Pinia ou Vuex seria um over-engineering, ferindo o princípio de praticidade (KISS).

### 10. Padronização de Encoding e Integridade (Análise Crítica)
- Problema Identificado: Inconsistência de caracteres (ex: SAÃšDE) devido ao conflito entre a codificação ISO-8859-1 da ANS e o padrão UTF-8 moderno.
- Solução Implementada: Criação de uma função dedicada `fix_encoding` que re-codifica strings de `latin1` para `utf-8`.
- Justificativa: Diferente de apenas ignorar os erros, essa abordagem garante que a informação original seja recuperada e exibida corretamente tanto no banco de dados quanto na interface web, elevando a qualidade do produto final.

### 11. Garantia de Qualidade e Validação
- Sanity Check de Dados: Realizei a conferência cruzada entre os arquivos brutos da ANS e o banco de dados PostgreSQL para garantir que o processo de ETL não causasse perda de registros.
- Validação de Regras de Negócio: Testei manualmente o endpoint de busca (`/api/operadoras`) com casos de borda (ex: nomes com caracteres especiais e CNPJs com zeros à esquerda) para validar a eficácia da função `fix_encoding`.
- Análise de Contrato da API: Utilizei o Swagger (FastAPI) para validar se os tipos de dados retornados (Strings, Decimais, Inteiros) estavam em conformidade com o planejado no Teste 3.
- Cross-Browser Testing: Validação visual da interface Vue.js no navegador para garantir que a paginação e o gráfico de despesas fossem renderizados corretamente após o Join das tabelas.
- Documentação de UX: A interface trata estados de Loading durante as requisições assíncronas e exibe mensagens de Dados Vazios  caso a busca não retorne resultados. Optei por mensagens específicas em vez de genéricas para facilitar o entendimento do usuário final.