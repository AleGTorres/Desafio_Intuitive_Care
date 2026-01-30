# Teste Técnico - Intuitive Care

Repositório destinado à resolução do teste para estágio da Intuitive Care.

## Como Executar o Projeto
1. Ter o **Docker** e o **Python 3.10+** instalados.
2. Clone o repositório.
3. No terminal, execute: `docker-compose up -d` para subir o banco de dados.
4. Instale as dependências: `pip install -r requirements.txt`

## Decisões Técnicas e Trade-offs
### 1. Linguagem e Frameworks
Optei por Python devido à sua robustez em Engenharia de Dados (Pandas) e rapidez na criação de APIs (FastAPI).

### 2. Banco de Dados (PostgreSQL)
Escolhi o PostgreSQL por sua conformidade ACID e suporte superior ao tipo `DECIMAL`, essencial para garantir a precisão em valores de despesas médicas.

### 3. Estratégia de Processamento
Para o processamento dos arquivos da ANS, utilizarei **processamento incremental (chunking)**. Embora os dados atuais possam caber na memória, essa decisão vai ajudar a garantir que o sistema seja resiliente a grandes aumentos no volume de dados.