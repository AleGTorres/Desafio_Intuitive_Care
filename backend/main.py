from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from typing import Optional

DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME = "postgres", "password123", "127.0.0.1", "5432", "intuitive_care"

app = FastAPI(title="API Intuitive Care - Desafio Estágio")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_engine():
    conn_str = f"postgresql+psycopg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conn_str)

@app.get("/")
def read_root():
    return {"message": "Bem-vindo à API de Consulta ANS. Use /api/operadoras para buscar dados."}

@app.get("/api/operadoras")
def listar_operadoras(
    page: int = Query(1, ge=1), 
    limit: int = Query(10, ge=1, le=100),
    busca: Optional[str] = None
):
    offset = (page - 1) * limit
    engine = get_engine()
    try:
        with engine.connect() as conn:
            # Query base
            query_base = "SELECT registro_ans, cnpj, razao_social, uf FROM operadoras"
            params = {"limit": limit, "offset": offset}
            
            if busca:
                query_base += " WHERE razao_social ILIKE :busca OR cnpj ILIKE :busca"
                params["busca"] = f"%{busca}%"
            
            query_base += " ORDER BY razao_social LIMIT :limit OFFSET :offset"
            
            result = conn.execute(text(query_base), params)
            dados = [dict(row._mapping) for row in result]
            
            count_query = "SELECT count(*) FROM operadoras"
            if busca:
                count_query += " WHERE razao_social ILIKE :busca OR cnpj ILIKE :busca"
            total = conn.execute(text(count_query), {"busca": f"%{busca}%"} if busca else {}).scalar()
            
            return {
                "metadata": {
                    "total": total,
                    "page": page,
                    "limit": limit
                },
                "data": dados
            }
    finally:
        engine.dispose()

@app.get("/api/operadoras/{cnpj}")
def buscar_por_cnpj(cnpj: str):
    engine = get_engine()
    try:
        with engine.connect() as conn:
            query = text("SELECT * FROM operadoras WHERE cnpj = :cnpj")
            result = conn.execute(query, {"cnpj": cnpj}).fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Operadora não encontrada.")
            return dict(result._mapping)
    finally:
        engine.dispose()

@app.get("/api/estatisticas")
def obter_estatisticas():
    engine = get_engine()
    try:
        with engine.connect() as conn:
            total = conn.execute(text("SELECT SUM(total_despesas) FROM despesas_estatisticas")).scalar()
            media = conn.execute(text("SELECT AVG(total_despesas) FROM despesas_estatisticas")).scalar()
            
            top5_query = text("SELECT razao_social, total_despesas FROM despesas_estatisticas ORDER BY total_despesas DESC LIMIT 5")
            top5 = [dict(row._mapping) for row in conn.execute(top5_query)]
            
            return {
                "total_despesas": round(float(total or 0), 2),
                "media_geral": round(float(media or 0), 2),
                "top_5_operadoras": top5
            }
    finally:
        engine.dispose()