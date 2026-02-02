SELECT razao_social, uf, total_despesas
FROM despesas_estatisticas
ORDER BY total_despesas DESC
LIMIT 10;

SELECT uf, ROUND(AVG(total_despesas), 2) as media_por_estado
FROM despesas_estatisticas
GROUP BY uf
ORDER BY media_por_estado DESC;