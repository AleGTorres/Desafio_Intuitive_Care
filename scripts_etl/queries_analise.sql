SELECT 
    razao_social, 
    uf, 
    total_despesas 
FROM despesas_estatisticas
ORDER BY total_despesas DESC
LIMIT 5;

SELECT 
    uf,
    SUM(total_despesas) AS despesas_totais,
    ROUND(AVG(total_despesas), 2) AS media_por_operadora,
    COUNT(razao_social) AS qtd_operadoras
FROM despesas_estatisticas
GROUP BY uf
ORDER BY despesas_totais DESC
LIMIT 5;

SELECT 
    razao_social, 
    uf, 
    desvio_padrao,
    media_trimestral
FROM despesas_estatisticas
WHERE desvio_padrao > (SELECT AVG(desvio_padrao) FROM despesas_estatisticas)
ORDER BY desvio_padrao DESC;