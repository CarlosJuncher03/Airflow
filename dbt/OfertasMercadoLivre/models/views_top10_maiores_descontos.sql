{{
    config(
        materialized='view'
    )
}}

WITH cte AS (
  SELECT 
    nome_produto,
    marca,
    loja_vendedora AS loja,
    CAST(NULLIF(preco_atual, 'N/A') AS numeric) AS valor_novo,
    CAST(NULLIF(preco_antigo, 'N/A') AS numeric) AS valor_antigo,
    (CAST(NULLIF(preco_antigo, 'N/A') AS numeric) - CAST(NULLIF(preco_atual, 'N/A') AS numeric)) AS valor_desconto,
    ((CAST(NULLIF(preco_antigo, 'N/A') AS numeric) - CAST(NULLIF(preco_atual, 'N/A') AS numeric)) / CAST(NULLIF(preco_antigo, 'N/A') AS numeric)) * 100 AS porcentagem_ganho_desconto
  FROM 
    {{ source('OfertasMercadoLivre', 'dl_ofertascelularesml') }}
  WHERE 
    preco_antigo IS NOT NULL 
    AND preco_atual IS NOT NULL
    AND (CAST(NULLIF(preco_antigo, 'N/A') AS numeric) - CAST(NULLIF(preco_atual, 'N/A') AS numeric)) > 0
)

SELECT * FROM cte
ORDER BY 
    valor_desconto DESC
LIMIT 10
