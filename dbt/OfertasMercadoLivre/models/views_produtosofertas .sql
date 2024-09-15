{{
    config(
        materialized='view'
    )
}}

WITH cte AS (
  SELECT 
    nome_produto, 
    CAST(NULLIF(preco_atual, 'N/A') AS numeric) AS preco_atual, 
    CAST(NULLIF(preco_antigo, 'N/A') AS numeric) AS preco_antigo, -- Inclui o preço antigo
    CASE 
      WHEN frete_full = 'FRETE FULL' THEN 'Sim'
      ELSE 'Não'
    END AS frete_full,  -- Verifica se o frete é full
    (CAST(NULLIF(preco_antigo, 'N/A') AS numeric) - CAST(NULLIF(preco_atual, 'N/A') AS numeric)) AS valor_desconto, -- Calcula o valor do desconto
    ((CAST(NULLIF(preco_antigo, 'N/A') AS numeric) - CAST(NULLIF(preco_atual, 'N/A') AS numeric)) / CAST(NULLIF(preco_antigo, 'N/A') AS numeric)) * 100 AS porcentagem_ganho_desconto, -- Calcula a porcentagem de desconto
    porcentagem_desconto, -- Mantém a porcentagem original como referência
    oferta_do_dia,
    marca,
    loja_vendedora
  FROM 
    {{ source('OfertasMercadoLivre', 'dl_ofertascelularesml') }}
  WHERE 
    oferta_do_dia = 'OFERTA DO DIA'
    AND preco_atual IS NOT NULL
    AND preco_antigo IS NOT NULL  -- Certifica-se de que o preço antigo está presente
)

SELECT * FROM cte
