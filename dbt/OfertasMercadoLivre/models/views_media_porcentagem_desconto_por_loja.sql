{{
    config(
        materialized='view'
    )
}}

WITH cte AS (
  SELECT 
    loja_vendedora,
    AVG(CAST(REGEXP_REPLACE(porcentagem_desconto, '[^0-9]', '', 'g') AS numeric)) AS media_porcentagem_desconto
  FROM 
    {{ source('OfertasMercadoLivre', 'dl_ofertascelularesml') }}
  WHERE 
    porcentagem_desconto IS NOT NULL
    AND REGEXP_REPLACE(porcentagem_desconto, '[^0-9]', '', 'g') <> ''
  GROUP BY 
    loja_vendedora
)

SELECT * FROM cte
