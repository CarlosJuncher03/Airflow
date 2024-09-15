{{
    config(
        materialized='view'
    )
}}

WITH cte AS (
  SELECT 
    SUBSTRING(loja_vendedora, 4) AS loja_vendedora_ajustada, -- Pega os caracteres a partir do 4º
    ROW_NUMBER() OVER (ORDER BY loja_vendedora) AS id
  FROM 
    {{ source('OfertasMercadoLivre', 'dl_ofertascelularesml') }}  
  GROUP BY 
    loja_vendedora
)

SELECT * FROM cte
