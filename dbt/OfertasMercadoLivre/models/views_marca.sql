{{ 
    config(
        materialized='view'
    ) 
}}

WITH cte AS (
  SELECT 
    marca,
    ROW_NUMBER() OVER (ORDER BY marca) AS id
  FROM 
    {{ source('OfertasMercadoLivre', 'dl_ofertascelularesml') }}  
    marca
)

SELECT * FROM cte
