-- La prueba falla si el resumen no coincide
-- con los registros del modelo de calidad.

WITH esperado AS (
    SELECT
        fuente,
        tipo_propiedad,
        moneda,
        COUNT(*) AS total_publicaciones,
        COUNT(*) FILTER (
            WHERE estado_calidad_precio IN (
                'precio_muy_bajo',
                'precio_extremadamente_alto'
            )
        ) AS publicaciones_extremas,
        COUNT(*) FILTER (
            WHERE alerta_uf
        ) AS publicaciones_alerta_uf
    FROM {{ ref('calidad_precios_publicaciones') }}
    GROUP BY fuente, tipo_propiedad, moneda
),

observado AS (
    SELECT
        fuente,
        tipo_propiedad,
        moneda,
        total_publicaciones,
        publicaciones_extremas,
        publicaciones_alerta_uf
    FROM {{ ref('resumen_precios_comparativo') }}
),

faltantes AS (
    SELECT * FROM esperado
    EXCEPT ALL
    SELECT * FROM observado
),

sobrantes AS (
    SELECT * FROM observado
    EXCEPT ALL
    SELECT * FROM esperado
)

SELECT * FROM faltantes
UNION ALL
SELECT * FROM sobrantes