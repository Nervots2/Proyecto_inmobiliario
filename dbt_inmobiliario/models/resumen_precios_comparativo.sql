{{ config(materialized='table') }}

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
    ) AS publicaciones_alerta_uf,

    ROUND(
        AVG(precio), 2
    ) AS promedio_original,

    ROUND(
        AVG(precio) FILTER (
            WHERE estado_calidad_precio =
                'sin_alerta_por_umbral'
        ), 2
    ) AS promedio_sin_extremos,

    ROUND((
        PERCENTILE_CONT(0.5)
        WITHIN GROUP (ORDER BY precio)
    )::numeric, 2) AS mediana_original,

    ROUND((
        PERCENTILE_CONT(0.5)
        WITHIN GROUP (ORDER BY precio)
        FILTER (
            WHERE estado_calidad_precio =
                'sin_alerta_por_umbral'
        )
    )::numeric, 2) AS mediana_sin_extremos

FROM {{ ref('calidad_precios_publicaciones') }}

GROUP BY
    fuente,
    tipo_propiedad,
    moneda