{{ config(materialized='table') }}

-- Seleccionar la extracción más reciente de cada publicación
WITH publicaciones_recientes AS (

    SELECT
        id_registro,
        id_publicacion,
        fuente,
        id_propiedad,
        precio,
        moneda,
        fecha_extraccion,

        ROW_NUMBER() OVER (
            PARTITION BY fuente, id_publicacion
            ORDER BY fecha_extraccion DESC, id_registro DESC
        ) AS numero_fila

    FROM dw.fact_publicaciones
)

-- Generar indicadores inmobiliarios
SELECT
    d.tipo_propiedad,
    p.moneda,

    COUNT(*) AS total_publicaciones,

    ROUND(
        AVG(p.precio), 2
    ) AS precio_promedio,

    ROUND(
        (
            PERCENTILE_CONT(0.5)
            WITHIN GROUP (
                ORDER BY p.precio::double precision
            )
        )::numeric,
        2
    ) AS mediana_precio

FROM publicaciones_recientes p

INNER JOIN dw.dim_propiedad d
    ON p.id_propiedad = d.id_propiedad

WHERE
    p.numero_fila = 1
    AND p.precio > 0
    AND p.moneda IN ('UF', 'CLP')

GROUP BY
    d.tipo_propiedad,
    p.moneda