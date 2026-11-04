{{ config(materialized='table') }}

SELECT
    p.id_registro,
    p.id_publicacion,
    p.fuente,
    d.tipo_propiedad,
    p.moneda,
    p.precio,
    p.id_ubicacion,

    CASE
        WHEN p.precio IS NULL THEN 'precio_nulo'
        WHEN p.precio <= 0 THEN 'precio_no_positivo'
        WHEN p.fuente = 'chilepropiedades'
             AND p.moneda = 'CLP'
             AND p.precio <= 10000
            THEN 'precio_muy_bajo'
        WHEN p.fuente = 'chilepropiedades'
             AND p.moneda = 'CLP'
             AND p.precio >= 10000000000
            THEN 'precio_extremadamente_alto'
        ELSE 'sin_alerta_por_umbral'
    END AS estado_calidad_precio,

    (a.id_registro IS NOT NULL) AS alerta_uf,

    a.precio_uf_original,
    a.precio_uf_titulo_candidata,
    a.estado AS estado_revision_uf

FROM dw.fact_publicaciones p
LEFT JOIN dw.dim_propiedad d
    ON p.id_propiedad = d.id_propiedad
LEFT JOIN dw.alertas_uf_chilepropiedades a
    ON p.id_registro = a.id_registro