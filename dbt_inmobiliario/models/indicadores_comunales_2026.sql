{{ config(materialized='table') }}

-- Grano: una fila por comuna (CUT).
-- Anuncios: muestra recopilada durante 2026, sin atribuirles cobertura de mercado.
-- SII: primer semestre 2026. Permisos ChileHub: año 2026 (provisionales).
WITH publicaciones AS (
    SELECT
        id_ubicacion,
        COUNT(*) AS publicaciones_total,
        COUNT(*) FILTER (WHERE fuente = 'chilepropiedades') AS publicaciones_chilepropiedades,
        COUNT(*) FILTER (WHERE fuente = 'portal_inmobiliario') AS publicaciones_portal
    FROM dw.fact_publicaciones
    WHERE id_ubicacion IS NOT NULL
    GROUP BY id_ubicacion
),
sii AS (
    SELECT
        id_ubicacion,
        MAX(predios_total) AS predios_total,
        MAX(predios_habitacional) AS predios_habitacionales
    FROM dw.fact_sii_destinos
    WHERE periodo = '2026-S1'
    GROUP BY id_ubicacion
),
permisos AS (
    SELECT
        id_ubicacion,
        MAX(unidades_total) AS unidades_autorizadas_2026,
        MAX(estado_dato) AS estado_permisos_2026
    FROM dw.fact_chilehub_permisos_edificacion
    WHERE anio = 2026
    GROUP BY id_ubicacion
)
SELECT
    u.id_ubicacion,
    u.codigo_comuna AS cut,
    u.comuna,
    u.region,
    COALESCE(p.publicaciones_total, 0) AS publicaciones_total,
    COALESCE(p.publicaciones_chilepropiedades, 0) AS publicaciones_chilepropiedades,
    COALESCE(p.publicaciones_portal, 0) AS publicaciones_portal,
    s.predios_total,
    s.predios_habitacionales,
    permisos.unidades_autorizadas_2026,
    permisos.estado_permisos_2026,
    '2026-S1'::text AS periodo_sii,
    2026 AS anio_permisos
FROM dw.dim_ubicacion AS u
LEFT JOIN publicaciones AS p USING (id_ubicacion)
LEFT JOIN sii AS s USING (id_ubicacion)
LEFT JOIN permisos USING (id_ubicacion)
