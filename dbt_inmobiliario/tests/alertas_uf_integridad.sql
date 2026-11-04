-- La prueba falla si encuentra alguna inconsistencia.

WITH inconsistencias AS (
    SELECT
        c.id_registro
    FROM {{ ref('calidad_precios_publicaciones') }} c

    FULL JOIN dw.alertas_uf_chilepropiedades a
        ON c.id_registro = a.id_registro

    WHERE
        c.id_registro IS NULL

        OR c.alerta_uf IS DISTINCT FROM
            (a.id_registro IS NOT NULL)

        OR c.precio_uf_original
            IS DISTINCT FROM a.precio_uf_original

        OR c.precio_uf_titulo_candidata
            IS DISTINCT FROM a.precio_uf_titulo_candidata

        OR c.estado_revision_uf
            IS DISTINCT FROM a.estado
)

SELECT *
FROM inconsistencias