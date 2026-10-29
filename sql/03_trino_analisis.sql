-- Avance 4: procesamiento tipo 3
-- SQL sobre archivos mediante Trino
-- Fuente: Chile Propiedades
-- Almacenamiento: SILO/S3, formato Parquet

SELECT
    tipo_propiedad,
    COUNT(*) AS publicaciones,
    ROUND(
        approx_percentile(precio_uf, 0.5),
        2
    ) AS mediana_precio_uf
FROM lake.inmobiliario.publicaciones
GROUP BY tipo_propiedad
ORDER BY publicaciones DESC;