CREATE TABLE IF NOT EXISTS lake.inmobiliario.publicaciones (
    id_publicacion VARCHAR,
    fuente VARCHAR,
    url VARCHAR,
    titulo VARCHAR,
    tipo_propiedad VARCHAR,
    comuna_slug VARCHAR,
    precio_clp BIGINT,
    precio_uf DOUBLE,
    superficie_m2 BIGINT,
    habitaciones BIGINT,
    banos BIGINT,
    fecha_extraccion TIMESTAMP(3),
    descripcion_completa VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://lake-procesado/silver/chilepropiedades_batch_spark/'
);