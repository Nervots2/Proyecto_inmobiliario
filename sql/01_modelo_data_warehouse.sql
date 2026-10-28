-- MODELO DIMENSIONAL
-- DATA WAREHOUSE INMOBILIARIO

CREATE SCHEMA IF NOT EXISTS dw;

-- Dimensión temporal
CREATE TABLE IF NOT EXISTS dw.dim_fecha (
    fecha DATE PRIMARY KEY,
    anio INTEGER,
    mes INTEGER,
    dia INTEGER
);

-- Dimensión geográfica
CREATE TABLE IF NOT EXISTS dw.dim_ubicacion (
    id_ubicacion BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    codigo_region TEXT,
    codigo_comuna TEXT,
    region TEXT,
    comuna TEXT
);

-- Dimensión de propiedad
CREATE TABLE IF NOT EXISTS dw.dim_propiedad (
    id_propiedad BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tipo_propiedad TEXT NOT NULL UNIQUE
);

-- Tabla de hechos
CREATE TABLE IF NOT EXISTS dw.fact_publicaciones (
    id_registro BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    id_publicacion TEXT NOT NULL,
    fuente TEXT NOT NULL,

    fecha DATE REFERENCES dw.dim_fecha(fecha),

    id_ubicacion BIGINT REFERENCES dw.dim_ubicacion(id_ubicacion),
    id_propiedad BIGINT REFERENCES dw.dim_propiedad(id_propiedad),

    precio NUMERIC(18, 2),
    moneda VARCHAR(3),
    superficie_m2 NUMERIC(12, 2),

    es_proyecto BOOLEAN,
    precio_desde BOOLEAN,

    fecha_extraccion TIMESTAMPTZ NOT NULL,

    UNIQUE (fuente, id_publicacion, fecha_extraccion)
);