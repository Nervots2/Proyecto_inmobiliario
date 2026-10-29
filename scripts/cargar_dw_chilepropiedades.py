"""Carga incremental de ChilePropiedades: Delta Lake (Trino) -> PostgreSQL DW.

Ejecutar desde la raiz del repositorio:
    python scripts/cargar_dw_chilepropiedades.py

No elimina los hechos existentes; evita duplicados usando la restriccion UNIQUE
(fuente, id_publicacion, fecha_extraccion) de dw.fact_publicaciones.
"""

import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from trino.dbapi import connect as trino_connect


RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / ".env")
FUENTE = "chilepropiedades"
LOTE = 1000
MAX_PRECIO = Decimal("9999999999999999.99")  # NUMERIC(18,2)
MAX_SUPERFICIE = Decimal("9999999999.99")  # NUMERIC(12,2)


def decimal_positivo(dato, maximo):
    if dato is None:
        return None
    try:
        valor = Decimal(str(dato))
        if not valor.is_finite() or not (0 < valor <= maximo):
            return None
        valor = valor.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return valor if 0 < valor <= maximo else None
    except (ValueError, InvalidOperation, TypeError):
        return None


def normalizar_fecha(valor):
    """Los TIMESTAMP sin zona de Trino se interpretan como UTC.

    Confirmar esta suposicion con el origen antes de una integracion productiva.
    """
    if isinstance(valor, datetime):
        fecha = valor
    elif isinstance(valor, str):
        fecha = datetime.fromisoformat(valor.strip().replace("Z", "+00:00"))
    else:
        raise ValueError("fecha_extraccion ausente o invalida")
    return fecha.replace(tzinfo=timezone.utc) if fecha.tzinfo is None else fecha


def normalizar_fila(fila):
    id_publicacion, tipo, precio_clp, precio_uf, superficie, fecha = fila
    identificador = str(id_publicacion).strip() if id_publicacion is not None else ""
    categoria = str(tipo).strip().lower() if tipo is not None else ""
    if not identificador or not categoria:
        return None

    # Se prioriza CLP para mantener precios comparables; UF es alternativa.
    precio = decimal_positivo(precio_clp, MAX_PRECIO)
    moneda = "CLP"
    if precio is None:
        precio = decimal_positivo(precio_uf, MAX_PRECIO)
        moneda = "UF"
    if precio is None:
        return None

    try:
        extraido = normalizar_fecha(fecha)
    except ValueError:
        return None

    metros = decimal_positivo(superficie, MAX_SUPERFICIE)
    return (identificador, categoria, precio, moneda, metros,
            extraido, extraido.date())


def main():
    variables = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    ausentes = [v for v in variables if not os.getenv(v)]
    if ausentes:
        raise RuntimeError("Faltan variables en .env: " + ", ".join(ausentes))

    trino = trino_connect(
        host="127.0.0.1", port=8088, user="inmobiliario",
        catalog="delta", schema="inmobiliario", http_scheme="http",
    )
    try:
        lector = trino.cursor()
        lector.execute("""
            SELECT id_publicacion, tipo_propiedad, precio_clp,
                   precio_uf, superficie_m2, fecha_extraccion
            FROM delta.inmobiliario.publicaciones_delta_final
        """)

        with psycopg.connect(
            host="127.0.0.1", port=5432,
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as postgres:
            # Una sola transaccion: si algo falla, se revierte la carga.
            with postgres.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM dw.fact_publicaciones")
                antes = cursor.fetchone()[0]

                cursor.execute("""
                    CREATE TEMP TABLE etapa_chilepropiedades (
                        id_publicacion text NOT NULL,
                        tipo_propiedad text NOT NULL,
                        precio numeric(18,2) NOT NULL,
                        moneda varchar(3) NOT NULL,
                        superficie_m2 numeric(12,2),
                        fecha_extraccion timestamptz NOT NULL,
                        fecha date NOT NULL
                    ) ON COMMIT DROP
                """)

                leidos = validos = omitidos = 0
                with cursor.copy("""
                    COPY etapa_chilepropiedades
                    (id_publicacion, tipo_propiedad, precio, moneda,
                     superficie_m2, fecha_extraccion, fecha)
                    FROM STDIN
                """) as copia:
                    while True:
                        grupo = lector.fetchmany(LOTE)
                        if not grupo:
                            break
                        for fila in grupo:
                            leidos += 1
                            registro = normalizar_fila(fila)
                            if registro is None:
                                omitidos += 1
                                continue
                            copia.write_row(registro)
                            validos += 1

                cursor.execute("""
                    INSERT INTO dw.dim_fecha (fecha, anio, mes, dia)
                    SELECT DISTINCT fecha, EXTRACT(YEAR FROM fecha)::int,
                           EXTRACT(MONTH FROM fecha)::int,
                           EXTRACT(DAY FROM fecha)::int
                    FROM etapa_chilepropiedades
                    ON CONFLICT (fecha) DO NOTHING
                """)
                cursor.execute("""
                    INSERT INTO dw.dim_propiedad (tipo_propiedad)
                    SELECT DISTINCT tipo_propiedad FROM etapa_chilepropiedades
                    ON CONFLICT (tipo_propiedad) DO NOTHING
                """)
                cursor.execute("""
                    INSERT INTO dw.fact_publicaciones
                    (id_publicacion, fuente, fecha, id_propiedad,
                     precio, moneda, superficie_m2, fecha_extraccion)
                    SELECT e.id_publicacion, %s, e.fecha, p.id_propiedad,
                           e.precio, e.moneda, e.superficie_m2,
                           e.fecha_extraccion
                    FROM etapa_chilepropiedades e
                    JOIN dw.dim_propiedad p
                      ON p.tipo_propiedad = e.tipo_propiedad
                    ON CONFLICT (fuente, id_publicacion, fecha_extraccion)
                    DO NOTHING
                """, (FUENTE,))
                insertados = cursor.rowcount
                cursor.execute("SELECT COUNT(*) FROM dw.fact_publicaciones")
                despues = cursor.fetchone()[0]

            # El with de conexion confirma la transaccion solo si no hubo error.
        print("Carga de ChilePropiedades finalizada")
        print(f"Registros leidos desde Delta: {leidos}")
        print(f"Registros validos: {validos}")
        print(f"Omitidos por datos esenciales invalidos: {omitidos}")
        print(f"Hechos nuevos insertados: {insertados}")
        print(f"Data Warehouse: {antes} -> {despues} hechos")
        print("Nota: id_ubicacion queda NULL hasta incorporar codigos INE.")
    finally:
        trino.close()


if __name__ == "__main__":
    main()
