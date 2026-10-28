import os
import re
import json
import psycopg

from pathlib import Path
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv


# ==========================================
# CONFIGURACIÓN
# ==========================================

RUTA_PROYECTO = Path(__file__).resolve().parent.parent

load_dotenv(RUTA_PROYECTO / ".env")

RUTA_DATOS = (
    RUTA_PROYECTO
    / "datalake_crudo"
    / "portal_inmobiliario"
)


# ==========================================
# TRANSFORMACIÓN DE PRECIOS
# ==========================================

def procesar_precio(precio):

    if not precio:
        return None, None

    texto = precio.strip().upper()

    coincidencia = re.match(
        r"^(UF|CLP|\$)\s*([\d.,]+)",
        texto
    )

    if not coincidencia:
        return None, None

    moneda, valor = coincidencia.groups()

    if moneda == "$":
        moneda = "CLP"

    # Convención chilena de separadores
    valor = valor.replace(".", "").replace(",", ".")

    return Decimal(valor), moneda


# ==========================================
# SELECCIÓN DE DATASETS
# ==========================================

patrones = [
    "portal_venta_departamento_santiago-metropolitana_*.json",
    "portal_venta_casa_santiago-metropolitana_*.json"
]

archivos = []

for patron in patrones:

    encontrados = list(RUTA_DATOS.glob(patron))

    if encontrados:
        mas_reciente = max(
            encontrados,
            key=lambda archivo: archivo.stat().st_mtime
        )
        archivos.append(mas_reciente)

if not archivos:
    raise FileNotFoundError(
        "No se encontraron datasets inmobiliarios."
    )


# ==========================================
# CONEXIÓN AL DATA WAREHOUSE
# ==========================================

with psycopg.connect(
    host="127.0.0.1",
    port=5432,
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"]
) as conexion:

    with conexion.cursor() as cursor:

        insertados = 0
        omitidos = 0

        for archivo in archivos:

            print(f"\nProcesando: {archivo.name}")

            with open(archivo, encoding="utf-8") as f:
                publicaciones = json.load(f)

            for publicacion in publicaciones:

                id_publicacion = publicacion.get("id")
                tipo = publicacion.get("tipo_propiedad_busqueda")
                fecha_original = publicacion.get("fecha_extraccion")

                if not all([id_publicacion, tipo, fecha_original]):
                    omitidos += 1
                    continue

                fecha_extraccion = datetime.fromisoformat(
                    fecha_original.replace("Z", "+00:00")
                )

                fecha = fecha_extraccion.date()

                precio, moneda = procesar_precio(
                    publicacion.get("precio_actual")
                )

                # Dimensión temporal
                cursor.execute(
                    """
                    INSERT INTO dw.dim_fecha
                        (fecha, anio, mes, dia)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (fecha) DO NOTHING
                    """,
                    (fecha, fecha.year, fecha.month, fecha.day)
                )

                # Dimensión propiedad
                cursor.execute(
                    """
                    INSERT INTO dw.dim_propiedad (tipo_propiedad)
                    VALUES (%s)
                    ON CONFLICT (tipo_propiedad) DO NOTHING
                    """,
                    (tipo,)
                )

                cursor.execute(
                    """
                    SELECT id_propiedad
                    FROM dw.dim_propiedad
                    WHERE tipo_propiedad = %s
                    """,
                    (tipo,)
                )

                id_propiedad = cursor.fetchone()[0]

                # Tabla de hechos
                cursor.execute(
                    """
                    INSERT INTO dw.fact_publicaciones (
                        id_publicacion,
                        fuente,
                        fecha,
                        id_propiedad,
                        precio,
                        moneda,
                        es_proyecto,
                        precio_desde,
                        fecha_extraccion
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (
                        fuente,
                        id_publicacion,
                        fecha_extraccion
                    ) DO NOTHING
                    """,
                    (
                        id_publicacion,
                        "portal_inmobiliario",
                        fecha,
                        id_propiedad,
                        precio,
                        moneda,
                        (
                            publicacion.get("tipo_anuncio") == "proyecto"
                            if "tipo_anuncio" in publicacion
                            else None
                        ),
                        publicacion.get("precio_es_desde"),
                        fecha_extraccion
                    )
                )

                if cursor.rowcount == 1:
                    insertados += 1
                else:
                    omitidos += 1

        print("\n=== RESULTADO DE LA CARGA ===")
        print("Registros insertados:", insertados)
        print("Registros omitidos:", omitidos)

        cursor.execute(
            """
            SELECT p.tipo_propiedad, COUNT(*)
            FROM dw.fact_publicaciones f
            JOIN dw.dim_propiedad p
              ON f.id_propiedad = p.id_propiedad
            GROUP BY p.tipo_propiedad
            ORDER BY p.tipo_propiedad
            """
        )

        print("\n=== REGISTROS POR CATEGORÍA ===")

        for tipo, cantidad in cursor.fetchall():
            print(f"{tipo}: {cantidad}")