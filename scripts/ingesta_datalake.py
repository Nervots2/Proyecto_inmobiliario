import os
import boto3

from pathlib import Path
from dotenv import load_dotenv

# Directorio principal del proyecto
RUTA_PROYECTO = Path(__file__).resolve().parent.parent

# Cargar credenciales locales
load_dotenv(RUTA_PROYECTO / ".env")

# Conexión al Data Lake
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    region_name="us-east-1"
)

# Consultar archivos almacenados
respuesta = s3.list_objects_v2(
    Bucket="lake-crudo"
)

print("=== ARCHIVOS DEL DATA LAKE ===")

for archivo in respuesta.get("Contents", []):
    print("Archivo:", archivo["Key"])
    print("Tamaño:", archivo["Size"], "bytes")



# ==========================================
# INGESTA BATCH DE ARCHIVOS
# ==========================================

import hashlib
from botocore.exceptions import ClientError

RUTA_ORIGEN = RUTA_PROYECTO / "datalake_crudo"

FORMATOS = {
    ".csv", ".xlsx", ".xls",
    ".json", ".parquet", ".pdf",
    ".geojson", ".gpkg", ".zip"
}

# Excluir temporalmente anuncios de terceros
CARPETAS_EXCLUIDAS = {"portal_inmobiliario"}

# Límite inicial: 100 MB por archivo
LIMITE_BYTES = 100 * 1024 * 1024


def calcular_hash(ruta):
    hash_archivo = hashlib.sha256()

    with open(ruta, "rb") as archivo:
        for bloque in iter(
            lambda: archivo.read(1024 * 1024),
            b""
        ):
            hash_archivo.update(bloque)

    return hash_archivo.hexdigest()


cargados = 0
omitidos = 0

print("\n=== INGESTA BATCH ===")

for archivo in sorted(RUTA_ORIGEN.rglob("*")):

    if not archivo.is_file():
        continue

    ruta_relativa = archivo.relative_to(RUTA_ORIGEN)

    if archivo.suffix.lower() not in FORMATOS:
        continue

    if any(
        carpeta in ruta_relativa.parts
        for carpeta in CARPETAS_EXCLUIDAS
    ):
        omitidos += 1
        continue

    if archivo.stat().st_size > LIMITE_BYTES:
        print("Archivo grande omitido:", archivo.name)
        omitidos += 1
        continue

    clave_s3 = ruta_relativa.as_posix()
    hash_local = calcular_hash(archivo)

    # Comprobar si ya existe la misma versión
    try:
        objeto = s3.head_object(
            Bucket="lake-crudo",
            Key=clave_s3
        )
        hash_remoto = objeto.get(
            "Metadata", {}
        ).get("sha256")

    except ClientError as error:
        codigo = error.response["Error"]["Code"]

        if codigo not in ("404", "NoSuchKey", "NotFound"):
            raise

        hash_remoto = None

    if hash_local == hash_remoto:
        print("Sin cambios:", clave_s3)
        omitidos += 1
        continue

    # Cargar el archivo original
    s3.upload_file(
        str(archivo),
        "lake-crudo",
        clave_s3,
        ExtraArgs={
            "Metadata": {"sha256": hash_local}
        }
    )

    print("Cargado:", clave_s3)
    cargados += 1


print("\n=== RESUMEN ===")
print("Archivos cargados:", cargados)
print("Archivos omitidos:", omitidos)