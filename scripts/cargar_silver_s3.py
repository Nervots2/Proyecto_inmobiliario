import os
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / ".env")

RUTA_DATOS = (
    RAIZ
    / "datalake_procesado"
    / "silver"
    / "chilepropiedades_batch_spark"
)

BUCKET = "lake-procesado"

# Conexión con SILO
s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    region_name="us-east-1",
    config=Config(
        s3={"addressing_style": "path"}
    )
)

# Crear el bucket si todavía no existe
buckets = [
    b["Name"]
    for b in s3.list_buckets()["Buckets"]
]

if BUCKET not in buckets:
    s3.create_bucket(Bucket=BUCKET)

# Localizar archivos generados por Spark
archivos = sorted(RUTA_DATOS.glob("part-*.parquet"))

if not archivos:
    raise FileNotFoundError(
        "No se encontraron archivos Parquet de Spark."
    )

print("=== CARGA DE SILVER A SILO ===")

for archivo in archivos:
    clave = (
        "silver/chilepropiedades_batch_spark/"
        + archivo.name
    )

    s3.upload_file(
        str(archivo),
        BUCKET,
        clave
    )

    print("Cargado:", clave)

print("\nArchivos cargados:", len(archivos))
print("Bucket:", BUCKET)
print("Carga finalizada.")