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