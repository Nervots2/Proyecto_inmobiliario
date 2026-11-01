"""Ingesta SII 2025-S2: conserva el PDF en Bronze y publica el CSV en Silver.

Ejecutar desde la raíz del repositorio:
    python scripts/subir_sii_s3.py

Requiere: boto3, python-dotenv y Docker con Silo/MinIO activo.
No crea buckets, no elimina objetos, no modifica PostgreSQL.
"""

import csv
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / ".env")

ORIGENES = [
    (
        RAIZ / "datalake_crudo" / "sii" / "ebbrrc_santiago.pdf",
        "lake-crudo",
        "sii/2025-S2/ebbrrc_santiago.pdf",
        "application/pdf",
    ),
    (
        RAIZ / "datalake_procesado" / "silver" / "sii" / "sii_rm_2025_s2_comunas.csv",
        "lake-procesado",
        "silver/sii/2025-S2/sii_rm_2025_s2_comunas.csv",
        "text/csv; charset=utf-8",
    ),
]


def validar_csv(ruta: Path) -> None:
    with ruta.open("r", encoding="utf-8-sig", newline="") as archivo:
        lector = csv.DictReader(archivo)
        necesarias = {"comuna", "periodo", "predios_totales", "avaluo_total_miles_clp"}
        if not necesarias.issubset(lector.fieldnames or []):
            raise ValueError(f"El CSV no contiene las columnas requeridas: {necesarias}")
        filas = list(lector)

    if len(filas) != 52:
        raise ValueError(f"Esperábamos 52 comunas y encontramos {len(filas)}")
    if len({fila["comuna"].strip().upper() for fila in filas}) != 52:
        raise ValueError("Hay comunas duplicadas en el CSV")
    if any(fila["periodo"] != "2025-S2" for fila in filas):
        raise ValueError("Hay registros de otro período en el CSV")
    total = sum(int(fila["predios_totales"]) for fila in filas)
    if total != 3_690_399:
        raise ValueError(f"El total de predios no coincide con el SII: {total}")
    print(f"CSV validado: {len(filas)} comunas, {total:,} predios".replace(",", "."))


def main() -> None:
    for ruta, _, _, _ in ORIGENES:
        if not ruta.is_file():
            raise FileNotFoundError(f"Falta el archivo: {ruta}")

    validar_csv(ORIGENES[1][0])

    usuario = os.getenv("MINIO_ROOT_USER")
    clave = os.getenv("MINIO_ROOT_PASSWORD")
    if not usuario or not clave:
        raise RuntimeError("Faltan MINIO_ROOT_USER o MINIO_ROOT_PASSWORD en .env")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000"),
        aws_access_key_id=usuario,
        aws_secret_access_key=clave,
        region_name="us-east-1",
    )

    # Comprobar ambos buckets antes de subir cualquier archivo.
    for bucket in {item[1] for item in ORIGENES}:
        s3.head_bucket(Bucket=bucket)

    for ruta, bucket, clave_objeto, tipo in ORIGENES:
        try:
            existente = s3.head_object(Bucket=bucket, Key=clave_objeto)
        except ClientError as exc:
            codigo = exc.response.get("Error", {}).get("Code", "")
            if codigo not in ("404", "NoSuchKey", "NotFound"):
                raise
        else:
            tamano_remoto = existente["ContentLength"]
            if tamano_remoto != ruta.stat().st_size:
                raise RuntimeError(
                    f"Ya existe s3://{bucket}/{clave_objeto} con un tamaño diferente. "
                    "Revisar antes de sobrescribir."
                )
            print(f"Ya existe, se conserva: s3://{bucket}/{clave_objeto}")
            continue

        s3.upload_file(
            str(ruta),
            bucket,
            clave_objeto,
            ExtraArgs={"ContentType": tipo},
        )
        print(f"Subido: s3://{bucket}/{clave_objeto}")

    print("Ingesta SII a S3 finalizada; PostgreSQL aún no se modificó.")


if __name__ == "__main__":
    main()
