"""Audita y carga la serie mensual de vivienda del Banco Central a Silo/Delta.

Desde la raíz de Proyecto_inmobiliario (entorno Conda `inmobiliario`):
    python scripts/integrar_banco_central_vivienda_delta.py --auditar
    python scripts/integrar_banco_central_vivienda_delta.py --cargar

La auditoría solo lee el CSV Silver y su archivo de metadatos.
La carga preserva el Excel en Bronze, publica el CSV y los metadatos en Silver,
y crea/verifica una tabla Delta separada. No modifica PostgreSQL, los archivos
locales, ni las demás tablas Delta. Si encuentra datos distintos, se detiene.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "datalake_crudo/banco_central/01_BancoCentral_Tasas_Flujos_Vivienda.xlsx"
SILVER = ROOT / "datalake_procesado/silver/banco_central"
CSV = SILVER / "banco_central_vivienda_mensual_2002_2026.csv"
META = SILVER / "banco_central_vivienda_mensual_metadatos.json"

BUCKET_RAW = "lake-crudo"
BUCKET_SILVER = "lake-procesado"
KEY_RAW = "banco_central/vivienda/01_BancoCentral_Tasas_Flujos_Vivienda.xlsx"
KEY_CSV = "silver/banco_central/vivienda/banco_central_vivienda_mensual_2002_2026.csv"
KEY_META = "silver/banco_central/vivienda/banco_central_vivienda_mensual_metadatos.json"
TABLE = "banco_central_vivienda_mensual_2002_2026"
DELTA_PREFIX = f"delta/{TABLE}/"

COLUMNS = ("periodo", "tasa_interes_vivienda_anual_pct", "flujo_vivienda_valor_original")


def parse_number(value: str, label: str, required: bool) -> Decimal | None:
    if not value.strip():
        if required:
            raise ValueError(f"Falta {label}")
        return None
    try:
        n = Decimal(value.strip().replace(",", "."))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Valor numérico inválido para {label}: {value!r}") from exc
    if not n.is_finite() or n < 0:
        raise ValueError(f"Valor negativo o no finito en {label}: {value!r}")
    # DECIMAL(38,20) conserva hasta 20 decimales y 18 dígitos enteros.
    # Rechazar valores que exijan redondeo o desborden la parte entera.
    decimales = max(0, -n.as_tuple().exponent)
    enteros = max(0, n.adjusted() + 1) if n != 0 else 0
    if decimales > 20 or enteros > 18:
        raise ValueError(f"Valor excede DECIMAL(38,20) en {label}: {n}")
    if label == "tasa" and n >= 100:
        raise ValueError(f"Tasa inusual: {n}")
    return n


def next_month(d: date) -> date:
    return date(d.year + (d.month == 12), 1 if d.month == 12 else d.month + 1, 1)


def audit():
    for p in (RAW, CSV, META):
        if not p.is_file():
            raise FileNotFoundError(f"No se encontró {p}; ejecutar desde la raíz del proyecto")

    meta = json.loads(META.read_text(encoding="utf-8-sig"))
    with CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if rdr.fieldnames != list(COLUMNS):
            raise ValueError(f"Encabezados distintos: {rdr.fieldnames}; esperados: {COLUMNS}")
        rows = []
        seen = set()
        for number, raw in enumerate(rdr, start=2):
            if None in raw or any(v is None for v in raw.values()):
                raise ValueError(f"CSV mal formado en fila {number}")
            try:
                period = date.fromisoformat(raw["periodo"])
            except ValueError as exc:
                raise ValueError(f"Fecha inválida en fila {number}") from exc
            if period.day != 1 or period in seen:
                raise ValueError(f"Mes fuera de formato o duplicado en fila {number}: {period}")
            seen.add(period)
            rate = parse_number(raw[COLUMNS[1]], "tasa", required=True)
            flow = parse_number(raw[COLUMNS[2]], "flujo", required=False)
            rows.append((period, rate, flow))

    rows.sort(key=lambda r: r[0])
    for older, newer in zip(rows, rows[1:]):
        if next_month(older[0]) != newer[0]:
            raise ValueError(f"Mes faltante entre {older[0]} y {newer[0]}")
    if not rows:
        raise ValueError("CSV sin observaciones")
    nonnull = sum(x[2] is not None for x in rows)
    expected = (297, 153, date(2002, 1, 1), date(2026, 9, 1))
    actual = (len(rows), nonnull, rows[0][0], rows[-1][0])
    if actual != expected:
        raise ValueError(f"Conteos o período inesperados: {actual}, se esperaba {expected}")
    check_meta = {
        "observaciones": len(rows), "tasas_disponibles": len(rows),
        "flujos_disponibles": nonnull, "flujos_ausentes": len(rows) - nonnull,
        "periodo_inicial": str(rows[0][0]), "periodo_final": str(rows[-1][0]),
    }
    for key, exp in check_meta.items():
        if meta.get(key) != exp:
            raise ValueError(f"Metadatos inconsistentes: {key}={meta.get(key)!r}; esperado={exp!r}")
    series = meta.get("series_del_excel", [])
    if len(series) < 2 or not all(x.get("codigo") for x in series[:2]):
        raise ValueError("Metadatos de series incompletos")

    print("=== BANCO CENTRAL: AUDITORÍA DE SILVER ===")
    print(f"CSV: {CSV.name} | meses: {len(rows)} | tasa: {len(rows)}")
    print(f"Flujo disponible: {nonnull} | flujo NULL: {len(rows) - nonnull}")
    print(f"Período: {rows[0][0]} a {rows[-1][0]}; sin meses omitidos ni fechas duplicadas")
    print("Serie tasa:", series[0]["codigo"])
    print("Serie flujo:", series[1]["codigo"])
    print("Unidad exacta del flujo: por confirmar; se conserva valor original")
    return rows, meta


def connect_s3():
    import boto3
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    user = os.getenv("MINIO_ROOT_USER")
    password = os.getenv("MINIO_ROOT_PASSWORD")
    if not user or not password:
        raise RuntimeError("Faltan MINIO_ROOT_USER o MINIO_ROOT_PASSWORD en .env")
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000"),
        aws_access_key_id=user, aws_secret_access_key=password,
        region_name="us-east-1",
    )


def upload_if_same_or_new(s3, bucket: str, key: str, data: bytes, content_type: str):
    from botocore.exceptions import ClientError
    try:
        prev = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
    else:
        if prev != data:
            raise RuntimeError(f"Objeto distinto ya publicado: s3://{bucket}/{key}. No se sobrescribió")
        print(f"Ya publicado sin cambios: s3://{bucket}/{key}")
        return
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    print(f"Publicado: s3://{bucket}/{key}")


def sql_num(n: Decimal | None) -> str:
    return "CAST(NULL AS DECIMAL(38,20))" if n is None else f"CAST('{n}' AS DECIMAL(38,20))"


def create_sql(rows, meta):
    # Una sentencia CTAS idempotente solo cuando la tabla está ausente.
    # Las fechas quedan tipadas DATE; los flujos faltantes quedan NULL.
    values = []
    for period, rate, flow in rows:
        values.append(f"(DATE '{period}', {sql_num(rate)}, {sql_num(flow)})")
    return (
        f"CREATE TABLE delta.inmobiliario.{TABLE} "
        f"WITH (location = 's3a://{BUCKET_SILVER}/{DELTA_PREFIX}') AS\n"
        "SELECT * FROM (VALUES\n  " + ",\n  ".join(values) +
        "\n) AS t (periodo, tasa_interes_vivienda_anual_pct, flujo_vivienda_valor_original)"
    )


def validate_table(cur, rows):
    cur.execute(
        f"SELECT periodo, tasa_interes_vivienda_anual_pct, flujo_vivienda_valor_original "
        f"FROM delta.inmobiliario.{TABLE} ORDER BY periodo"
    )
    received = cur.fetchall()
    if len(received) != len(rows):
        raise RuntimeError(f"Delta tiene {len(received)} filas, se esperaban {len(rows)}")
    for i, (actual, expected) in enumerate(zip(received, rows), start=1):
        d, rate, flow = actual
        if str(d) != str(expected[0]) or Decimal(str(rate)) != expected[1] or (
            (flow is None) != (expected[2] is None)
        ) or (flow is not None and Decimal(str(flow)) != expected[2]):
            raise RuntimeError(f"Fila Delta {i} distinta; período {d}")
    print(f"Delta validado: delta.inmobiliario.{TABLE} — {len(received)} meses, 153 flujos no nulos")


def load(rows, meta):
    from trino.dbapi import connect
    s3 = connect_s3()
    upload_if_same_or_new(s3, BUCKET_RAW, KEY_RAW, RAW.read_bytes(),
                          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    upload_if_same_or_new(s3, BUCKET_SILVER, KEY_CSV, CSV.read_bytes(), "text/csv")
    upload_if_same_or_new(s3, BUCKET_SILVER, KEY_META, META.read_bytes(), "application/json")

    conn = connect(host="127.0.0.1", port=8088, user="inmobiliario",
                   catalog="delta", schema="inmobiliario", http_scheme="http")
    try:
        cur = conn.cursor()
        cur.execute("SHOW TABLES FROM delta.inmobiliario")
        existing = {r[0] for r in cur.fetchall()}
        if TABLE not in existing:
            if s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=DELTA_PREFIX,
                                  MaxKeys=1).get("KeyCount", 0):
                raise RuntimeError(f"Hay archivos Delta sin tabla registrada en {DELTA_PREFIX}; no se sobrescribió")
            cur.execute(create_sql(rows, meta))
            cur.fetchall()
            print(f"Delta creado: delta.inmobiliario.{TABLE}")
        else:
            print(f"Delta ya existente: delta.inmobiliario.{TABLE}; se validará sin escribir")
        validate_table(cur, rows)
    finally:
        conn.close()
    print("INTEGRACIÓN A SILO Y DELTA COMPLETA. PostgreSQL y otras tablas intactos.")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    modes = ap.add_mutually_exclusive_group(required=True)
    modes.add_argument("--auditar", action="store_true", help="Solo lectura local")
    modes.add_argument("--cargar", action="store_true", help="Publica en S3 y crea/valida Delta")
    args = ap.parse_args()
    rows, meta = audit()
    if args.auditar:
        print("AUDITORÍA COMPLETADA SIN ESCRITURAS.")
    else:
        load(rows, meta)


if __name__ == "__main__":
    main()
