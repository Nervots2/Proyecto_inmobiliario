"""Audita e integra ChileHub permisos de edificación a Silo (S3) y Delta Lake.

Desde la raíz de Proyecto_inmobiliario, con (inmobiliario):
    python scripts/integrar_chilehub_permisos_delta.py --auditar
    python scripts/integrar_chilehub_permisos_delta.py --cargar

--auditar: solo lectura local.  --cargar: sube Parquet original a Bronze,
publica Parquet validado en Silver, registra stage Hive y crea/verifica Delta.
NO modifica PostgreSQL ni otras tablas. Los objetos y la tabla Delta existentes
se verifican; nunca se sobreescriben con datos distintos.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'datalake_crudo/chilehub/chilehub_permisos_edificacion.parquet'
SILVER = ROOT / 'datalake_procesado/silver/chilehub/chilehub_permisos_edificacion_2002_2026.parquet'
RAW_BUCKET = 'lake-crudo'
SILVER_BUCKET = 'lake-procesado'
RAW_KEY = 'chilehub/chilehub_permisos_edificacion.parquet'
SILVER_KEY = 'silver/chilehub/permisos_edificacion_2002_2026/parte-00000.parquet'
STAGE_DIR = 'silver/chilehub/permisos_edificacion_2002_2026/'
DELTA_NAME = 'chilehub_permisos_edificacion_2002_2026'
DELTA_PREFIX = f'delta/{DELTA_NAME}/'
DELTA_TABLE = f'delta.inmobiliario.{DELTA_NAME}'

FIELDS = [
    ('anio', 'BIGINT'), ('codigo_region', 'VARCHAR'), ('codigo_comuna', 'VARCHAR'),
    ('nombre_comuna', 'VARCHAR'), ('unidades_total', 'BIGINT'),
    ('superficie_m2_total', 'BIGINT'), ('unidades_casas', 'BIGINT'),
    ('superficie_m2_casas', 'BIGINT'), ('unidades_departamentos', 'BIGINT'),
    ('superficie_m2_departamentos', 'BIGINT'), ('estado_dato', 'VARCHAR'),
    ('fuente', 'VARCHAR'), ('url_fuente', 'VARCHAR'), ('fecha_fuente', 'VARCHAR'),
]
NUMERIC = [f for f, t in FIELDS if t == 'BIGINT']
TEXT = [f for f, t in FIELDS if t == 'VARCHAR']
YEARS = list(range(2002, 2027))
EXPECTED_ROWS = 346 * len(YEARS)


def audit():
    import pandas as pd
    if not RAW.is_file():
        raise FileNotFoundError(f'No se encontró el Parquet original: {RAW}')
    df = pd.read_parquet(RAW)
    expected = [f for f, _ in FIELDS]
    if list(df.columns) != expected:
        raise ValueError('Columnas distintas de las esperadas.\n'
                         f'Encontradas: {list(df.columns)}\nEsperadas: {expected}')
    if len(df) != EXPECTED_ROWS:
        raise ValueError(f'Registros: {len(df)}; se esperaban {EXPECTED_ROWS}')
    if df.isna().any().any():
        raise ValueError('Hay valores nulos inesperados en el Parquet')

    for col in TEXT:
        df[col] = df[col].astype(str).str.strip()
        if (df[col] == '').any():
            raise ValueError(f'Hay textos vacíos en {col}')
    # Los códigos tienen longitud significativa: no convertirlos a números.
    if (~df['codigo_region'].str.fullmatch(r'\d{2}')).any():
        raise ValueError('codigo_region debe conservar exactamente dos dígitos')
    if (~df['codigo_comuna'].str.fullmatch(r'\d{5}')).any():
        raise ValueError('codigo_comuna debe conservar exactamente cinco dígitos')
    if (df['codigo_region'] != df['codigo_comuna'].str[:2]).any():
        raise ValueError('Hay códigos de región incompatibles con el CUT comunal')

    # Convertir enteros explícitamente, rechazando decimales, textos y negativos.
    for col in NUMERIC:
        numeric = pd.to_numeric(df[col], errors='raise')
        if numeric.isna().any() or (numeric < 0).any() or (numeric != numeric.round()).any():
            raise ValueError(f'Valores inválidos o negativos en {col}')
        df[col] = numeric.astype('int64')
    if sorted(df['anio'].unique().tolist()) != YEARS:
        raise ValueError('El período no cubre exactamente 2002–2026')
    if df['codigo_comuna'].nunique() != 346 or df['codigo_region'].nunique() != 16:
        raise ValueError('Se esperaban 346 CUT comunales y 16 códigos regionales')
    if df.duplicated(['anio', 'codigo_comuna']).any():
        raise ValueError('Hay pares año-CUT repetidos')
    coverage = df.groupby('anio')['codigo_comuna'].nunique()
    if not coverage.eq(346).all():
        raise ValueError(f'Cobertura comunal incompleta: {coverage[coverage != 346].to_dict()}')
    codes = set(df[df['anio'] == 2002]['codigo_comuna'])
    if any(set(g['codigo_comuna']) != codes for _, g in df.groupby('anio')):
        raise ValueError('El conjunto de comunas cambia entre años')
    if (df['unidades_total'] != df['unidades_casas'] + df['unidades_departamentos']).any():
        raise ValueError('Unidades totales no cuadran con casas + departamentos')
    if (df['superficie_m2_total'] !=
            df['superficie_m2_casas'] + df['superficie_m2_departamentos']).any():
        raise ValueError('Superficie total no cuadra con casas + departamentos')
    if not df['estado_dato'].isin(['definitivo', 'provisional']).all():
        raise ValueError('Se encontró un estado_dato desconocido')
    expected_status = df['anio'].map(lambda yr: 'provisional' if yr >= 2024 else 'definitivo')
    if (df['estado_dato'] != expected_status).any():
        raise ValueError('Los estados no coinciden con 2002–2023 definitivo / 2024–2026 provisional')
    try:
        fechas = pd.to_datetime(df['fecha_fuente'], errors='raise')
    except Exception as exc:
        raise ValueError('fecha_fuente contiene una fecha no interpretable') from exc
    if fechas.isna().any():
        raise ValueError('fecha_fuente contiene valores vacíos')
    df.sort_values(['anio', 'codigo_comuna'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    print('=== CHILEHUB: AUDITORÍA PERMISOS DE EDIFICACIÓN (SIN ESCRITURAS) ===')
    print(f'Registros: {len(df)} | años: {min(YEARS)}–{max(YEARS)} | comunas: 346 | regiones: 16')
    print('Cobertura: 346 comunas cada año; sin duplicados ni códigos geográficos inconsistentes')
    print('Unidades y superficies: no negativas, totales = casas + departamentos')
    print('Estados: definitivo 7.612 (2002–2023); provisional 1.038 (2024–2026)')
    print('Advertencia: coherencia estructural no certifica exactitud de la fuente original.')
    return df


def parquet_bytes(df):
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema_types = {'BIGINT': pa.int64(), 'VARCHAR': pa.string()}
    arrays = [pa.array(df[name].tolist(), type=schema_types[typ]) for name, typ in FIELDS]
    table = pa.Table.from_arrays(arrays, names=[f for f, _ in FIELDS])
    buf = io.BytesIO()
    pq.write_table(table, buf, compression='snappy')
    return buf.getvalue()


def connect_s3():
    import boto3
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    user, pwd = os.getenv('MINIO_ROOT_USER'), os.getenv('MINIO_ROOT_PASSWORD')
    if not user or not pwd:
        raise RuntimeError('Faltan MINIO_ROOT_USER y MINIO_ROOT_PASSWORD en .env')
    return boto3.client('s3', endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://127.0.0.1:9000'),
                        aws_access_key_id=user, aws_secret_access_key=pwd, region_name='us-east-1')


def publish_immutable(s3, bucket, key, data, mime):
    from botocore.exceptions import ClientError
    try:
        existing = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') not in {'404', 'NoSuchKey', 'NotFound'}:
            raise
    else:
        if hashlib.sha256(existing).digest() != hashlib.sha256(data).digest():
            raise RuntimeError(f'Contenido diferente en s3://{bucket}/{key}; se aborta sin sobrescribir')
        print(f'Ya publicado, idéntico: s3://{bucket}/{key}')
        return
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=mime)
    print(f'Publicado: s3://{bucket}/{key}')


def trino_connect():
    from trino.dbapi import connect
    return connect(host='127.0.0.1', port=8088, user='inmobiliario',
                   catalog='delta', schema='inmobiliario', http_scheme='http')


def fetch_compare(cur, table, df):
    cols = ', '.join(name for name, _ in FIELDS)
    cur.execute(f'SELECT {cols} FROM {table} ORDER BY anio, codigo_comuna')
    got = cur.fetchall()
    if len(got) != len(df):
        raise RuntimeError(f'{table}: {len(got)} filas; esperadas {len(df)}')
    for i, (observed, expected) in enumerate(zip(got, df.itertuples(index=False, name=None)), 1):
        comparable = tuple(int(x) if typ == 'BIGINT' else str(x) for x, (_, typ) in zip(observed, FIELDS))
        want = tuple(int(x) if typ == 'BIGINT' else str(x) for x, (_, typ) in zip(expected, FIELDS))
        if comparable != want:
            raise RuntimeError(f'{table}: diferencia en fila ordenada {i}, año/CUT {want[:3]}')
    print(f'Tabla validada fila a fila: {table} ({len(got)} registros, sin diferencias)')


def load(df):
    original = RAW.read_bytes()
    transformed = parquet_bytes(df)
    # Escribir Silver local únicamente en --cargar; nunca reescribir un archivo distinto.
    SILVER.parent.mkdir(parents=True, exist_ok=True)
    if SILVER.exists():
        if hashlib.sha256(SILVER.read_bytes()).digest() != hashlib.sha256(transformed).digest():
            raise RuntimeError(f'Hay un Silver local diferente: {SILVER}; no se sobrescribió')
    else:
        SILVER.write_bytes(transformed)
        print(f'Parquet Silver local generado: {SILVER.relative_to(ROOT)}')

    s3 = connect_s3()
    s3.head_bucket(Bucket=RAW_BUCKET)
    s3.head_bucket(Bucket=SILVER_BUCKET)
    publish_immutable(s3, RAW_BUCKET, RAW_KEY, original, 'application/octet-stream')
    publish_immutable(s3, SILVER_BUCKET, SILVER_KEY, transformed, 'application/octet-stream')

    digest = hashlib.sha256(transformed).hexdigest()[:12]
    stage = f'lake.inmobiliario.chilehub_stage_{digest}'
    stage_name = f'chilehub_stage_{digest}'
    conn = trino_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES FROM lake.inmobiliario LIKE '{stage_name}'")
        if not cur.fetchall():
            columns_sql = ', '.join(f'{name} {typ}' for name, typ in FIELDS)
            cur.execute(f'CREATE TABLE {stage} ({columns_sql}) WITH '
                        f"(format='PARQUET', external_location='s3a://{SILVER_BUCKET}/{STAGE_DIR}')")
            cur.fetchall()
            print(f'Tabla temporal de lectura registrada: {stage}')
        fetch_compare(cur, stage, df)

        cur.execute(f"SHOW TABLES FROM delta.inmobiliario LIKE '{DELTA_NAME}'")
        if not cur.fetchall():
            has_objects = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=DELTA_PREFIX,
                                              MaxKeys=1).get('KeyCount', 0)
            if has_objects:
                raise RuntimeError(f'Ruta Delta tiene objetos sin tabla registrada: {DELTA_PREFIX}; revisar')
            columns_sql = ', '.join(name for name, _ in FIELDS)
            cur.execute(f'CREATE TABLE {DELTA_TABLE} WITH '
                        f"(location='s3a://{SILVER_BUCKET}/{DELTA_PREFIX}') "
                        f'AS SELECT {columns_sql} FROM {stage}')
            cur.fetchall()
            print(f'Tabla Delta creada: {DELTA_TABLE}')
        else:
            print(f'Tabla Delta existente; se validará sin escribir: {DELTA_TABLE}')
        fetch_compare(cur, DELTA_TABLE, df)
    finally:
        conn.close()
    print('INTEGRACIÓN CHILEHUB A SILO Y DELTA COMPLETADA. PostgreSQL intacto.')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    p = ap.add_mutually_exclusive_group(required=True)
    p.add_argument('--auditar', action='store_true', help='Solo lectura local')
    p.add_argument('--cargar', action='store_true', help='Publicar S3 + crear/verificar Delta')
    args = ap.parse_args()
    df = audit()
    if args.cargar:
        load(df)
    else:
        print('AUDITORÍA COMPLETA SIN ESCRITURAS.')


if __name__ == '__main__':
    main()
