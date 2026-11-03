"""Observatorio: empleo en construcción (2024) -> Silo/S3 y Delta Lake.

Desde la raíz de Proyecto_inmobiliario:
    python scripts/integrar_observatorio_construccion_delta.py --auditar
    python scripts/integrar_observatorio_construccion_delta.py --cargar

--auditar: solo lectura local; --cargar: publica Bronze/Silver y crea o
verifica tabla Delta. No modifica PostgreSQL ni otras tablas.

Mantiene tres niveles geográficos sin sumar agregados (país, región, comuna),
y conserva como NULL el valor sin dato de Río Verde.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import math
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'datalake_crudo/observatorio/observatorio_ocupados_construccion_2024.csv'
SILVER = ROOT / 'datalake_procesado/silver/observatorio/observatorio_ocupados_construccion_2024.parquet'
RAW_BUCKET, SILVER_BUCKET = 'lake-crudo', 'lake-procesado'
RAW_KEY = 'observatorio/observatorio_ocupados_construccion_2024.csv'
SILVER_KEY = 'silver/observatorio/ocupados_construccion_2024/parte-00000.parquet'
STAGE_DIR = 'silver/observatorio/ocupados_construccion_2024/'
DELTA_NAME = 'observatorio_ocupados_construccion_2024'
DELTA_PREFIX = f'delta/{DELTA_NAME}/'
DELTA_TABLE = f'delta.inmobiliario.{DELTA_NAME}'

FIELDS = [
    ('nivel', 'VARCHAR'), ('codigo', 'VARCHAR'), ('territorio', 'VARCHAR'),
    ('region', 'VARCHAR'), ('indicador_id', 'VARCHAR'), ('indicador', 'VARCHAR'),
    ('unidad', 'VARCHAR'), ('periodo', 'BIGINT'), ('valor', 'DOUBLE'),
    ('numerador', 'DOUBLE'), ('denominador', 'DOUBLE'), ('nota', 'VARCHAR'),
    ('estado', 'VARCHAR'), ('fuente', 'VARCHAR'), ('licencia', 'VARCHAR'),
    ('limite_inferior', 'DOUBLE'), ('limite_superior', 'DOUBLE'),
    ('dudoso', 'BOOLEAN'), ('licencia_nombre', 'VARCHAR'),
    ('territorio_distinto', 'BOOLEAN'),
]
COLUMNS = [name for name, _ in FIELDS]
STRING_COLUMNS = [name for name, typ in FIELDS if typ == 'VARCHAR']
FLOAT_COLUMNS = [name for name, typ in FIELDS if typ == 'DOUBLE']
BOOL_COLUMNS = [name for name, typ in FIELDS if typ == 'BOOLEAN']
KEY = ['nivel', 'codigo', 'indicador_id', 'periodo']


def audit():
    import pandas as pd

    if not RAW.is_file():
        raise FileNotFoundError(f'No existe: {RAW}')
    df = pd.read_csv(RAW, sep=None, engine='python', encoding='utf-8-sig',
                     dtype={'codigo': 'string'})
    if set(df.columns) != set(COLUMNS):
        raise ValueError(f'Columnas inesperadas. Faltan: {sorted(set(COLUMNS)-set(df.columns))}; '
                         f'sobran: {sorted(set(df.columns)-set(COLUMNS))}')
    df = df[COLUMNS].copy()
    if len(df) != 363:
        raise ValueError(f'Esperadas 363 filas; encontradas {len(df)}')

    for name in STRING_COLUMNS:
        # Preserve explicit NULLs, never coerce them to literal 'nan'.
        df[name] = df[name].astype('string').str.strip()
    for name in FLOAT_COLUMNS:
        df[name] = pd.to_numeric(df[name], errors='raise').astype('float64')
        if any(not math.isfinite(float(x)) for x in df[name].dropna()):
            raise ValueError(f'Número infinito o inválido: {name}')
    df['periodo'] = pd.to_numeric(df['periodo'], errors='raise').astype('int64')
    if not df['periodo'].eq(2024).all():
        raise ValueError('Años distintos a 2024')

    for name in BOOL_COLUMNS:
        def bool_or_fail(v):
            if isinstance(v, bool):
                return v
            if str(v).strip().lower() in {'true', '1'}:
                return True
            if str(v).strip().lower() in {'false', '0'}:
                return False
            raise ValueError(f'Booleano no reconocido en {name}: {v!r}')
        df[name] = df[name].map(bool_or_fail).astype(bool)

    if df[KEY].isna().any().any() or df.duplicated(KEY).any():
        raise ValueError('Identificadores geográficos/temporales nulos o duplicados')
    if df['nivel'].value_counts().to_dict() != {'comuna': 346, 'region': 16, 'pais': 1}:
        raise ValueError(f'Niveles inesperados: {df.nivel.value_counts().to_dict()}')
    if set(df['indicador_id'].dropna()) != {'pct_ocupados_construccion'}:
        raise ValueError('Indicadores distintos del porcentaje de ocupados en construcción')
    if not df['unidad'].eq('%').all():
        raise ValueError('Se esperaba unidad % para todos los registros')

    comunas = df.loc[df.nivel == 'comuna', 'codigo']
    regiones = df.loc[df.nivel == 'region', 'codigo']
    pais = df.loc[df.nivel == 'pais', 'codigo']
    if not comunas.str.fullmatch(r'\d{5}').all() or comunas.nunique() != 346:
        raise ValueError('Códigos comunales no válidos / no hay 346 CUT distintos')
    if not regiones.str.fullmatch(r'\d{2}').all() or regiones.nunique() != 16:
        raise ValueError('Códigos regionales no válidos / no hay 16 regiones')
    if pais.tolist() != ['CL']:
        raise ValueError('Código nacional distinto de CL')
    if set(comunas.str[:2]) != set(regiones):
        raise ValueError('Los códigos de región no concuerdan con los CUT comunales')

    missing = df['valor'].isna()
    if missing.sum() != 1:
        raise ValueError(f'Se esperaba exactamente un valor ausente; encontrados {missing.sum()}')
    falta = df.loc[missing].iloc[0]
    if (falta['nivel'], falta['codigo'], falta['estado']) != ('comuna', '12103', 'sin_valor'):
        raise ValueError('El único valor faltante debería ser Río Verde, CUT 12103')
    if df.loc[missing, ['numerador', 'denominador']].notna().any().any():
        raise ValueError('Río Verde tiene valor nulo pero numerador/denominador no nulos')
    if not df.loc[~missing, 'estado'].eq('observado').all():
        raise ValueError('Hay estados no observados en filas con valor')
    if df.loc[~missing, ['numerador', 'denominador']].isna().any().any():
        raise ValueError('Hay numerador o denominador ausente con valor disponible')

    v = df.loc[~missing]
    if (v['denominador'] <= 0).any() or (v['numerador'] < 0).any():
        raise ValueError('Conteos o denominadores negativos/0')
    computed = v['numerador'] / v['denominador'] * 100
    err = (v['valor'] - computed).abs()
    if (err > 0.01).any():
        raise ValueError(f'Porcentaje incompatible: diferencia máxima {err.max()}')
    if ((v['valor'] < 0) | (v['valor'] > 100)).any():
        raise ValueError('Porcentaje fuera del intervalo [0,100]')

    df = df.sort_values(KEY).reset_index(drop=True)
    print('=== OBSERVATORIO: AUDITORÍA (SIN ESCRITURAS) ===')
    print('Filas: 363 | nivel: 346 comunas + 16 regiones + 1 país | año: 2024')
    print('CUT comunales: 346 únicos | duplicados: 0 | indicadores: 1')
    print(f'Porcentajes evaluados: {len(v)} | diferencia máxima: {err.max():.9g}')
    print('Valor NULL: 1 (Río Verde, CUT 12103), sin imputar')
    print('Los niveles regional/nacional son agregados; NO sumar con comunas.')
    return df


def make_parquet(df):
    import pyarrow as pa
    import pyarrow.parquet as pq

    mapping = {
        'VARCHAR': pa.string(), 'DOUBLE': pa.float64(),
        'BIGINT': pa.int64(), 'BOOLEAN': pa.bool_(),
    }
    arrays = []
    for name, typ in FIELDS:
        data = df[name].tolist()
        data = [None if __import__('pandas').isna(x) else x for x in data]
        arrays.append(pa.array(data, type=mapping[typ]))
    table = pa.Table.from_arrays(arrays, names=COLUMNS)
    buff = io.BytesIO()
    pq.write_table(table, buff, compression='snappy')
    return buff.getvalue()


def connect_s3():
    import boto3
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    user, pwd = os.getenv('MINIO_ROOT_USER'), os.getenv('MINIO_ROOT_PASSWORD')
    if not user or not pwd:
        raise RuntimeError('Faltan MINIO_ROOT_USER y MINIO_ROOT_PASSWORD en .env')
    return boto3.client('s3', endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://127.0.0.1:9000'),
                        aws_access_key_id=user, aws_secret_access_key=pwd,
                        region_name='us-east-1')


def publish_immutable(s3, bucket, key, data, content_type):
    from botocore.exceptions import ClientError
    try:
        previous = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') not in {'404', 'NoSuchKey', 'NotFound'}:
            raise
    else:
        if hashlib.sha256(previous).digest() != hashlib.sha256(data).digest():
            raise RuntimeError(f'El objeto existe con contenido distinto: s3://{bucket}/{key}')
        print(f'Ya publicado, idéntico: s3://{bucket}/{key}')
        return
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    print(f'Publicado: s3://{bucket}/{key}')


def trino_connect():
    from trino.dbapi import connect
    return connect(host='127.0.0.1', port=8088, user='inmobiliario',
                   catalog='delta', schema='inmobiliario', http_scheme='http')


def compare_all(cur, table, df):
    cols = ', '.join(COLUMNS)
    order = ', '.join(KEY)
    cur.execute(f'SELECT {cols} FROM {table} ORDER BY {order}')
    found = cur.fetchall()
    if len(found) != len(df):
        raise RuntimeError(f'{table}: {len(found)} registros frente a {len(df)} esperados')
    import pandas as pd
    for idx, (actual, expected) in enumerate(zip(found, df.itertuples(index=False, name=None)), 1):
        for j, ((name, typ), a, b) in enumerate(zip(FIELDS, actual, expected)):
            aa = None if a is None or pd.isna(a) else a
            bb = None if b is None or pd.isna(b) else b
            if aa is None or bb is None:
                ok = aa is None and bb is None
            elif typ == 'DOUBLE':
                ok = math.isclose(float(aa), float(bb), abs_tol=1e-9, rel_tol=1e-11)
            elif typ == 'BOOLEAN':
                ok = bool(aa) == bool(bb)
            elif typ == 'BIGINT':
                ok = int(aa) == int(bb)
            else:
                ok = str(aa) == str(bb)
            if not ok:
                raise RuntimeError(f'{table}: diferencia en fila {idx}, columna {name}: '
                                   f'{str(aa)[:80]} vs {str(bb)[:80]}')
    print(f'Tabla verificada fila a fila: {table} ({len(found)} registros)')


def load(df):
    payload = make_parquet(df)
    SILVER.parent.mkdir(parents=True, exist_ok=True)
    if SILVER.exists():
        if hashlib.sha256(SILVER.read_bytes()).digest() != hashlib.sha256(payload).digest():
            raise RuntimeError(f'Silver local existente con datos diferentes: {SILVER}')
    else:
        SILVER.write_bytes(payload)
        print(f'Parquet Silver local: {SILVER.relative_to(ROOT)}')

    s3 = connect_s3()
    for bucket in (RAW_BUCKET, SILVER_BUCKET):
        s3.head_bucket(Bucket=bucket)
    publish_immutable(s3, RAW_BUCKET, RAW_KEY, RAW.read_bytes(), 'text/csv')
    publish_immutable(s3, SILVER_BUCKET, SILVER_KEY, payload, 'application/octet-stream')

    digest = hashlib.sha256(payload).hexdigest()[:12]
    stage_name = f'observatorio_stage_{digest}'
    stage = f'lake.inmobiliario.{stage_name}'
    conn = trino_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES FROM lake.inmobiliario LIKE '{stage_name}'")
        if not cur.fetchall():
            cols = ', '.join(f'{name} {typ}' for name, typ in FIELDS)
            cur.execute(f'CREATE TABLE {stage} ({cols}) WITH '
                        f"(format='PARQUET', external_location='s3a://{SILVER_BUCKET}/{STAGE_DIR}')")
            cur.fetchall()
            print(f'Tabla de lectura registrada: {stage}')
        compare_all(cur, stage, df)

        cur.execute(f"SHOW TABLES FROM delta.inmobiliario LIKE '{DELTA_NAME}'")
        if not cur.fetchall():
            if s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=DELTA_PREFIX,
                                   MaxKeys=1).get('KeyCount', 0):
                raise RuntimeError('Ruta Delta contiene archivos sin tabla registrada; revisar antes de continuar')
            cols = ', '.join(COLUMNS)
            cur.execute(f'CREATE TABLE {DELTA_TABLE} WITH '
                        f"(location='s3a://{SILVER_BUCKET}/{DELTA_PREFIX}') "
                        f'AS SELECT {cols} FROM {stage}')
            cur.fetchall()
            print(f'Tabla Delta creada: {DELTA_TABLE}')
        else:
            print('Tabla Delta existente: se compara sin sobrescribir')
        compare_all(cur, DELTA_TABLE, df)
    finally:
        conn.close()
    print('INTEGRACIÓN OBSERVATORIO A SILO Y DELTA COMPLETADA. PostgreSQL intacto.')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    m = p.add_mutually_exclusive_group(required=True)
    m.add_argument('--auditar', action='store_true')
    m.add_argument('--cargar', action='store_true')
    args = p.parse_args()
    df = audit()
    if args.cargar:
        load(df)
    else:
        print('AUDITORÍA COMPLETA SIN ESCRITURAS.')


if __name__ == '__main__':
    main()
