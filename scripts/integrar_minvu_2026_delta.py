"""Integra las cinco tablas regionales del informe MINVU DT N.10 (2026).

Desde la raíz de Proyecto_inmobiliario:
    python scripts/integrar_minvu_2026_delta.py --auditar
    python scripts/integrar_minvu_2026_delta.py --cargar

--auditar: lee los cinco CSV, comprueba 16 regiones + total nacional,
           los totales publicados y coherencia de transacciones.
--cargar: además publica PDF crudo y CSV silver en Silo/MinIO, crea cinco
         tablas Delta independientes mediante Trino y comprueba fila a fila.

No escribe en PostgreSQL, ni modifica tablas Delta existentes o archivos
locales. Si existe una tabla con datos distintos, falla sin sobrescribirla.
"""
from __future__ import annotations

import argparse
import csv
from decimal import Decimal, InvalidOperation
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CSV_DIR = ROOT / 'datalake_procesado' / 'silver' / 'minvu'
PDF = ROOT / 'datalake_crudo' / 'minvu' / 'minvu_analisis_suelo_urbano_2026.pdf'
BUCKET_SILVER = 'lake-procesado'
BUCKET_RAW = 'lake-crudo'
SILVER_PREFIX = 'silver/minvu/2026/'
DELTA_PREFIX = 'delta/'
RAW_KEY = 'minvu/2026/minvu_analisis_suelo_urbano_2026.pdf'
META = ('fuente', 'tabla', 'pagina_pdf', 'periodo_referencia',
        'nivel_geografico', 'codigo_region', 'region')

# Esquemas y totales tal como los produce extraer_minvu_tablas.py.
# Se preservan periodos diferentes entre tablas; el anio de publicacion es 2026.
TABLES = (
    (1, 5, 'crecimiento_urbano_2017_2024', '2017-2024',
     ('superficie_2017_ha', 'superficie_2024_ha', 'crecimiento_bruto_ha',
      'crecimiento_bruto_pct', 'crecimiento_dentro_limite_urbano_ha',
      'crecimiento_dentro_limite_urbano_pct', 'crecimiento_fuera_limite_urbano_ha',
      'crecimiento_fuera_limite_urbano_pct'),
     ('186152.6', '212715.2', '26562.5', '14.3', '16137.0', '60.8',
      '10472.4', '39.4')),
    (2, 6, 'superficie_urbana_territorios_sanitarios', 'cobertura_sanitaria_2025',
     ('area_urbana_normada_ha', 'area_dentro_to_ha', 'area_fuera_to_ha',
      'area_dentro_to_pct', 'area_fuera_to_pct'),
     ('749674.0', '276859.37', '472204.0', '37.0', '63.0')),
    (3, 7, 'suelo_urbano_disponible', 'cobertura_sanitaria_2025',
     ('suelo_disponible_total_ha', 'suelo_disponible_dentro_to_ha',
      'suelo_disponible_fuera_to_ha', 'suelo_disponible_dentro_to_pct',
      'suelo_disponible_fuera_to_pct'),
     ('487455.89', '64964.42', '422491.47', '13.0', '87.0')),
    (4, 10, 'transacciones_valor_suelo', '2020-2025',
     ('transacciones_cantidad', 'valor_promedio_uf_m2'),
     ('26729', '1.8')),
    (5, 11, 'transacciones_suelo_territorios_sanitarios', '2020-2025',
     ('transacciones_fuera_to_cantidad', 'valor_promedio_fuera_to_uf_m2',
      'transacciones_dentro_to_cantidad', 'valor_promedio_dentro_to_uf_m2'),
     ('17778', '1.0', '8951', '3.4')),
)
REGIONES = {
    'arica y parinacota': '15', 'tarapaca': '01', 'antofagasta': '02',
    'atacama': '03', 'coquimbo': '04', 'valparaiso': '05',
    'metropolitana': '13', 'ohiggins': '06', 'maule': '07', 'nuble': '16',
    'biobio': '08', 'la araucania': '09', 'los rios': '14',
    'los lagos': '10', 'aysen': '11', 'magallanes': '12', 'nacional': None,
}


def filename(t):
    return f'minvu_2026_t{t[0]}_{t[2]}.csv'


def delta_name(t):
    return f'minvu_2026_t{t[0]}_{t[2]}'


def is_count(col):
    return col.endswith('_cantidad')


def number(raw, *, integer=False):
    try:
        d = Decimal(str(raw))
    except (InvalidOperation, TypeError):
        raise ValueError(f'Número inválido en CSV: {raw!r}')
    if not d.is_finite() or d < 0 or (integer and d != d.to_integral_value()):
        raise ValueError(f'Número inválido o negativo: {raw!r}')
    if len(d.as_tuple().digits) > 20:
        raise ValueError(f'Número fuera de rango: {raw!r}')
    return d


def auditar():
    archivos, conjuntos = {}, {}
    for spec in TABLES:
        num, page, name, period, measures, national = spec
        path = CSV_DIR / filename(spec)
        if not path.is_file():
            raise FileNotFoundError(f'Falta CSV del MINVU: {path}')
        raw = path.read_bytes()
        with path.open(encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f, delimiter=';')
            expected_columns = list(META) + list(measures)
            if reader.fieldnames != expected_columns:
                raise ValueError(f'{path.name}: columnas incorrectas.\n'
                                 f'Encontradas: {reader.fieldnames}\n'
                                 f'Esperadas: {expected_columns}')
            rows = list(reader)
        if len(rows) != 17:
            raise ValueError(f'{path.name}: se esperaban 17 filas; hay {len(rows)}')
        parsed = {}
        for row in rows:
            region = row['region'].strip()
            if region not in REGIONES or region in parsed:
                raise ValueError(f'{path.name}: región desconocida o repetida: {region!r}')
            code = REGIONES[region]
            observed = row['codigo_region'].strip()
            if (code is None and observed) or (code is not None and
                    (not observed.isdecimal() or f'{int(observed):02d}' != code)):
                raise ValueError(f'{path.name}: código regional no coincide: {region}: {observed!r}')
            if (row['fuente'] != 'MINVU_DT10_2026' or row['tabla'] != str(num)
                    or row['pagina_pdf'] != str(page) or row['periodo_referencia'] != period
                    or row['nivel_geografico'] != ('nacional' if region == 'nacional' else 'regional')):
                raise ValueError(f'{path.name}: metadatos incorrectos en {region!r}')
            values = {col: number(row[col], integer=is_count(col)) for col in measures}
            parsed[region] = values
        if set(parsed) != set(REGIONES):
            raise ValueError(f'{path.name}: regiones faltantes: {set(REGIONES) - set(parsed)}')
        for col, expected in zip(measures, national):
            if abs(parsed['nacional'][col] - Decimal(expected)) > Decimal('0.02'):
                raise ValueError(f'{path.name}: total nacional distinto en {col}: '
                                 f'{parsed["nacional"][col]} != {expected}')
        conjuntos[num] = parsed
        archivos[num] = raw
        print(f'OK tabla {num}: 16 regiones + Nacional; {len(measures)} indicadores; '
              f'archivo {path.name}')

    for region in REGIONES:
        amount = conjuntos[4][region]['transacciones_cantidad']
        components = (conjuntos[5][region]['transacciones_fuera_to_cantidad'] +
                      conjuntos[5][region]['transacciones_dentro_to_cantidad'])
        if amount != components:
            raise ValueError(f'Transacciones no cuadran en {region}: {amount} != {components}')
    print('OK: transacciones tabla 4 = suma dentro/fuera de la tabla 5 (17 filas).')
    print('AUDITORÍA MINVU COMPLETA: 5 CSV y 85 filas. La fila Nacional es un agregado.')
    return archivos, conjuntos


def connect_s3():
    import boto3
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    user = os.getenv('MINIO_ROOT_USER')
    password = os.getenv('MINIO_ROOT_PASSWORD')
    if not user or not password:
        raise RuntimeError('Faltan credenciales MINIO_ROOT_USER / MINIO_ROOT_PASSWORD en .env')
    return boto3.client('s3',
                        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://127.0.0.1:9000'),
                        aws_access_key_id=user, aws_secret_access_key=password,
                        region_name='us-east-1')


def upload_immutable(s3, bucket, key, payload, content_type):
    from botocore.exceptions import ClientError
    try:
        actual = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
    except ClientError as e:
        if e.response.get('Error', {}).get('Code', '') not in {'404', 'NoSuchKey', 'NotFound'}:
            raise
    else:
        if actual != payload:
            raise RuntimeError(f'Existe un archivo DIFERENTE en s3://{bucket}/{key}; '
                               'no se sobrescribió. Revisar manualmente.')
        print(f'Ya publicado, intacto: s3://{bucket}/{key}')
        return
    s3.put_object(Bucket=bucket, Key=key, Body=payload, ContentType=content_type)
    print(f'Publicado: s3://{bucket}/{key}')


def sql_string(s):
    return "'" + str(s).replace("'", "''") + "'"


def sql_for_create(spec, parsed):
    num, page, name, period, measures, _ = spec
    parts = []
    for region, code in REGIONES.items():
        row = [
            sql_string('MINVU_DT10_2026'), f'CAST({num} AS INTEGER)',
            f'CAST({page} AS INTEGER)', sql_string(period),
            sql_string('nacional' if region == 'nacional' else 'regional'),
            'CAST(NULL AS VARCHAR)' if code is None else sql_string(code),
            sql_string(region),
        ]
        for col in measures:
            val = parsed[region][col]
            row.append(f'CAST({str(val)} AS ' +
                       ('BIGINT)' if is_count(col) else 'DECIMAL(20,4))'))
        parts.append('(' + ', '.join(row) + ')')
    cols = ', '.join(META + measures)
    table = delta_name(spec)
    location = f's3a://{BUCKET_SILVER}/{DELTA_PREFIX}{table}/'
    return (f'CREATE TABLE delta.inmobiliario.{table} '
            f'WITH (location = {sql_string(location)}) AS\n'
            'SELECT * FROM (VALUES\n  ' + ',\n  '.join(parts) +
            '\n) AS t (' + cols + ')')


def expected_rows(spec, parsed):
    num, page, _, period, measures, _ = spec
    expected = {}
    for region, code in REGIONES.items():
        expected[region] = (
            'MINVU_DT10_2026', num, page, period,
            'nacional' if code is None else 'regional', code, region,
            *[parsed[region][c] for c in measures]
        )
    return expected


def validate_delta(cursor, spec, parsed):
    table = delta_name(spec)
    cols = ', '.join(META + spec[4])
    cursor.execute(f'SELECT {cols} FROM delta.inmobiliario.{table}')
    results = cursor.fetchall()
    if len(results) != 17:
        raise RuntimeError(f'{table}: se esperaban 17 filas en Delta y hay {len(results)}')
    expected = expected_rows(spec, parsed)
    seen = set()
    for row in results:
        region = row[6]
        if region not in expected or region in seen:
            raise RuntimeError(f'{table}: región desconocida o duplicada: {region!r}')
        seen.add(region)
        goal = expected[region]
        if tuple(row[:7]) != goal[:7]:
            raise RuntimeError(f'{table}: metadatos distintos para {region}: {row[:7]}')
        for col, actual, exp in zip(spec[4], row[7:], goal[7:]):
            if Decimal(str(actual)) != exp:
                raise RuntimeError(f'{table}: {region}, {col}: {actual} != {exp}')
    if seen != set(REGIONES):
        raise RuntimeError(f'{table}: faltan regiones: {set(REGIONES) - seen}')
    print(f'Delta validado: delta.inmobiliario.{table} (17 filas, sin duplicados).')


def create_deltas(s3, conjuntos):
    from trino.dbapi import connect
    conn = connect(host='127.0.0.1', port=8088, user='inmobiliario',
                   catalog='delta', schema='inmobiliario', http_scheme='http')
    try:
        cursor = conn.cursor()
        for spec in TABLES:
            table = delta_name(spec)
            cursor.execute(f"SHOW TABLES FROM delta.inmobiliario LIKE {sql_string(table)}")
            if not cursor.fetchall():
                prefix = f'{DELTA_PREFIX}{table}/'
                if s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=prefix,
                                      MaxKeys=1).get('KeyCount', 0):
                    raise RuntimeError(f'Hay archivos Delta huérfanos en {prefix}; '
                                       'revisar antes de seguir. No se borró nada.')
                cursor.execute(sql_for_create(spec, conjuntos[spec[0]]))
                cursor.fetchall()
                print(f'Tabla Delta creada: delta.inmobiliario.{table}')
            else:
                print(f'Tabla Delta ya existente: delta.inmobiliario.{table}; solo se validará.')
            validate_delta(cursor, spec, conjuntos[spec[0]])
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description='Integra las 5 tablas MINVU 2026 a Silo y Delta Lake')
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument('--auditar', action='store_true', help='Sin escrituras')
    mode.add_argument('--cargar', action='store_true', help='Carga PDF/CSV a S3 y crea Delta')
    args = ap.parse_args()

    archivos, conjuntos = auditar()
    if args.auditar:
        print('No se escribieron archivos, S3, PostgreSQL ni Delta Lake.')
        return
    if not PDF.is_file():
        raise FileNotFoundError(f'Falta el PDF original: {PDF}')
    s3 = connect_s3()
    upload_immutable(s3, BUCKET_RAW, RAW_KEY, PDF.read_bytes(), 'application/pdf')
    for spec in TABLES:
        filename_ = filename(spec)
        upload_immutable(s3, BUCKET_SILVER, SILVER_PREFIX + filename_,
                         archivos[spec[0]], 'text/csv; charset=utf-8')
    create_deltas(s3, conjuntos)
    print('INTEGRACIÓN MINVU A SILO Y DELTA COMPLETADA. PostgreSQL no se modificó.')


if __name__ == '__main__':
    main()
