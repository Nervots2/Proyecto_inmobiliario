"""Integra estadísticas por destino del SII, semestre 2026-S1.

Desde la raíz del repositorio:
    python scripts/integrar_sii_destinos_2026_s1.py --auditar
    python scripts/integrar_sii_destinos_2026_s1.py --cargar

- Lee los dos CSV ya preparados en datalake_procesado/silver/sii/.
- Publica copias idénticas en Silo/MinIO: lake-procesado/silver/sii/2026-S1/.
- Crea UNA tabla Delta nacional; la RM se consulta con WHERE region = 'Metropolitana'.
- No modifica tablas existentes, publicaciones, PostgreSQL ni archivos originales.
- Si ya existen objetos S3 diferentes o archivos Delta huérfanos, detiene el proceso.
- Una tabla Delta existente se valida y no se recrea.

Requisitos de --cargar: boto3, python-dotenv, trino y Docker con servicios
postgres_gis, minio, hive_metastore y trino disponibles.
"""

import argparse
import csv
import io
import os
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
ORIGEN = RAIZ / 'datalake_procesado' / 'silver' / 'sii'
ARCHIVO_NACIONAL = 'sii_destinos_comuna_2026_s1_nacional.csv'
ARCHIVO_RM = 'sii_destinos_comuna_2026_s1_rm.csv'

BUCKET = 'lake-procesado'
PREFIJO_SILVER = 'silver/sii/2026-S1/'
TABLA = 'delta.inmobiliario.sii_destinos_comuna_2026_s1'
UBICACION = 's3a://lake-procesado/delta/sii_destinos_comuna_2026_s1/'
PREFIJO_DELTA = 'delta/sii_destinos_comuna_2026_s1/'

COLUMNAS_TEXTO = ['region', 'comuna', 'periodo', 'fuente']
COLUMNAS_CATEGORIAS = [
    'predios_comercio', 'predios_deporte_y_recreacion',
    'predios_educacion_y_cultura', 'predios_hotel_o_motel',
    'predios_habitacional', 'predios_industria', 'predios_bienes_comunes',
    'predios_bodega_y_almacenaje', 'predios_mineria', 'predios_oficina',
    'predios_administracion_publica_y_defensa', 'predios_culto',
    'predios_salud', 'predios_transporte', 'predios_otros_no_considerados',
    'predios_sitio_eriazo', 'predios_estacionamiento',
]
COLUMNAS = COLUMNAS_TEXTO + COLUMNAS_CATEGORIAS + ['predios_total']

# Valores verificados en las copias originales del proyecto; si cambian,
# hay que revisar la procedencia antes de actualizar estas referencias.
N_FILAS = 346
TOTAL_NACIONAL = 8_560_728
N_FILAS_RM = 52
TOTAL_RM = 3_700_127


def leer_csv(nombre):
    ruta = ORIGEN / nombre
    if not ruta.is_file():
        raise FileNotFoundError(f'No se encontró {ruta}')
    contenido = ruta.read_bytes()
    lector = csv.DictReader(io.StringIO(contenido.decode('utf-8-sig')))
    if lector.fieldnames != COLUMNAS:
        raise ValueError(f'Columnas inesperadas en {nombre}: {lector.fieldnames}')
    filas = list(lector)
    return contenido, filas


def validar_filas(filas, *, esperado, total, solo_rm=False):
    if len(filas) != esperado:
        raise ValueError(f'Se esperaban {esperado} filas y se encontraron {len(filas)}')
    ids = set()
    suma_predios = 0
    for fila in filas:
        if fila['periodo'] != '2026-S1' or fila['fuente'] != 'SII':
            raise ValueError(f'Período o fuente inesperados: {fila}')
        if not fila['comuna'].strip() or not fila['region'].strip():
            raise ValueError('Comuna o región vacías')
        if solo_rm and fila['region'] != 'Metropolitana':
            raise ValueError(f'El archivo RM contiene otra región: {fila["region"]}')
        clave = (fila['region'].strip().casefold(), fila['comuna'].strip().casefold())
        if clave in ids:
            raise ValueError(f'Comuna duplicada: {clave}')
        ids.add(clave)
        for columna in COLUMNAS_CATEGORIAS + ['predios_total']:
            valor = fila[columna]
            if not valor.isdecimal():
                raise ValueError(f'Dato no entero en {fila["comuna"]}, {columna}: {valor!r}')
        categorias = sum(int(fila[c]) for c in COLUMNAS_CATEGORIAS)
        predios = int(fila['predios_total'])
        if categorias != predios:
            raise ValueError(f'Las categorías no suman el total en {fila["comuna"]}')
        suma_predios += predios
    if suma_predios != total:
        raise ValueError(f'Total de predios inesperado: {suma_predios:,} en vez de {total:,}')
    return suma_predios


def auditar():
    nacional_bytes, nacional = leer_csv(ARCHIVO_NACIONAL)
    rm_bytes, rm = leer_csv(ARCHIVO_RM)
    validar_filas(nacional, esperado=N_FILAS, total=TOTAL_NACIONAL)
    validar_filas(rm, esperado=N_FILAS_RM, total=TOTAL_RM, solo_rm=True)
    nacionales_rm = sorted(
        (r for r in nacional if r['region'] == 'Metropolitana'),
        key=lambda r: r['comuna']
    )
    rm_ordenado = sorted(rm, key=lambda r: r['comuna'])
    if nacionales_rm != rm_ordenado:
        raise ValueError('El CSV RM no coincide exactamente con el subconjunto nacional')
    print('=== AUDITORÍA SII 2026-S1 (SIN ESCRITURAS) ===')
    print(f'Nacional: {len(nacional)} comunas | predios: {TOTAL_NACIONAL:,}'.replace(',', '.'))
    print(f'RM: {len(rm)} comunas | predios: {TOTAL_RM:,}'.replace(',', '.'))
    print(f'Categorías de destino: {len(COLUMNAS_CATEGORIAS)} | sumas por fila: correctas')
    print('Duplicados regionales/comunales: 0 | CSV RM coincide con nacional')
    return {ARCHIVO_NACIONAL: nacional_bytes, ARCHIVO_RM: rm_bytes}, nacional


def cliente_s3():
    import boto3
    from dotenv import load_dotenv
    load_dotenv(RAIZ / '.env')
    usuario = os.getenv('MINIO_ROOT_USER')
    clave = os.getenv('MINIO_ROOT_PASSWORD')
    if not usuario or not clave:
        raise RuntimeError('Faltan MINIO_ROOT_USER o MINIO_ROOT_PASSWORD en .env')
    return boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://127.0.0.1:9000'),
        aws_access_key_id=usuario,
        aws_secret_access_key=clave,
        region_name='us-east-1',
    )


def publicar_csv(s3, nombre, contenido):
    from botocore.exceptions import ClientError
    llave = PREFIJO_SILVER + nombre
    try:
        remoto = s3.get_object(Bucket=BUCKET, Key=llave)['Body'].read()
    except ClientError as exc:
        codigo = exc.response.get('Error', {}).get('Code', '')
        if codigo not in {'NoSuchKey', '404', 'NotFound'}:
            raise
    else:
        if remoto != contenido:
            raise RuntimeError(f'Ya existe un CSV DIFERENTE en s3://{BUCKET}/{llave}. No se sobrescribirá.')
        print(f'CSV ya publicado, sin cambios: s3://{BUCKET}/{llave}')
        return
    s3.put_object(Bucket=BUCKET, Key=llave, Body=contenido, ContentType='text/csv; charset=utf-8')
    print(f'CSV publicado: s3://{BUCKET}/{llave}')


def literal_sql(valor):
    return "'" + valor.replace("'", "''") + "'"


def sql_crear(filas):
    valores = []
    for fila in filas:
        partes = [literal_sql(fila[c]) for c in COLUMNAS_TEXTO]
        partes += [f'CAST({int(fila[c])} AS BIGINT)' for c in COLUMNAS_CATEGORIAS + ['predios_total']]
        valores.append('(' + ', '.join(partes) + ')')
    return (
        f'CREATE TABLE {TABLA} WITH (location = {literal_sql(UBICACION)}) AS\n'
        'SELECT * FROM (VALUES\n  ' + ',\n  '.join(valores) +
        '\n) AS datos (' + ', '.join(COLUMNAS) + ')'
    )


def comprobar_tabla(cursor):
    cursor.execute(
        f'''SELECT COUNT(*),
               COUNT(DISTINCT CONCAT(region, '|', comuna)),
               SUM(predios_total),
               SUM(CASE WHEN region = 'Metropolitana' THEN 1 ELSE 0 END),
               SUM(CASE WHEN region = 'Metropolitana' THEN predios_total ELSE 0 END)
            FROM {TABLA}'''
    )
    resultado = cursor.fetchone()
    esperado = (N_FILAS, N_FILAS, TOTAL_NACIONAL, N_FILAS_RM, TOTAL_RM)
    if tuple(resultado) != esperado:
        raise RuntimeError(f'Delta no coincide con la auditoría: {resultado} (esperado {esperado})')
    print(f'Tabla validada: {resultado[0]} comunas, {resultado[2]:,} predios'.replace(',', '.'))
    print(f'Subconjunto RM: {resultado[3]} comunas, {resultado[4]:,} predios'.replace(',', '.'))


def cargar_delta(s3, filas):
    from trino.dbapi import connect
    conexion = connect(
        host='127.0.0.1', port=8088, user='inmobiliario',
        catalog='delta', schema='inmobiliario', http_scheme='http'
    )
    try:
        cursor = conexion.cursor()
        cursor.execute("SHOW TABLES FROM delta.inmobiliario LIKE 'sii_destinos_comuna_2026_s1'")
        if cursor.fetchall():
            print('La tabla Delta ya existe; no se recreará.')
            comprobar_tabla(cursor)
            return

        # Previene que un CTAS pise archivos residuales de otro intento.
        objetos = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIJO_DELTA, MaxKeys=1)
        if objetos.get('KeyCount', 0):
            raise RuntimeError('Hay archivos en la ubicación Delta sin tabla registrada. Revisar antes de seguir.')

        print('Creando tabla Delta nacional (sin modificar tablas existentes)...')
        cursor.execute(sql_crear(filas))
        cursor.fetchall()
        comprobar_tabla(cursor)
        print(f'Tabla Delta creada: {TABLA}')
    finally:
        conexion.close()


def main():
    ap = argparse.ArgumentParser(description='Integración SII destinos 2026-S1')
    modo = ap.add_mutually_exclusive_group(required=True)
    modo.add_argument('--auditar', action='store_true', help='Solo validar CSV localmente')
    modo.add_argument('--cargar', action='store_true', help='Publicar CSV en S3 y crear Delta')
    args = ap.parse_args()

    archivos, filas = auditar()
    if args.auditar:
        print('Auditoría terminada. No se modificó ningún archivo ni servicio.')
        return
    s3 = cliente_s3()
    # Ambos CSV son copias exactas de las versiones locales ya auditadas.
    for nombre, contenido in archivos.items():
        publicar_csv(s3, nombre, contenido)
    cargar_delta(s3, filas)
    print('Integración SII 2026-S1 completada. PostgreSQL no se modificó.')


if __name__ == '__main__':
    main()
