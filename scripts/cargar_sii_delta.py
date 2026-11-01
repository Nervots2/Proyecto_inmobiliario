"""Carga estadísticas comunales del SII desde Silver (S3) a Delta Lake (Trino).

Ejecutar desde la raíz del repositorio:
    python scripts/cargar_sii_delta.py

Requiere: boto3, python-dotenv, trino y servicios Silo/MinIO, Trino, Hive.
- Lee CSV ya publicado en lake-procesado/silver/sii/2025-S2/.
- Valida 52 comunas y 3.690.399 predios.
- Crea la tabla Delta, sin modificar la tabla de publicaciones ni PostgreSQL.
- Si la tabla ya existe, verifica su contenido y no duplica registros.
- Nunca borra archivos; si el destino tiene restos de intentos anteriores,
  detiene la carga para revisión manual.
"""

import csv
import io
import os
from datetime import date
from pathlib import Path

import boto3
from dotenv import load_dotenv
from trino.dbapi import connect

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / '.env')
BUCKET = 'lake-procesado'
CLAVE_CSV = 'silver/sii/2025-S2/sii_rm_2025_s2_comunas.csv'
TABLA = 'delta.inmobiliario.sii_rm_2025_s2'
UBICACION = 's3a://lake-procesado/delta/sii_rm_2025_s2/'
PREFIJO_DESTINO = 'delta/sii_rm_2025_s2/'
ESPERADAS = 52
TOTAL_PREDIOS_ESPERADO = 3_690_399

TEXTO = {'fuente', 'region', 'periodo', 'comuna'}
FECHAS = {'fecha_referencia_montos'}
PORCENTAJES_DECIMALES = {'porcentaje_contribucion_region', 'porcentaje_contribucion_pais'}


def validar(filas, campos):
    columnas_necesarias = TEXTO | FECHAS | {
        'predios_totales', 'avaluo_total_miles_clp',
        'predios_exentos', 'predios_afectos', 'predios_no_agricolas',
    }
    if not columnas_necesarias.issubset(campos):
        raise ValueError('Faltan columnas requeridas: ' + str(columnas_necesarias - set(campos)))
    if len(filas) != ESPERADAS:
        raise ValueError(f'Se esperaban {ESPERADAS} comunas y hay {len(filas)}')
    if any(f['periodo'] != '2025-S2' or f['fuente'] != 'SII' for f in filas):
        raise ValueError('Origen o período inesperado')
    if len({f['comuna'].strip().upper() for f in filas}) != ESPERADAS:
        raise ValueError('Existen comunas duplicadas')
    predios = sum(int(f['predios_totales']) for f in filas)
    if predios != TOTAL_PREDIOS_ESPERADO:
        raise ValueError(f'Total de predios incompatible con el SII: {predios}')
    for fila in filas:
        if sum(int(fila[c]) for c in ['predios_exentos', 'predios_afectos']) != int(fila['predios_totales']):
            raise ValueError(f'Predios exentos + afectos no coinciden en {fila["comuna"]}')
    print(f'CSV remoto validado: {len(filas)} comunas, {predios:,} predios'.replace(',', '.'))


def valor_sql(columna, valor):
    valor = (valor or '').strip()
    if not valor:
        return 'NULL'
    if columna in TEXTO:
        return "'" + valor.replace("'", "''") + "'"
    if columna in FECHAS:
        date.fromisoformat(valor)
        return f"DATE '{valor}'"
    if columna in PORCENTAJES_DECIMALES:
        # Double para los porcentajes expresados con decimales en el CSV.
        numero = float(valor)
        if not (float('-inf') < numero < float('inf')):
            raise ValueError('Valor numérico no finito en ' + columna)
        return f'CAST({numero} AS DOUBLE)'
    # Enteros: millones de M$ pueden exceder INT32.
    return f'CAST({int(valor)} AS BIGINT)'


def crear_sql(filas, campos):
    valores = []
    for fila in filas:
        valores.append('(' + ', '.join(valor_sql(c, fila[c]) for c in campos) + ')')
    alias = ', '.join(campos)
    return (
        f'CREATE TABLE {TABLA} WITH (location = \'{UBICACION}\') AS\n'
        f'SELECT * FROM (VALUES\n  ' + ',\n  '.join(valores) +
        f'\n) AS datos ({alias})'
    )


def main():
    usuario = os.getenv('MINIO_ROOT_USER')
    clave = os.getenv('MINIO_ROOT_PASSWORD')
    if not usuario or not clave:
        raise RuntimeError('Se requieren MINIO_ROOT_USER y MINIO_ROOT_PASSWORD en .env')
    s3 = boto3.client('s3', endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://127.0.0.1:9000'),
                      aws_access_key_id=usuario, aws_secret_access_key=clave,
                      region_name='us-east-1')
    contenido = s3.get_object(Bucket=BUCKET, Key=CLAVE_CSV)['Body'].read()
    lector = csv.DictReader(io.StringIO(contenido.decode('utf-8-sig')))
    campos = lector.fieldnames or []
    filas = list(lector)
    validar(filas, campos)

    conexion = connect(host='127.0.0.1', port=8088, user='inmobiliario',
                       catalog='delta', schema='inmobiliario', http_scheme='http')
    try:
        cursor = conexion.cursor()
        cursor.execute("SHOW TABLES FROM delta.inmobiliario LIKE 'sii_rm_2025_s2'")
        if cursor.fetchall():
            cursor.execute(f'SELECT COUNT(*), SUM(predios_totales) FROM {TABLA}')
            cantidad, total = cursor.fetchone()
            if (cantidad, total) != (ESPERADAS, TOTAL_PREDIOS_ESPERADO):
                raise RuntimeError(f'Tabla existente incompatible: filas={cantidad}, predios={total}')
            print(f'Tabla Delta ya existe y está validada: {cantidad} comunas, {total} predios')
            return

        # Nunca reutilizar una ruta con residuos de CREATE TABLE fallidos.
        objetos = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIJO_DESTINO, MaxKeys=1)
        if objetos.get('KeyCount', 0):
            raise RuntimeError('La ruta Delta ya tiene archivos sin tabla registrada. Revisar antes de crear.')

        print('Creando tabla Delta independiente (52 filas) ...')
        cursor.execute(crear_sql(filas, campos))
        cursor.fetchall()  # Consumir la respuesta del CTAS.
        cursor.execute(f'SELECT COUNT(*), SUM(predios_totales) FROM {TABLA}')
        cantidad, total = cursor.fetchone()
        if (cantidad, total) != (ESPERADAS, TOTAL_PREDIOS_ESPERADO):
            raise RuntimeError(f'Tabla creada, pero su validación falló: {cantidad}, {total}')
        print(f'Tabla Delta creada: {TABLA}')
        print(f'Registros: {cantidad}; predios totales: {total:,}'.replace(',', '.'))
        print('PostgreSQL y la tabla Delta de publicaciones no se modificaron.')
    finally:
        conexion.close()


if __name__ == '__main__':
    main()
