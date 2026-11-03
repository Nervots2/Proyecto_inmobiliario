"""Carga idempotente MINVU 2026: 5 tablas Delta -> DW PostgreSQL.

Desde la raíz de Proyecto_inmobiliario, con Docker y (inmobiliario):
    python scripts/cargar_dw_minvu_2026.py --auditar
    python scripts/cargar_dw_minvu_2026.py --cargar

Requiere que integrar_minvu_2026_delta.py esté en scripts/, como en el proyecto.
La auditoría compara los 5 CSV locales con las 5 tablas Delta y verifica
las 16 regiones contra dw.dim_ubicacion (códigos oficiales ya integrados).

--cargar crea dw.dim_region (16 regiones) y cinco tablas de hechos MINVU,
          cada una con 16 regiones + 1 agregado nacional. Idempotente:
          nunca cambia hechos preexistentes diferentes; aborta y revierte.

No modifica dw.fact_publicaciones, dw.fact_sii_destinos, dw.dim_ubicacion,
los JSON, los CSV, Silo/MinIO ni Delta Lake.
"""
from __future__ import annotations

import argparse
from decimal import Decimal
import os
from pathlib import Path
import unicodedata
import re

from integrar_minvu_2026_delta import (
    TABLES, REGIONES, auditar, validate_delta, delta_name, is_count,
)

ROOT = Path(__file__).resolve().parent.parent
EXPECTED_PUBS = 35_916
EXPECTED_SII = 346
EXPECTED_SII_PREDIOS = 8_560_728
FUENTE = 'MINVU_DT10_2026'


def normalize(s):
    s = unicodedata.normalize('NFKD', str(s or '').casefold())
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return re.sub(r'[^a-z0-9]+', '', s)


def postgresql():
    import psycopg
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    vars_ = ('POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD')
    missing = [v for v in vars_ if not os.getenv(v)]
    if missing:
        raise RuntimeError('Faltan variables en .env: ' + ', '.join(missing))
    return psycopg.connect(host='127.0.0.1', port=5432,
                           dbname=os.environ['POSTGRES_DB'],
                           user=os.environ['POSTGRES_USER'],
                           password=os.environ['POSTGRES_PASSWORD'])


def validar_delta(conjuntos):
    from trino.dbapi import connect
    con = connect(host='127.0.0.1', port=8088, user='inmobiliario',
                  catalog='delta', schema='inmobiliario', http_scheme='http')
    try:
        cur = con.cursor()
        for t in TABLES:
            validate_delta(cur, t, conjuntos[t[0]])
    finally:
        con.close()


def validar_dw_actual(cur):
    cur.execute('SELECT COUNT(*) FROM dw.fact_publicaciones')
    pubs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*), COALESCE(SUM(predios_total), 0) "
                "FROM dw.fact_sii_destinos WHERE periodo='2026-S1' AND fuente='SII'")
    sii = cur.fetchone()
    if pubs != EXPECTED_PUBS or tuple(sii) != (EXPECTED_SII, EXPECTED_SII_PREDIOS):
        raise RuntimeError(f'Estado previo del DW diferente: publicaciones={pubs}; SII={sii}')
    cur.execute('''SELECT codigo_region, MIN(region), COUNT(DISTINCT region), COUNT(*)
                   FROM dw.dim_ubicacion GROUP BY codigo_region''')
    rows = cur.fetchall()
    by_code = {}
    for code, name, names_count, count in rows:
        code = str(code or '')
        if not re.fullmatch(r'\d{2}', code):
            raise RuntimeError(f'Código de región no validado en dw.dim_ubicacion: {code!r}')
        if names_count != 1:
            raise RuntimeError(f'Inconsistencia de nombres para la región {code}')
        by_code[code] = name
    expected_codes = {c for c in REGIONES.values() if c is not None}
    if set(by_code) != expected_codes or len(by_code) != 16:
        raise RuntimeError(f'El DW debe contener las 16 regiones: recibidas={by_code}')
    for name_minvu, code in REGIONES.items():
        if code is None:
            continue
        if normalize(by_code[code]) != normalize(name_minvu):
            raise RuntimeError(f'Región INE/SII y MINVU no coinciden: {code}: '
                               f'{by_code[code]} vs {name_minvu}')
    cur.execute('''SELECT COUNT(*), COUNT(DISTINCT codigo_comuna)
                   FROM dw.dim_ubicacion''')
    locations = cur.fetchone()
    if tuple(locations) != (346, 346):
        raise RuntimeError(f'dw.dim_ubicacion difiere de 346 comunas: {locations}')
    print(f'PostgreSQL validado: 16 regiones / 346 comunas; '
          f'{pubs:,} publicaciones; {sii[0]} hechos SII.'.replace(',', '.'))
    return by_code


def table_name(t):
    return f'fact_minvu_2026_t{t[0]}_{t[2]}'


def create_schema(cur, t):
    measures = t[4]
    cols = ',\n        '.join(
        f'{c} ' + ('BIGINT' if is_count(c) else 'NUMERIC(20,4)') +
        f' NOT NULL CHECK ({c} >= 0)' for c in measures
    )
    cur.execute(f'''CREATE TABLE IF NOT EXISTS dw.{table_name(t)} (
        region_key TEXT NOT NULL,
        codigo_region VARCHAR(2) REFERENCES dw.dim_region(codigo_region),
        region TEXT NOT NULL,
        nivel_geografico TEXT NOT NULL CHECK (nivel_geografico IN ('regional','nacional')),
        fuente TEXT NOT NULL,
        periodo_referencia TEXT NOT NULL,
        pagina_pdf SMALLINT NOT NULL,
        {cols},
        PRIMARY KEY (region_key, periodo_referencia, fuente),
        CONSTRAINT {"minvu_t" + str(t[0]) + "_nivel_consistente"} CHECK (
            (region_key = 'NACIONAL' AND codigo_region IS NULL AND nivel_geografico = 'nacional')
            OR (region_key = codigo_region AND nivel_geografico = 'regional')
        )
    )''')
    cur.execute('''SELECT column_name FROM information_schema.columns
                   WHERE table_schema='dw' AND table_name=%s''', (table_name(t),))
    found = {r[0] for r in cur.fetchall()}
    mandatory = {'region_key', 'codigo_region', 'region', 'nivel_geografico',
                 'fuente', 'periodo_referencia', 'pagina_pdf'} | set(measures)
    if found != mandatory:
        raise RuntimeError(f'dw.{table_name(t)} tiene columnas inesperadas: {found ^ mandatory}')


def desired_rows(t, conjunto):
    number, page, _, period, measures, _ = t
    result = []
    for region, code in REGIONES.items():
        result.append((code or 'NACIONAL', code, region,
                       'regional' if code else 'nacional', FUENTE, period, page,
                       *[int(conjunto[region][m]) if is_count(m)
                         else conjunto[region][m] for m in measures]))
    return result


def upsert_and_verify(cur, t, conjunto):
    cols = ('region_key', 'codigo_region', 'region', 'nivel_geografico',
            'fuente', 'periodo_referencia', 'pagina_pdf') + t[4]
    desired = desired_rows(t, conjunto)
    placeholders = ', '.join(['%s'] * len(cols))
    col_sql = ', '.join(cols)
    cur.executemany(f'''INSERT INTO dw.{table_name(t)} ({col_sql})
          VALUES ({placeholders})
          ON CONFLICT (region_key, periodo_referencia, fuente) DO NOTHING''', desired)
    cur.execute(f'''SELECT {col_sql} FROM dw.{table_name(t)}''')
    rows = cur.fetchall()
    if len(rows) != 17:
        raise RuntimeError(f'dw.{table_name(t)}: se esperaban 17 filas, hay {len(rows)}')
    expected = {r[0]: r for r in desired}
    for row in rows:
        key = row[0]
        if key not in expected:
            raise RuntimeError(f'dw.{table_name(t)} contiene región extra: {key}')
        goal = expected[key]
        # Normalizar Decimal y bigint para comparar cifras de forma exacta.
        if tuple(row[:7]) != goal[:7]:
            raise RuntimeError(f'Metadatos distintos para {table_name(t)} / {key}')
        for column, actual, value in zip(t[4], row[7:], goal[7:]):
            if Decimal(str(actual)) != Decimal(str(value)):
                raise RuntimeError(f'Diferencia en {table_name(t)} / {key} / {column}: '
                                   f'{actual} != {value}. Se revierte la transacción.')
    print(f'OK DW: dw.{table_name(t)} (16 regiones + Nacional; {len(t[4])} indicadores).')


def auditar_pg():
    with postgresql() as conn:
        with conn.cursor() as cur:
            validar_dw_actual(cur)
            for t in TABLES:
                cur.execute('SELECT to_regclass(%s)', (f'dw.{table_name(t)}',))
                if cur.fetchone()[0] is not None:
                    cur.execute(f'SELECT COUNT(*) FROM dw.{table_name(t)}')
                    print(f'dw.{table_name(t)} ya existe: {cur.fetchone()[0]} filas.')
            cur.execute("SELECT to_regclass('dw.dim_region')")
            if cur.fetchone()[0] is not None:
                cur.execute('SELECT COUNT(*) FROM dw.dim_region')
                print(f'dw.dim_region ya existe: {cur.fetchone()[0]} filas.')


def cargar(conjuntos):
    with postgresql() as con:
        with con.cursor() as cur:
            # Bloquea cambios concurrentes de la dimensión de comunas
            # mientras se comprueba la correspondencia regional.
            cur.execute('LOCK TABLE dw.dim_ubicacion IN SHARE MODE')
            region_names = validar_dw_actual(cur)
            cur.execute('''CREATE TABLE IF NOT EXISTS dw.dim_region (
                codigo_region VARCHAR(2) PRIMARY KEY,
                region TEXT NOT NULL UNIQUE
            )''')
            cur.executemany('''INSERT INTO dw.dim_region (codigo_region, region)
                                VALUES (%s, %s)
                                ON CONFLICT (codigo_region) DO NOTHING''',
                            sorted(region_names.items()))
            cur.execute('SELECT codigo_region, region FROM dw.dim_region')
            actual = dict(cur.fetchall())
            if actual != region_names:
                raise RuntimeError('dw.dim_region contiene valores incompatibles; se revierte todo')
            for t in TABLES:
                create_schema(cur, t)
                upsert_and_verify(cur, t, conjuntos[t[0]])
            # Confirma que la inserción MINVU no alteró nuestros hechos previos.
            validar_dw_actual(cur)
        # commit de las 5 tablas y la dimensión; si hubo error, rollback automático.
    print('INTEGRACIÓN MINVU 2026 A POSTGRESQL COMPLETA: 5 hechos (17 filas c/u) + dim_region.')
    print('Aviso: Nacional es un agregado. Para sumar regiones filtrar nivel_geografico = regional.')


def main():
    p = argparse.ArgumentParser(description='Integra MINVU 2026 desde Delta Lake hacia PostgreSQL DW')
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--auditar', action='store_true', help='solo lectura, valida 5 Delta y DW')
    group.add_argument('--cargar', action='store_true', help='escribe 5 hechos y dim_region en DW')
    args = p.parse_args()
    print('=== MINVU 2026: DELTA LAKE → DATA WAREHOUSE ===')
    _, conjuntos = auditar()  # validación local CSV; no escrituras
    validar_delta(conjuntos)   # verifica Delta fila a fila
    auditar_pg()
    if args.auditar:
        print('AUDITORÍA COMPLETA SIN ESCRITURAS. Para integrar: --cargar')
    else:
        cargar(conjuntos)


if __name__ == '__main__':
    main()
