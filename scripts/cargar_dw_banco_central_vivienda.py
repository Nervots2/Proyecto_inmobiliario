"""Banco Central: carga idempotente desde Delta Lake a PostgreSQL.

Desde la raiz de Proyecto_inmobiliario, con Docker iniciado:
    python scripts/cargar_dw_banco_central_vivienda.py --auditar
    python scripts/cargar_dw_banco_central_vivienda.py --cargar

Requiere scripts/integrar_banco_central_vivienda_delta.py y su CSV/metadatos
validados. La auditoria no escribe. La carga no sobrescribe datos distintos:
ante cualquier discrepancia hace rollback.
"""
from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal
import os
from pathlib import Path

from dotenv import load_dotenv
from trino.dbapi import connect as trino_connect
import psycopg

from integrar_banco_central_vivienda_delta import audit, TABLE as DELTA_TABLE

ROOT = Path(__file__).resolve().parents[1]
PG_TABLE = 'dw.fact_banco_central_vivienda_mensual'
SOURCE = 'Banco Central de Chile'
FIELDS = (
    'periodo', 'tasa_interes_vivienda_anual_pct',
    'flujo_vivienda_valor_original', 'fuente',
    'serie_tasa', 'serie_flujo', 'frecuencia',
)


def pg_connect():
    load_dotenv(ROOT / '.env')
    required = ('POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB')
    absent = [key for key in required if not os.getenv(key)]
    if absent:
        raise RuntimeError('Faltan variables en .env: ' + ', '.join(absent))
    return psycopg.connect(
        host='127.0.0.1', port=5432,
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        connect_timeout=10,
    )


def delta_source(expected):
    conn = trino_connect(host='127.0.0.1', port=8088,
                         user='inmobiliario', catalog='delta',
                         schema='inmobiliario', http_scheme='http')
    try:
        cur = conn.cursor()
        cur.execute(
            'SELECT periodo, tasa_interes_vivienda_anual_pct, '
            'flujo_vivienda_valor_original '
            f'FROM delta.inmobiliario.{DELTA_TABLE} ORDER BY periodo'
        )
        received = cur.fetchall()
    finally:
        conn.close()
    if len(received) != len(expected):
        raise RuntimeError(f'Delta contiene {len(received)} filas; CSV contiene {len(expected)}')
    normalized = []
    for index, ((period, rate, flow), (exp_d, exp_r, exp_f)) in enumerate(zip(received, expected), 1):
        if isinstance(period, str):
            period = date.fromisoformat(period)
        rate = Decimal(str(rate))
        flow = None if flow is None else Decimal(str(flow))
        if (period, rate, flow) != (exp_d, exp_r, exp_f):
            raise RuntimeError(f'Fila {index}: Delta y CSV no coinciden ({period})')
        normalized.append((period, rate, flow))
    print(f'Delta verificado: {len(normalized)} meses, '
          f'{sum(r[2] is not None for r in normalized)} flujos disponibles')
    return normalized


def baseline(cur):
    names = ('fact_publicaciones', 'fact_sii_destinos')
    cur.execute('''SELECT table_name FROM information_schema.tables
                   WHERE table_schema='dw' AND table_name LIKE 'fact_minvu_2026_%'
                   ORDER BY table_name''')
    names += tuple(row[0] for row in cur.fetchall())
    if len(names) != 7:  # dos tablas previas y cinco tablas MINVU
        raise RuntimeError(f'Se esperaban 5 tablas MINVU en dw, se encontraron {len(names)-2}')
    counts = {}
    for name in names:
        # Los nombres vienen de constantes y de information_schema, no del usuario.
        cur.execute(f'SELECT COUNT(*) FROM dw."{name}"')
        counts[name] = cur.fetchone()[0]
    if counts['fact_publicaciones'] != 35916 or counts['fact_sii_destinos'] != 346:
        raise RuntimeError(f'Conteos anteriores del DW inesperados: {counts}')
    if any(counts[n] != 17 for n in names if n.startswith('fact_minvu_')):
        raise RuntimeError(f'Conteos MINVU inesperados: {counts}')
    return counts


def expected_rows(delta_rows, metadata):
    series = metadata['series_del_excel']
    if len(series) < 2:
        raise RuntimeError('Metadatos de las series incompletos')
    tasa_code, flujo_code = series[0]['codigo'], series[1]['codigo']
    if not tasa_code or not flujo_code:
        raise RuntimeError('Los códigos de serie no pueden estar vacíos')
    return [
        (day, rate, flow, SOURCE, tasa_code, flujo_code, 'mensual')
        for day, rate, flow in delta_rows
    ]


def existing_table(cur):
    cur.execute('SELECT to_regclass(%s)', (PG_TABLE,))
    return cur.fetchone()[0] is not None


def verify_existing(cur, desired):
    cur.execute(f'SELECT {", ".join(FIELDS)} FROM {PG_TABLE} ORDER BY periodo')
    found = cur.fetchall()
    if len(found) != len(desired):
        raise RuntimeError(f'{PG_TABLE}: {len(found)} filas, se esperaban {len(desired)}')
    for index, (actual, exp) in enumerate(zip(found, desired), 1):
        # PostgreSQL devuelve Decimal; la comparacion es numericamente exacta.
        if tuple(actual) != tuple(exp):
            raise RuntimeError(f'Diferencia en DW, fila {index}, periodo {exp[0]}; '
                               'no se sobrescribieron datos')
    print(f'DW verificado: {len(found)} meses; '
          f'{sum(r[2] is None for r in found)} flujos NULL')


def load_pg(desired):
    with pg_connect() as conn:
        with conn.cursor() as cur:
            before = baseline(cur)
            cur.execute(f'''CREATE TABLE IF NOT EXISTS {PG_TABLE} (
                periodo DATE PRIMARY KEY,
                tasa_interes_vivienda_anual_pct NUMERIC(38,20) NOT NULL
                    CHECK (tasa_interes_vivienda_anual_pct >= 0),
                flujo_vivienda_valor_original NUMERIC(38,20)
                    CHECK (flujo_vivienda_valor_original >= 0),
                fuente TEXT NOT NULL,
                serie_tasa TEXT NOT NULL,
                serie_flujo TEXT NOT NULL,
                frecuencia TEXT NOT NULL CHECK (frecuencia = 'mensual')
            )''')
            sql = (f'INSERT INTO {PG_TABLE} ({", ".join(FIELDS)}) '
                   'VALUES (%s, %s, %s, %s, %s, %s, %s) '
                   'ON CONFLICT (periodo) DO NOTHING')
            cur.executemany(sql, desired)
            verify_existing(cur, desired)
            after = baseline(cur)
            if after != before:
                raise RuntimeError('Una tabla previa del DW cambió; se revierte la transacción')
    print('CARGA BANCO CENTRAL A POSTGRESQL COMPLETA; tablas anteriores intactas.')


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    mode = cli.add_mutually_exclusive_group(required=True)
    mode.add_argument('--auditar', action='store_true', help='No escribe nada')
    mode.add_argument('--cargar', action='store_true', help='Carga de forma idempotente en DW')
    args = cli.parse_args()
    print('=== BANCO CENTRAL: DELTA LAKE -> DATA WAREHOUSE ===')
    csv_rows, metadata = audit()
    delta_rows = delta_source(csv_rows)
    desired = expected_rows(delta_rows, metadata)
    if args.auditar:
        with pg_connect() as conn:
            with conn.cursor() as cur:
                counts = baseline(cur)
                print('DW previo validado:', counts['fact_publicaciones'],
                      'publicaciones,', counts['fact_sii_destinos'], 'hechos SII,',
                      '5 tablas MINVU')
                if existing_table(cur):
                    verify_existing(cur, desired)
                else:
                    print(f'Tabla {PG_TABLE} todavía no existe')
        print('AUDITORÍA COMPLETA SIN ESCRITURAS.')
    else:
        load_pg(desired)


if __name__ == '__main__':
    main()
