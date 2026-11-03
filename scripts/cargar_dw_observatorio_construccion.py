"""Observatorio 2024: Delta Lake -> Data Warehouse PostgreSQL.

Desde la raíz de Proyecto_inmobiliario (Conda activo, Docker corriendo):
    python scripts/cargar_dw_observatorio_construccion.py --auditar
    python scripts/cargar_dw_observatorio_construccion.py --cargar

- Una fila por nivel/código/año/indicador: 346 comunas + 16 regiones + 1 país.
- Solo --cargar realiza escrituras, en una transacción con rollback ante errores.
- Guarda NULL para Río Verde (CUT 12103); no duplica agregados geográficos.
- Enlaza comunas a dw.dim_ubicacion y regiones a dw.dim_region.
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path
import os

from dotenv import load_dotenv
import psycopg
import pandas as pd

from integrar_observatorio_construccion_delta import (
    audit as audit_source, compare_all, trino_connect, DELTA_TABLE, COLUMNS,
)

ROOT = Path(__file__).resolve().parents[1]
DW_TABLE = 'dw.fact_observatorio_ocupados_construccion_2024'
N = 363
# Cuatro campos de granularidad y las claves foráneas; el resto preserva la fuente.
DW_COLS = (
    'nivel', 'codigo', 'periodo', 'indicador_id',
    'id_ubicacion', 'codigo_region', 'territorio', 'region',
    'indicador', 'unidad', 'valor', 'numerador', 'denominador', 'nota',
    'estado', 'fuente', 'licencia', 'limite_inferior', 'limite_superior',
    'dudoso', 'licencia_nombre', 'territorio_distinto',
)
KEY = ('nivel', 'codigo', 'periodo', 'indicador_id')
FLOATS = {'valor', 'numerador', 'denominador', 'limite_inferior', 'limite_superior'}


def pg_connect():
    load_dotenv(ROOT / '.env')
    required = ('POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD')
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError('Variables ausentes en .env: ' + ', '.join(missing))
    return psycopg.connect(
        host='127.0.0.1', port=5432, dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'],
        connect_timeout=10,
    )


def verify_delta(df):
    conn = trino_connect()
    try:
        compare_all(conn.cursor(), DELTA_TABLE, df)
    finally:
        conn.close()


def baseline(cur):
    required = {
        'fact_publicaciones': 35916,
        'fact_sii_destinos': 346,
        'fact_banco_central_vivienda_mensual': 297,
        'fact_chilehub_permisos_edificacion': 8650,
    }
    cur.execute("""SELECT table_name FROM information_schema.tables
                   WHERE table_schema='dw' AND table_name LIKE 'fact_minvu_2026_%'
                   ORDER BY table_name""")
    minvu = [x[0] for x in cur.fetchall()]
    if len(minvu) != 5:
        raise RuntimeError(f'Se requieren 5 tablas MINVU; aparecen {len(minvu)}')
    required.update({name: 17 for name in minvu})
    found = {}
    for name, expected in required.items():
        cur.execute('SELECT to_regclass(%s)', (f'dw.{name}',))
        if cur.fetchone()[0] is None:
            raise RuntimeError(f'Falta la tabla dw.{name}')
        cur.execute(f'SELECT COUNT(*) FROM dw."{name}"')
        found[name] = cur.fetchone()[0]
        if found[name] != expected:
            raise RuntimeError(f'dw.{name}: {found[name]} filas, esperadas {expected}')
    return found


def maps(cur):
    cur.execute('SELECT id_ubicacion, codigo_region, codigo_comuna FROM dw.dim_ubicacion')
    locations = {}
    for id_loc, region, cut in cur.fetchall():
        cut, region = str(cut), str(region)
        if len(cut) != 5 or cut[:2] != region or cut in locations:
            raise RuntimeError(f'CUT/región inconsistentes en dw.dim_ubicacion: {cut}/{region}')
        locations[cut] = (int(id_loc), region)
    cur.execute('SELECT codigo_region FROM dw.dim_region')
    regions = [str(x[0]) for x in cur.fetchall()]
    if len(locations) != 346 or len(regions) != 16 or len(set(regions)) != 16:
        raise RuntimeError('dim_ubicacion debe contener 346 CUT y dim_region 16 códigos')
    if set(regions) != {r for _, r in locations.values()}:
        raise RuntimeError('Las regiones en ambas dimensiones no coinciden')
    return locations, set(regions)


def scalar(v):
    if v is None or pd.isna(v):
        return None
    if hasattr(v, 'item') and not isinstance(v, str):
        return v.item()
    return v


def expected_rows(df, locations, regions):
    rows = []
    for source in df.itertuples(index=False, name=None):
        r = {c: scalar(v) for c, v in zip(COLUMNS, source)}
        level, code = str(r['nivel']), str(r['codigo'])
        if level == 'comuna':
            if code not in locations:
                raise RuntimeError(f'CUT Observatorio no está en INE/SII: {code}')
            id_loc, region = locations[code]
        elif level == 'region':
            if code not in regions:
                raise RuntimeError(f'Región Observatorio no está en DW: {code}')
            id_loc, region = None, code
        elif level == 'pais' and code == 'CL':
            id_loc, region = None, None
        else:
            raise RuntimeError(f'Nivel/código inesperado: {level}/{code}')
        row = (
            level, code, int(r['periodo']), str(r['indicador_id']),
            id_loc, region, r['territorio'], r['region'],
            r['indicador'], r['unidad'], r['valor'], r['numerador'],
            r['denominador'], r['nota'], r['estado'], r['fuente'],
            r['licencia'], r['limite_inferior'], r['limite_superior'],
            r['dudoso'], r['licencia_nombre'], r['territorio_distinto'],
        )
        rows.append(row)
    rows.sort(key=lambda r: tuple(r[DW_COLS.index(c)] for c in KEY))
    if len(rows) != N or len({tuple(r[DW_COLS.index(c)] for c in KEY) for r in rows}) != N:
        raise RuntimeError('Registros o claves duplicadas en Observatorio')
    null_rows = [r for r in rows if r[DW_COLS.index('valor')] is None]
    if len(null_rows) != 1 or null_rows[0][1] != '12103':
        raise RuntimeError('Se espera un solo valor NULL: Río Verde, CUT 12103')
    print('Geografía conciliada: 346 comunas / 16 regiones / 1 país')
    print('Valores NULL: 1 (Río Verde, CUT 12103), sin imputación')
    return rows


def table_exists(cur):
    cur.execute('SELECT to_regclass(%s)', (DW_TABLE,))
    return cur.fetchone()[0] is not None


def equal(a, b, col):
    if a is None or b is None:
        return a is None and b is None
    if col in FLOATS:
        return math.isclose(float(a), float(b), rel_tol=1e-11, abs_tol=1e-9)
    return a == b


def verify_dw(cur, desired):
    if not table_exists(cur):
        print(f'Tabla {DW_TABLE} todavía no existe.')
        return False
    col_sql = ', '.join(DW_COLS)
    order_sql = ', '.join(KEY)
    cur.execute(f'SELECT {col_sql} FROM {DW_TABLE} ORDER BY {order_sql}')
    actual = cur.fetchall()
    if len(actual) != len(desired):
        raise RuntimeError(f'DW contiene {len(actual)} filas; se esperaban {len(desired)}')
    for idx, (got, want) in enumerate(zip(actual, desired), 1):
        for col, a, b in zip(DW_COLS, got, want):
            if not equal(a, b, col):
                raise RuntimeError(f'Diferencia DW en fila {idx}, {col}: {a!r} != {b!r}; no se sobrescribe')
    print(f'DW validado: {len(actual)} filas, sin diferencias y con un valor NULL')
    return True


def create_table(cur):
    cur.execute(f'''CREATE TABLE IF NOT EXISTS {DW_TABLE} (
        nivel VARCHAR(6) NOT NULL CHECK (nivel IN ('comuna','region','pais')),
        codigo VARCHAR(5) NOT NULL,
        periodo INTEGER NOT NULL CHECK (periodo = 2024),
        indicador_id TEXT NOT NULL,
        id_ubicacion BIGINT REFERENCES dw.dim_ubicacion(id_ubicacion),
        codigo_region VARCHAR(2) REFERENCES dw.dim_region(codigo_region),
        territorio TEXT NOT NULL,
        region TEXT,
        indicador TEXT NOT NULL,
        unidad TEXT NOT NULL CHECK (unidad = '%'),
        valor DOUBLE PRECISION CHECK (valor BETWEEN 0 AND 100),
        numerador DOUBLE PRECISION CHECK (numerador >= 0),
        denominador DOUBLE PRECISION CHECK (denominador > 0),
        nota TEXT,
        estado TEXT NOT NULL CHECK (estado IN ('observado', 'sin_valor')),
        fuente TEXT NOT NULL,
        licencia TEXT NOT NULL,
        limite_inferior DOUBLE PRECISION,
        limite_superior DOUBLE PRECISION,
        dudoso BOOLEAN NOT NULL,
        licencia_nombre TEXT NOT NULL,
        territorio_distinto BOOLEAN NOT NULL,
        PRIMARY KEY (nivel, codigo, periodo, indicador_id),
        CONSTRAINT observatorio_geografia_valida CHECK (
            (nivel = 'comuna' AND id_ubicacion IS NOT NULL AND codigo_region IS NOT NULL
             AND codigo_region = LEFT(codigo, 2)) OR
            (nivel = 'region' AND id_ubicacion IS NULL AND codigo_region = codigo) OR
            (nivel = 'pais' AND codigo = 'CL' AND id_ubicacion IS NULL AND codigo_region IS NULL)
        ),
        CONSTRAINT observatorio_valor_consistente CHECK (
            (estado = 'sin_valor' AND valor IS NULL AND numerador IS NULL AND denominador IS NULL)
            OR (estado = 'observado' AND valor IS NOT NULL AND numerador IS NOT NULL AND denominador IS NOT NULL)
        )
    )''')


def load_rows(cur, rows):
    create_table(cur)
    placeholders = ', '.join(['%s'] * len(DW_COLS))
    columns = ', '.join(DW_COLS)
    cur.executemany(
        f'INSERT INTO {DW_TABLE} ({columns}) VALUES ({placeholders}) '
        'ON CONFLICT (nivel, codigo, periodo, indicador_id) DO NOTHING',
        rows,
    )
    verify_dw(cur, rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    modes = ap.add_mutually_exclusive_group(required=True)
    modes.add_argument('--auditar', action='store_true', help='Solo lectura')
    modes.add_argument('--cargar', action='store_true', help='Carga transaccional sin sobrescrituras')
    opts = ap.parse_args()

    print('=== OBSERVATORIO: DELTA LAKE -> POSTGRESQL ===')
    df = audit_source()
    verify_delta(df)
    with pg_connect() as pg:
        with pg.cursor() as cur:
            before = baseline(cur)
            locations, regions = maps(cur)
            rows = expected_rows(df, locations, regions)
            if opts.auditar:
                verify_dw(cur, rows)
                print('AUDITORÍA COMPLETA SIN ESCRITURAS. Para integrar: --cargar')
            else:
                load_rows(cur, rows)
                after = baseline(cur)
                if before != after:
                    raise RuntimeError('Se detectó cambio en tablas previas; transacción revertida')
                print('INTEGRACIÓN OBSERVATORIO A POSTGRESQL COMPLETA. Tablas previas intactas.')


if __name__ == '__main__':
    main()
