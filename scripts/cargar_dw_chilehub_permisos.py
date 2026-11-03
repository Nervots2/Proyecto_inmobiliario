"""ChileHub: carga idempotente Delta Lake -> Data Warehouse PostgreSQL.

Desde la raíz de Proyecto_inmobiliario (entorno inmobiliario, Docker activo):
    python scripts/cargar_dw_chilehub_permisos.py --auditar
    python scripts/cargar_dw_chilehub_permisos.py --cargar

Granularidad: una fila por año y comuna (CUT del INE).
Solo --cargar escribe en PostgreSQL, en una única transacción.
El script no modifica las tablas previas, ni Silo/MinIO ni Delta Lake.
"""
from __future__ import annotations

import argparse
from datetime import date
import os
from pathlib import Path
import unicodedata

from integrar_chilehub_permisos_delta import (
    audit as auditar_parquet,
    DELTA_TABLE,
    fetch_compare,
    trino_connect,
)

ROOT = Path(__file__).resolve().parents[1]
TABLE = 'dw.fact_chilehub_permisos_edificacion'
EXPECTED_ROWS = 8_650
EXPECTED_COMMUNES = 346
EXPECTED_YEARS = 25
MEASURES = (
    'unidades_total', 'superficie_m2_total',
    'unidades_casas', 'superficie_m2_casas',
    'unidades_departamentos', 'superficie_m2_departamentos',
)
COLUMNS = (
    'anio', 'id_ubicacion', *MEASURES,
    'estado_dato', 'nombre_comuna_fuente', 'fuente', 'url_fuente', 'fecha_fuente',
)


def connect_pg():
    import psycopg
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    keys = ('POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD')
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise RuntimeError('Faltan variables en .env: ' + ', '.join(missing))
    return psycopg.connect(
        host='127.0.0.1', port=5432, dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'],
        connect_timeout=10,
    )


def normalizar(texto):
    s = unicodedata.normalize('NFKD', str(texto or '').casefold())
    return ' '.join(''.join(c for c in s if not unicodedata.combining(c)).split())


def validar_delta(df):
    conn = trino_connect()
    try:
        cur = conn.cursor()
        fetch_compare(cur, DELTA_TABLE, df)
    finally:
        conn.close()


def revisar_anteriores(cur):
    """Validaciones de solo lectura de tablas preexistentes."""
    counts = {}
    for name in ('fact_publicaciones', 'fact_sii_destinos',
                 'fact_banco_central_vivienda_mensual'):
        cur.execute('SELECT to_regclass(%s)', (f'dw.{name}',))
        if cur.fetchone()[0] is None:
            raise RuntimeError(f'Falta dw.{name}; revisar el Data Warehouse')
        cur.execute(f'SELECT COUNT(*) FROM dw.{name}')
        counts[name] = cur.fetchone()[0]

    cur.execute("""SELECT table_name FROM information_schema.tables
                   WHERE table_schema='dw' AND table_name LIKE 'fact_minvu_2026_%'
                   ORDER BY table_name""")
    minvu = [r[0] for r in cur.fetchall()]
    if len(minvu) != 5:
        raise RuntimeError(f'Se esperaban 5 tablas MINVU; encontradas {len(minvu)}')
    for name in minvu:
        cur.execute(f'SELECT COUNT(*) FROM dw."{name}"')
        counts[name] = cur.fetchone()[0]
    if counts['fact_publicaciones'] != 35_916:
        raise RuntimeError('Cantidad de publicaciones distinta de 35.916')
    if counts['fact_sii_destinos'] != 346:
        raise RuntimeError('Cantidad de hechos SII distinta de 346')
    if counts['fact_banco_central_vivienda_mensual'] != 297:
        raise RuntimeError('Cantidad de meses Banco Central distinta de 297')
    if any(counts[name] != 17 for name in minvu):
        raise RuntimeError('Una tabla MINVU no tiene 17 registros')
    return counts


def mapear_ubicaciones(cur, df):
    cur.execute("""SELECT id_ubicacion, codigo_region, codigo_comuna, comuna
                   FROM dw.dim_ubicacion""")
    rows = cur.fetchall()
    if len(rows) != EXPECTED_COMMUNES:
        raise RuntimeError(f'Dimensión con {len(rows)} comunas; esperadas 346')
    by_cut = {}
    for id_ubicacion, region, comuna, nombre in rows:
        if region is None or comuna is None:
            raise RuntimeError('Existen códigos INE nulos en dw.dim_ubicacion')
        region, comuna = str(region), str(comuna)
        if len(region) != 2 or len(comuna) != 5 or comuna[:2] != region:
            raise RuntimeError(f'Código geográfico inconsistente en dimensión: {region}/{comuna}')
        if comuna in by_cut:
            raise RuntimeError(f'Código CUT duplicado en dimensión: {comuna}')
        by_cut[comuna] = (int(id_ubicacion), region, str(nombre))

    source_codes = set(df['codigo_comuna'])
    missing = sorted(source_codes - set(by_cut))
    extra = sorted(set(by_cut) - source_codes)
    if missing or extra:
        raise RuntimeError(f'Códigos CUT no emparejados. Faltantes={missing[:12]}, extras={extra[:12]}')

    name_diffs = set()
    desired = []
    for row in df.itertuples(index=False):
        loc_id, region_dw, nombre_dw = by_cut[row.codigo_comuna]
        if str(row.codigo_region) != region_dw:
            raise RuntimeError(f'Región diferente para CUT {row.codigo_comuna}')
        if normalizar(row.nombre_comuna) != normalizar(nombre_dw):
            name_diffs.add((row.codigo_comuna, str(row.nombre_comuna), nombre_dw))
        fecha = date.fromisoformat(str(row.fecha_fuente)[:10])
        desired.append((
            int(row.anio), loc_id,
            int(row.unidades_total), int(row.superficie_m2_total),
            int(row.unidades_casas), int(row.superficie_m2_casas),
            int(row.unidades_departamentos), int(row.superficie_m2_departamentos),
            str(row.estado_dato), str(row.nombre_comuna),
            str(row.fuente), str(row.url_fuente), fecha,
        ))
    desired.sort(key=lambda r: (r[0], r[1]))
    if len(desired) != EXPECTED_ROWS or len({(r[0], r[1]) for r in desired}) != EXPECTED_ROWS:
        raise RuntimeError('Registros o claves (año, ubicación) inesperados')
    if len({r[1] for r in desired}) != EXPECTED_COMMUNES:
        raise RuntimeError('Faltan comunas en el mapeo')
    if len({r[0] for r in desired}) != EXPECTED_YEARS:
        raise RuntimeError('Faltan años en el mapeo')
    print(f'CUT conciliados: {EXPECTED_COMMUNES}/{EXPECTED_COMMUNES}; registros listos: {len(desired)}')
    print(f'Nombres que difieren entre ChileHub y el SII/INE: {len(name_diffs)} CUT')
    if name_diffs:
        for cut, nombre_fuente, nombre_dw in sorted(name_diffs)[:10]:
            print(f'  {cut}: ChileHub={nombre_fuente!r}; DW={nombre_dw!r}')
        print('Los nombres originales se conservan; la relación utiliza CUT oficial.')
    return desired


def tabla_existente(cur):
    cur.execute('SELECT to_regclass(%s)', (TABLE,))
    return cur.fetchone()[0] is not None


def verificar_fact(cur, desired):
    if not tabla_existente(cur):
        print('La tabla del DW ChileHub aún no existe.')
        return False
    cur.execute(f'SELECT {", ".join(COLUMNS)} FROM {TABLE} ORDER BY anio, id_ubicacion')
    actual = cur.fetchall()
    if len(actual) != len(desired):
        raise RuntimeError(f'{TABLE}: {len(actual)} filas; se esperaban {len(desired)}')
    for index, (got, exp) in enumerate(zip(actual, desired), 1):
        if tuple(got) != exp:
            raise RuntimeError(f'Diferencia en tabla ChileHub, fila {index}, clave {exp[:2]}; no se sobrescribe')
    print(f'DW validado: {len(actual)} hechos, sin diferencias ni duplicados')
    return True


def crear_fact(cur):
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        anio INTEGER NOT NULL CHECK (anio BETWEEN 2002 AND 2026),
        id_ubicacion BIGINT NOT NULL REFERENCES dw.dim_ubicacion(id_ubicacion),
        unidades_total BIGINT NOT NULL CHECK (unidades_total >= 0),
        superficie_m2_total BIGINT NOT NULL CHECK (superficie_m2_total >= 0),
        unidades_casas BIGINT NOT NULL CHECK (unidades_casas >= 0),
        superficie_m2_casas BIGINT NOT NULL CHECK (superficie_m2_casas >= 0),
        unidades_departamentos BIGINT NOT NULL CHECK (unidades_departamentos >= 0),
        superficie_m2_departamentos BIGINT NOT NULL CHECK (superficie_m2_departamentos >= 0),
        estado_dato TEXT NOT NULL CHECK (estado_dato IN ('definitivo','provisional')),
        nombre_comuna_fuente TEXT NOT NULL,
        fuente TEXT NOT NULL,
        url_fuente TEXT NOT NULL,
        fecha_fuente DATE NOT NULL,
        PRIMARY KEY (anio, id_ubicacion),
        CONSTRAINT chilehub_unidades_suma CHECK
          (unidades_total = unidades_casas + unidades_departamentos),
        CONSTRAINT chilehub_superficie_suma CHECK
          (superficie_m2_total = superficie_m2_casas + superficie_m2_departamentos),
        CONSTRAINT chilehub_estado_anio CHECK
          ((anio <= 2023 AND estado_dato = 'definitivo') OR
           (anio >= 2024 AND estado_dato = 'provisional'))
    )""")


def cargar(cur, desired):
    crear_fact(cur)
    # COPY temporal permite cargas eficientes y evita modificaciones parciales.
    cur.execute(f'CREATE TEMP TABLE chilehub_etapa (LIKE {TABLE}) ON COMMIT DROP')
    copy_sql = 'COPY chilehub_etapa (' + ', '.join(COLUMNS) + ') FROM STDIN'
    with cur.copy(copy_sql) as copy:
        for row in desired:
            copy.write_row(row)
    insert_sql = (f'INSERT INTO {TABLE} ({", ".join(COLUMNS)}) '
                  f'SELECT {", ".join(COLUMNS)} FROM chilehub_etapa WHERE TRUE '
                  'ON CONFLICT (anio, id_ubicacion) DO NOTHING')
    cur.execute(insert_sql)
    inserted = cur.rowcount
    verificar_fact(cur, desired)
    print(f'Filas nuevas insertadas: {inserted}; total ChileHub: {len(desired)}')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument('--auditar', action='store_true', help='Solo lectura; no modifica datos')
    mode.add_argument('--cargar', action='store_true', help='Carga transaccional idempotente')
    args = ap.parse_args()

    print('=== CHILEHUB: DELTA LAKE -> DATA WAREHOUSE ===')
    df = auditar_parquet()
    validar_delta(df)
    with connect_pg() as conn:
        with conn.cursor() as cur:
            before = revisar_anteriores(cur)
            desired = mapear_ubicaciones(cur, df)
            if args.auditar:
                verificar_fact(cur, desired)
                print('AUDITORÍA COMPLETA SIN ESCRITURAS. Para cargar: --cargar')
            else:
                cargar(cur, desired)
                after = revisar_anteriores(cur)
                if after != before:
                    raise RuntimeError('Cambió alguna tabla anterior; transacción revertida')
                print('CARGA CHILEHUB A POSTGRESQL COMPLETA. Tablas previas intactas.')


if __name__ == '__main__':
    main()
