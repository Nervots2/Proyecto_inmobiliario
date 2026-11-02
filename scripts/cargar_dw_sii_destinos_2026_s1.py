"""Carga idempotente de estadísticas SII 2026-S1: Delta Lake (Trino) -> PostgreSQL DW.

Ejecutar desde la raíz del repositorio (con Docker activo):
  python scripts/cargar_dw_sii_destinos_2026_s1.py --auditar
  python scripts/cargar_dw_sii_destinos_2026_s1.py --cargar

Crea `dw.fact_sii_destinos` (granularidad comuna + semestre + fuente) y
puebla `dw.dim_ubicacion` sin inventar códigos INE. No modifica las
publicaciones ni el Delta Lake. Re-ejecutar con los mismos datos no duplica.
Si se detectan cifras diferentes para una clave ya cargada, aborta.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
TABLA_DELTA = 'delta.inmobiliario.sii_destinos_comuna_2026_s1'
TABLA_FACT = 'dw.fact_sii_destinos'
PERIODO = '2026-S1'
FUENTE = 'SII'
CATEGORIAS = [
    'predios_comercio', 'predios_deporte_y_recreacion',
    'predios_educacion_y_cultura', 'predios_hotel_o_motel',
    'predios_habitacional', 'predios_industria', 'predios_bienes_comunes',
    'predios_bodega_y_almacenaje', 'predios_mineria', 'predios_oficina',
    'predios_administracion_publica_y_defensa', 'predios_culto',
    'predios_salud', 'predios_transporte', 'predios_otros_no_considerados',
    'predios_sitio_eriazo', 'predios_estacionamiento',
]
MEDIDAS = CATEGORIAS + ['predios_total']
COLUMNAS = ['region', 'comuna', 'periodo', 'fuente'] + MEDIDAS
ESPERADO_FILAS = 346
ESPERADO_TOTAL = 8_560_728
ESPERADO_RM_FILAS = 52
ESPERADO_RM_TOTAL = 3_700_127


def datos_delta():
    from trino.dbapi import connect
    conn = connect(host='127.0.0.1', port=8088, user='inmobiliario',
                   catalog='delta', schema='inmobiliario', http_scheme='http')
    try:
        cur = conn.cursor()
        cur.execute('SELECT ' + ', '.join(COLUMNAS) + ' FROM ' + TABLA_DELTA)
        filas = cur.fetchall()
        return [dict(zip(COLUMNAS, fila)) for fila in filas]
    finally:
        conn.close()


def validar(filas):
    if len(filas) != ESPERADO_FILAS:
        raise ValueError(f'Delta tiene {len(filas)} comunas; se esperaban {ESPERADO_FILAS}')
    claves, suma, rm_filas, rm_suma = set(), 0, 0, 0
    for fila in filas:
        if fila['periodo'] != PERIODO or fila['fuente'] != FUENTE:
            raise ValueError(f'Período o fuente inesperados: {fila["comuna"]}')
        region, comuna = str(fila['region']).strip(), str(fila['comuna']).strip()
        if not region or not comuna:
            raise ValueError('Hay regiones o comunas vacías')
        clave = (region.casefold(), comuna.casefold())
        if clave in claves:
            raise ValueError(f'Región/comuna repetida: {region}/{comuna}')
        claves.add(clave)
        for medida in MEDIDAS:
            if type(fila[medida]) is not int or fila[medida] < 0:
                raise ValueError(f'Valor inválido: {comuna}/{medida}={fila[medida]}')
        if sum(fila[c] for c in CATEGORIAS) != fila['predios_total']:
            raise ValueError(f'Categorías no coinciden con total en {region}/{comuna}')
        suma += fila['predios_total']
        if region == 'Metropolitana':
            rm_filas += 1
            rm_suma += fila['predios_total']
    if (suma, rm_filas, rm_suma) != (ESPERADO_TOTAL, ESPERADO_RM_FILAS, ESPERADO_RM_TOTAL):
        raise ValueError(f'Totales inesperados: {(suma, rm_filas, rm_suma)}')
    print(f'Delta validado: {len(filas)} comunas; {suma:,} predios'.replace(',', '.'))
    print(f'RM: {rm_filas} comunas; {rm_suma:,} predios'.replace(',', '.'))


def conexion_postgres():
    import psycopg
    from dotenv import load_dotenv
    load_dotenv(RAIZ / '.env')
    variables = ('POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD')
    faltan = [v for v in variables if not os.getenv(v)]
    if faltan:
        raise RuntimeError('Faltan variables .env: ' + ', '.join(faltan))
    return psycopg.connect(host='127.0.0.1', port=5432,
                           dbname=os.environ['POSTGRES_DB'],
                           user=os.environ['POSTGRES_USER'],
                           password=os.environ['POSTGRES_PASSWORD'])


def verificar_esquema(cur):
    cur.execute("""SELECT column_name FROM information_schema.columns
        WHERE table_schema='dw' AND table_name='dim_ubicacion'""")
    columnas = {r[0] for r in cur.fetchall()}
    requerido = {'id_ubicacion', 'region', 'comuna', 'codigo_region', 'codigo_comuna'}
    if not requerido <= columnas:
        raise RuntimeError('dw.dim_ubicacion tiene esquema distinto. Columnas: ' + str(columnas))
    cur.execute("SELECT to_regclass('dw.fact_publicaciones')")
    if cur.fetchone()[0] is None:
        raise RuntimeError('No existe dw.fact_publicaciones; revisar DW primero')
    cur.execute("""SELECT lower(btrim(region)), lower(btrim(comuna)), COUNT(*)
        FROM dw.dim_ubicacion
        GROUP BY 1,2 HAVING COUNT(*) > 1 LIMIT 1""")
    repetida = cur.fetchone()
    if repetida:
        raise RuntimeError('Existen ubicaciones duplicadas en dimensión: ' + str(repetida))


def crear_fact(cur):
    columnas = ',\n        '.join(f'{c} BIGINT NOT NULL CHECK ({c} >= 0)' for c in MEDIDAS)
    suma = ' + '.join(CATEGORIAS)
    cur.execute(f'''CREATE TABLE IF NOT EXISTS {TABLA_FACT} (
        id_ubicacion BIGINT NOT NULL REFERENCES dw.dim_ubicacion(id_ubicacion),
        periodo TEXT NOT NULL,
        fuente TEXT NOT NULL,
        {columnas},
        PRIMARY KEY (id_ubicacion, periodo, fuente),
        CONSTRAINT sii_destinos_suma_correcta CHECK (predios_total = {suma})
    )''')
    cur.execute("""SELECT column_name FROM information_schema.columns
        WHERE table_schema='dw' AND table_name='fact_sii_destinos'""")
    existentes = {r[0] for r in cur.fetchall()}
    necesarios = {'id_ubicacion', 'periodo', 'fuente'} | set(MEDIDAS)
    if not necesarios <= existentes:
        raise RuntimeError('Tabla dw.fact_sii_destinos existente con columnas incompatibles')


def cargar(filas):
    with conexion_postgres() as conn:
        with conn.cursor() as cur:
            verificar_esquema(cur)
            cur.execute('SELECT COUNT(*) FROM dw.fact_publicaciones')
            hechos_antes = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM dw.dim_ubicacion')
            ubicaciones_antes = cur.fetchone()[0]
            # Se adquiere bloqueo para evitar altas simultáneas duplicadas en dimensión.
            cur.execute('LOCK TABLE dw.dim_ubicacion IN SHARE ROW EXCLUSIVE MODE')
            cur.execute('''CREATE TEMP TABLE sii_destinos_etapa (
                region TEXT NOT NULL, comuna TEXT NOT NULL,
                periodo TEXT NOT NULL, fuente TEXT NOT NULL,
                ''' + ', '.join(f'{c} BIGINT NOT NULL' for c in MEDIDAS) +
                ') ON COMMIT DROP')
            with cur.copy('COPY sii_destinos_etapa (' + ', '.join(COLUMNAS) + ') FROM STDIN') as copia:
                for fila in filas:
                    copia.write_row(tuple(fila[c] for c in COLUMNAS))
            cur.execute('''INSERT INTO dw.dim_ubicacion (region, comuna)
                SELECT e.region, e.comuna FROM sii_destinos_etapa e
                WHERE NOT EXISTS (
                    SELECT 1 FROM dw.dim_ubicacion d
                    WHERE lower(btrim(d.region)) = lower(btrim(e.region))
                      AND lower(btrim(d.comuna)) = lower(btrim(e.comuna))
                )''')
            ubicaciones_nuevas = cur.rowcount
            crear_fact(cur)
            keys = ', '.join('e.' + c for c in MEDIDAS)
            cur.execute(f'''INSERT INTO {TABLA_FACT}
                (id_ubicacion, periodo, fuente, {', '.join(MEDIDAS)})
                SELECT d.id_ubicacion, e.periodo, e.fuente, {keys}
                FROM sii_destinos_etapa e
                JOIN dw.dim_ubicacion d
                  ON lower(btrim(d.region)) = lower(btrim(e.region))
                 AND lower(btrim(d.comuna)) = lower(btrim(e.comuna))
                ON CONFLICT (id_ubicacion, periodo, fuente) DO NOTHING''')
            hechos_nuevos = cur.rowcount
            # Detectar diferencias en cualquier métrica al re-ejecutar.
            distintos = ' OR '.join(f'f.{c} IS DISTINCT FROM e.{c}' for c in MEDIDAS)
            cur.execute(f'''SELECT COUNT(*) FROM sii_destinos_etapa e
                JOIN dw.dim_ubicacion d
                  ON lower(btrim(d.region))=lower(btrim(e.region))
                 AND lower(btrim(d.comuna))=lower(btrim(e.comuna))
                LEFT JOIN {TABLA_FACT} f
                  ON f.id_ubicacion=d.id_ubicacion AND f.periodo=e.periodo AND f.fuente=e.fuente
                WHERE f.id_ubicacion IS NULL OR ({distintos})''')
            discrepancias = cur.fetchone()[0]
            if discrepancias:
                raise RuntimeError(f'{discrepancias} comunas con datos distintos o sin cargar: se revierte transacción')
            cur.execute(f'''SELECT COUNT(*), SUM(predios_total)
                FROM {TABLA_FACT} WHERE periodo=%s AND fuente=%s''', (PERIODO, FUENTE))
            resultado = cur.fetchone()
            if resultado != (ESPERADO_FILAS, ESPERADO_TOTAL):
                raise RuntimeError('Cifras de hechos inesperadas: ' + str(resultado))
            cur.execute('''SELECT COUNT(*), SUM(f.predios_total)
                FROM dw.fact_sii_destinos f
                JOIN dw.dim_ubicacion d ON d.id_ubicacion=f.id_ubicacion
                WHERE f.periodo=%s AND f.fuente=%s AND d.region='Metropolitana' ''',
                (PERIODO, FUENTE))
            if cur.fetchone() != (ESPERADO_RM_FILAS, ESPERADO_RM_TOTAL):
                raise RuntimeError('Cifras de Región Metropolitana inesperadas')
            cur.execute('SELECT COUNT(*) FROM dw.fact_publicaciones')
            hechos_despues = cur.fetchone()[0]
            if hechos_antes != hechos_despues:
                raise RuntimeError('Cambió el total de publicaciones durante carga SII')
            cur.execute('SELECT COUNT(*) FROM dw.dim_ubicacion')
            ubicaciones_despues = cur.fetchone()[0]
        # commit automático cuando todas las pruebas fueron exitosas
    print(f'Ubicaciones: {ubicaciones_antes} -> {ubicaciones_despues} (+{ubicaciones_nuevas})')
    print(f'Hechos SII nuevos: {hechos_nuevos}; periodo {PERIODO}: {ESPERADO_FILAS} comunas')
    print(f'Predios SII {PERIODO}: {ESPERADO_TOTAL:,}'.replace(',', '.'))
    print(f'dw.fact_publicaciones intacta: {hechos_despues} filas')
    print('Códigos oficiales INE: pendientes; no se inventaron.')


def main():
    ap = argparse.ArgumentParser(description='SII Delta -> DW PostgreSQL (2026-S1)')
    modo = ap.add_mutually_exclusive_group(required=True)
    modo.add_argument('--auditar', action='store_true', help='Solo lecturas; validar Delta y esquema DW')
    modo.add_argument('--cargar', action='store_true', help='Integrar al DW con una transacción segura')
    args = ap.parse_args()
    print('Leyendo tabla Delta:', TABLA_DELTA)
    filas = datos_delta()
    validar(filas)
    if args.auditar:
        with conexion_postgres() as conn:
            with conn.cursor() as cur:
                verificar_esquema(cur)
                cur.execute('SELECT COUNT(*) FROM dw.dim_ubicacion')
                print('Ubicaciones actuales en PostgreSQL:', cur.fetchone()[0])
                cur.execute('SELECT COUNT(*) FROM dw.fact_publicaciones')
                print('Publicaciones actuales en PostgreSQL:', cur.fetchone()[0])
        print('Auditoría completada SIN ESCRITURAS.')
    else:
        cargar(filas)
        print('Integración SII 2026-S1 a PostgreSQL completada.')


if __name__ == '__main__':
    main()
