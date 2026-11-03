"""Resuelve 3 diferencias INE 2024 / SII 2026-S1 y completa códigos en DW.

Ejecutar desde la raíz de Proyecto_inmobiliario:
    python scripts/completar_codigos_ine_dw.py --auditar
    python scripts/completar_codigos_ine_dw.py --cargar

Entradas: informe de validar_ine_2024_sii.py y dw.dim_ubicacion.
No requiere GeoPackage. Nunca modifica fact_publicaciones ni fact_sii_destinos.
Solo --cargar actualiza codigo_region/codigo_comuna en dw.dim_ubicacion.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import os
from pathlib import Path
import re
import unicodedata

ROOT = Path(__file__).resolve().parent.parent
INPUT = ROOT / 'datalake_procesado/silver/ine/ine_comunas_2024_vs_sii_2026_s1.csv'
OUTPUT = ROOT / 'datalake_procesado/silver/ine/ine_sii_codigos_resueltos_2026_s1.csv'
EXPECTED_N = 346
EXPECTED_PREDIOS = 8_560_728
EXPECTED_DW = 35_916

# Códigos verificados mediante las fuentes oficiales indicadas.
# Ninguno de los tres se presentó como coincidencia exacta en la capa Comunal_CPV24.
OVERRIDES = {
    ('coquimbo', 'paiguano'): {
        'region': '04', 'cut': '04105', 'referencia': 'Paihuano / Paiguano',
        'fuente': 'SUBDERE, decreto 1381 (CUT 04105)',
        'geometria': 'no_verificada_en_capa',
    },
    ('nuble', 'treguaco'): {
        'region': '16', 'cut': '16207', 'referencia': 'Treguaco / Trehuaco',
        'fuente': 'INE, libro de codigos ENE 2025 (CUT 16207)',
        'geometria': 'no_verificada_en_capa',
    },
    ('magallanes', 'antartica'): {
        'region': '12', 'cut': '12202', 'referencia': 'Antartica',
        'fuente': 'INE, clasificacion de comunas (CUT 12202)',
        'geometria': 'ausente_en_capa_ine_2024',
    },
}


def normalize(value: object) -> str:
    txt = unicodedata.normalize('NFKD', str(value or '').casefold())
    txt = ''.join(c for c in txt if not unicodedata.combining(c))
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', txt).split())


def code(value: object, n: int) -> str | None:
    if value is None or str(value).strip() == '':
        return None
    text = str(value).strip()
    if re.fullmatch(r'\d+(?:\.0+)?', text) is None:
        raise ValueError(f'Código inválido: {value!r}')
    integer = int(float(text))
    if integer < 0 or len(str(integer)) > n:
        raise ValueError(f'Código fuera de rango: {value!r}')
    return f'{integer:0{n}d}'


def prepare() -> list[dict]:
    if not INPUT.is_file():
        raise FileNotFoundError(f'Falta el reporte generado por validar_ine_2024_sii.py: {INPUT}')
    with INPUT.open(encoding='utf-8-sig', newline='') as file:
        reader = csv.DictReader(file)
        required = {'region_sii', 'comuna_sii', 'predios_total_sii', 'periodo_sii',
                    'estado', 'codigo_region_ine', 'codigo_comuna_ine', 'comuna_ine'}
        if not required <= set(reader.fieldnames or []):
            raise ValueError(f'Columnas faltantes: {required - set(reader.fieldnames or [])}')
        raw = list(reader)
    if len(raw) != EXPECTED_N:
        raise ValueError(f'Se esperaban 346 filas; hay {len(raw)}')
    found = set()
    overrides_seen = set()
    res = []
    total = 0
    for r in raw:
        key = (normalize(r['region_sii']), normalize(r['comuna_sii']))
        if key in found:
            raise ValueError(f'Región/comuna duplicada: {key}')
        found.add(key)
        if r['periodo_sii'] != '2026-S1':
            raise ValueError(f'Período inesperado para {key}')
        predios = int(r['predios_total_sii'])
        if predios < 0:
            raise ValueError(f'Predios negativos: {key}')
        total += predios
        original_estado = r['estado'].strip()
        if original_estado == 'validado':
            region = code(r['codigo_region_ine'], 2)
            cut = code(r['codigo_comuna_ine'], 5)
            if not region or not cut:
                raise ValueError(f'Faltan códigos INE en fila validada {key}')
            status = 'validado_ine2024'
            source = 'INE Censo 2024, capa Comunal_CPV24'
            geometry = 'incluida_en_capa'
            reference_name = r['comuna_ine']
        elif original_estado == 'sin_coincidencia' and key in OVERRIDES:
            special = OVERRIDES[key]
            region, cut = special['region'], special['cut']
            source, geometry = special['fuente'], special['geometria']
            reference_name = special['referencia']
            status = 'codigo_oficial_conciliado'
            overrides_seen.add(key)
        else:
            raise ValueError(f'Caso no resuelto; se detiene: {key} [{original_estado}]')
        if cut[:2] != region:
            raise ValueError(f'Código CUT no corresponde a región: {key} {region}/{cut}')
        res.append({
            'region_sii': r['region_sii'], 'comuna_sii': r['comuna_sii'],
            'periodo_sii': r['periodo_sii'], 'predios_total_sii': predios,
            'codigo_region': region, 'codigo_comuna': cut,
            'estado_integracion': status, 'nombre_referencia': reference_name,
            'procedencia_codigo': source, 'geometria_ine_2024': geometry,
        })
    if overrides_seen != set(OVERRIDES):
        raise ValueError(f'Faltan diferencias previstas: {set(OVERRIDES) - overrides_seen}')
    if len({x['codigo_comuna'] for x in res}) != EXPECTED_N:
        duplicated = [k for k, v in Counter(x['codigo_comuna'] for x in res).items() if v > 1]
        raise ValueError(f'CUT duplicados: {duplicated}')
    if total != EXPECTED_PREDIOS:
        raise ValueError(f'Predios esperados {EXPECTED_PREDIOS}; obtuvimos {total}')
    if len([x for x in res if normalize(x['region_sii']) == 'metropolitana']) != 52:
        raise ValueError('La RM no tiene 52 comunas')
    return res


def connect():
    try:
        import psycopg
        from dotenv import load_dotenv
    except ImportError as e:
        raise RuntimeError('Se requieren psycopg y python-dotenv en el entorno inmobiliario') from e
    load_dotenv(ROOT / '.env')
    missing = [v for v in ('POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_DB') if not os.getenv(v)]
    if missing:
        raise RuntimeError('Variables ausentes de .env: ' + ', '.join(missing))
    return psycopg.connect(host='127.0.0.1', port=5432,
                           user=os.environ['POSTGRES_USER'],
                           password=os.environ['POSTGRES_PASSWORD'],
                           dbname=os.environ['POSTGRES_DB'])


def inspect_database(cur, mapping: list[dict]):
    cur.execute("SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='dw' AND table_name='dim_ubicacion'")
    columns = {x[0] for x in cur.fetchall()}
    must = {'id_ubicacion', 'region', 'comuna', 'codigo_region', 'codigo_comuna'}
    if not must <= columns:
        raise ValueError(f'Esquema dw.dim_ubicacion no coincide: {columns}')
    cur.execute('SELECT id_ubicacion, region, comuna, codigo_region, codigo_comuna FROM dw.dim_ubicacion')
    dbrows = cur.fetchall()
    if len(dbrows) != EXPECTED_N:
        raise ValueError(f'Se esperaban 346 ubicaciones en DW; hay {len(dbrows)}')
    by_key = {}
    for identifier, region, comuna, codigo_region, codigo_comuna in dbrows:
        key = (normalize(region), normalize(comuna))
        if key in by_key:
            raise ValueError(f'Ubicación duplicada en DW: {key}')
        by_key[key] = (identifier, codigo_region, codigo_comuna)
    plan = []
    for x in mapping:
        key = (normalize(x['region_sii']), normalize(x['comuna_sii']))
        if key not in by_key:
            raise ValueError(f'Ubicación del SII ausente en DW: {key}')
        ident, actual_region, actual_comuna = by_key[key]
        old_region = code(actual_region, 2)
        old_comuna = code(actual_comuna, 5)
        new_region, new_comuna = x['codigo_region'], x['codigo_comuna']
        if old_region is not None and old_region != new_region:
            raise ValueError(f'Conflicto con codigo_region existente: {key}: {old_region} vs {new_region}')
        if old_comuna is not None and old_comuna != new_comuna:
            raise ValueError(f'Conflicto con codigo_comuna existente: {key}: {old_comuna} vs {new_comuna}')
        if old_region != new_region or old_comuna != new_comuna:
            plan.append((new_region, new_comuna, ident))
    cur.execute('SELECT COUNT(*) FROM dw.fact_publicaciones')
    fact_count = cur.fetchone()[0]
    if fact_count != EXPECTED_DW:
        raise ValueError(f'El DW de publicaciones tiene {fact_count}; se esperaban {EXPECTED_DW}')
    cur.execute("SELECT COUNT(*), COALESCE(SUM(predios_total), 0) FROM dw.fact_sii_destinos "
                "WHERE periodo='2026-S1' AND fuente='SII'")
    fact_sii = cur.fetchone()
    if tuple(fact_sii) != (EXPECTED_N, EXPECTED_PREDIOS):
        raise ValueError(f'Hechos SII no coinciden: {fact_sii}')
    return plan


def run(mode: str):
    mapping = prepare()
    print('=== INE 2024 / SII 2026-S1: CÓDIGOS COMUNALES ===')
    print('Correspondencias: 343 de cartografía INE + 2 denominaciones + 1 Antártica')
    print('346 CUT únicos; 8.560.728 predios; 52 comunas de RM')
    print('Antártica tiene código administrativo, NO geometría en la capa inspeccionada.')
    for x in mapping:
        if x['estado_integracion'] != 'validado_ine2024':
            print(f"  {x['region_sii']} / {x['comuna_sii']}: "
                  f"CUT {x['codigo_comuna']} [{x['geometria_ine_2024']}]")
    with connect() as conn:
        with conn.cursor() as cur:
            if mode == 'cargar':
                cur.execute('LOCK TABLE dw.dim_ubicacion IN SHARE ROW EXCLUSIVE MODE')
            changes = inspect_database(cur, mapping)
            print(f'Filas en dw.dim_ubicacion: 346; códigos que faltan completar: {len(changes)}')
            print('dw.fact_publicaciones: 35.916; hechos SII: 346; verificados')
            if mode == 'cargar':
                if changes:
                    cur.executemany(
                        'UPDATE dw.dim_ubicacion SET codigo_region=%s, codigo_comuna=%s '
                        'WHERE id_ubicacion=%s', changes
                    )
                if inspect_database(cur, mapping):
                    raise RuntimeError('Quedaron códigos sin completar; transacción revertida')
                cur.execute('SELECT COUNT(DISTINCT codigo_comuna) FROM dw.dim_ubicacion')
                if cur.fetchone()[0] != 346:
                    raise RuntimeError('No hay 346 códigos distintos en DW: se revierte')
            else:
                # La auditoría no escribe. Evitamos cualquier modificación accidental.
                conn.rollback()
    if mode == 'cargar':
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        with OUTPUT.open('w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(mapping[0]))
            writer.writeheader()
            writer.writerows(mapping)
        print(f'Carga confirmada. Códigos actualizados: {len(changes)}.')
        print(f'Reporte de trazabilidad: {OUTPUT.relative_to(ROOT)}')
    else:
        print('AUDITORÍA COMPLETADA SIN ESCRITURAS. Para cargar: --cargar')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--auditar', action='store_true', help='Valida los códigos y consulta PostgreSQL sin escribir')
    group.add_argument('--cargar', action='store_true', help='Completa códigos en una transacción con validaciones')
    args = parser.parse_args()
    run('cargar' if args.cargar else 'auditar')
