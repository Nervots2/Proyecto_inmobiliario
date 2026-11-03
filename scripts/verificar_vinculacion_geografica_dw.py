"""Auditoría de cruce de reportes geográficos / Delta / PostgreSQL.

NO MODIFICA ni crea archivos o tablas. Ejecutar desde la raíz del repositorio:
    python scripts/verificar_vinculacion_geografica_dw.py

Compara por ID de publicación (maneja prefijo histórico MLC- en Portal).
ChilePropiedades: slug exacto. Portal: informa candidatos y subset conservador
cuando el último segmento de la ubicación es exactamente la comuna reconocida.
"""
from __future__ import annotations

import csv
import os
import re
import unicodedata
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIR = ROOT / 'reports' / 'auditorias' / 'geografia_publicaciones'
ARCHIVOS = {
    'chilepropiedades': ('candidatos_chilepropiedades.csv', ('comuna_slug',)),
    'portal_inmobiliario': ('candidatos_portal_inmobiliario.csv', ('region_busqueda', 'ubicacion')),
}
CONSULTAS = {
    'chilepropiedades': '''SELECT CAST(id_publicacion AS VARCHAR), COALESCE(comuna_slug, '')
      FROM delta.inmobiliario.publicaciones_delta_final''',
    'portal_inmobiliario': '''SELECT CAST(id_publicacion AS VARCHAR),
      COALESCE(region_busqueda, ''), COALESCE(ubicacion, '')
      FROM delta.inmobiliario.portal_inmobiliario_publicaciones''',
}
EXPECTED = {'chilepropiedades': 28307, 'portal_inmobiliario': 7609}


def norm(s):
    s = unicodedata.normalize('NFKD', str(s or '').casefold())
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return ' '.join(re.sub(r'[^a-z0-9]+', ' ', s).split())


def eq_name(s):
    n = norm(s)
    return {'paihuano': 'paiguano', 'trehuaco': 'treguaco',
            'marchigue': 'marchihue',
            'cabo de hornos ex navarino': 'cabo de hornos'}.get(n, n)


def ident(source, raw):
    s = str(raw or '').strip()
    if source == 'portal_inmobiliario':
        s = re.sub(r'^MLC[-_ ]?', '', s, flags=re.IGNORECASE)
    if not s:
        raise ValueError(f'ID vacío en {source}')
    return (source, s)


def check(cond, message):
    if not cond:
        raise RuntimeError(message)


def reportes():
    report = {}
    counters = {}
    for source, (name, fields) in ARCHIVOS.items():
        path = DIR / name
        check(path.is_file(), f'Falta reporte: {path}. Ejecutar auditoría con --guardar-reportes')
        groups = {}
        total = 0
        state_counts = Counter()
        with path.open('r', encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f)
            required = {'fuente', 'registros', 'estado', 'codigo_comuna', 'id_ubicacion',
                        'comuna_ine_sii', *fields}
            check(required.issubset(reader.fieldnames or []), f'Cabeceras incompletas: {name}')
            for row in reader:
                key = tuple((row[k] or '') for k in fields)
                check(key not in groups, f'Grupo repetido en {name}: {key}')
                n = int(row['registros'])
                check(n > 0 and row['fuente'] == source, f'Fila inválida en {name}: {row}')
                groups[key] = row
                total += n
                state_counts[row['estado']] += n
                if row['estado'] == 'candidato_unico':
                    check(re.fullmatch(r'\d{5}', row['codigo_comuna'] or '') is not None,
                          f'CUT incorrecto en {name}: {key}')
                    check((row['id_ubicacion'] or '').isdigit(), f'Ubicación inválida: {key}')
                else:
                    check(not row['id_ubicacion'] and not row['codigo_comuna'],
                          f'Pendiente con ubicación asignada: {key}')
        check(total == EXPECTED[source], f'Reporte {name}: {total}, esperado {EXPECTED[source]}')
        report[source] = groups
        counters[source] = state_counts
        print(f'{source}: {len(groups)} grupos | {total} registros | estados {dict(state_counts)}')
    return report


def postgres_data():
    import psycopg
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    mandatory = ('POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD')
    check(all(os.getenv(k) for k in mandatory), 'Faltan variables PostgreSQL en .env')
    with psycopg.connect(host='127.0.0.1', port=5432,
                         dbname=os.getenv('POSTGRES_DB'), user=os.getenv('POSTGRES_USER'),
                         password=os.getenv('POSTGRES_PASSWORD'), connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT id_ubicacion, codigo_comuna, comuna
                         FROM dw.dim_ubicacion''')
            lookup = {int(id_loc): (str(cut).zfill(5), comuna) for id_loc, cut, comuna in cur.fetchall()}
            check(len(lookup) == 346, 'La dimensión geográfica no tiene 346 filas')
            cur.execute('''SELECT fuente, id_publicacion, id_ubicacion
                         FROM dw.fact_publicaciones
                         WHERE fuente IN ('chilepropiedades', 'portal_inmobiliario')''')
            dw = {}
            for source, raw, loc in cur.fetchall():
                key = ident(source, raw)
                check(key not in dw, f'ID DW duplicado (tras normalizar): {key}')
                dw[key] = int(loc) if loc is not None else None
    check(len(dw) == sum(EXPECTED.values()),
          f'DW contiene {len(dw)} IDs únicos; esperado {sum(EXPECTED.values())}')
    return lookup, dw


def delta_data():
    from trino.dbapi import connect
    conn = connect(host='127.0.0.1', port=8088, user='inmobiliario',
                   catalog='delta', schema='inmobiliario', http_scheme='http')
    rows = {}
    try:
        cur = conn.cursor()
        for source, sql in CONSULTAS.items():
            cur.execute(sql)
            count = 0
            while batch := cur.fetchmany(1000):
                for row in batch:
                    key = ident(source, row[0])
                    check(key not in rows, f'ID Delta duplicado: {key}')
                    rows[key] = tuple(str(v or '') for v in row[1:])
                    count += 1
            check(count == EXPECTED[source], f'Delta {source}: {count} != {EXPECTED[source]}')
    finally:
        conn.close()
    return rows


def main():
    print('=== CRUCE GEOGRÁFICO: REPORTES / DELTA / DATA WAREHOUSE ===')
    print('MODO SOLO LECTURA. No se modificará ninguna base ni archivo.\n')
    reports = reportes()
    lookup, dw = postgres_data()
    delta = delta_data()
    missing_dw = sorted(set(delta) - set(dw))
    missing_delta = sorted(set(dw) - set(delta))
    check(not missing_dw and not missing_delta,
          f'IDs no alineados: {len(missing_dw)} Delta sin DW, {len(missing_delta)} DW sin Delta. '
          f'Ejemplos: {missing_dw[:4]}, {missing_delta[:4]}')
    print('\nIDs Delta ↔ DW: 35.916/35.916 conciliados por fuente e ID.\n')
    groups_found = {source: Counter() for source in ARCHIVOS}
    states = Counter()
    last_segments = Counter()
    preassigned = 0
    preassigned_wrong = []
    for (source, _), fields in delta.items():
        groups_found[source][fields] += 1
        row = reports[source].get(fields)
        check(row is not None, f'Grupo no encontrado en reporte {source}: {fields}')
        status = row['estado']
        states[(source, status)] += 1
        source_key = (source, _)
        loc = dw[source_key]
        if loc is not None:
            preassigned += 1
        if status == 'candidato_unico':
            proposal_id = int(row['id_ubicacion'])
            check(proposal_id in lookup, f'ID ubicación inexistente: {proposal_id}')
            check(lookup[proposal_id][0] == row['codigo_comuna'],
                  f'Código CUT diferente en dw.dim_ubicacion: {fields}')
            if loc is not None and loc != proposal_id:
                preassigned_wrong.append((source_key, loc, proposal_id))
            if source == 'portal_inmobiliario':
                final = eq_name(fields[1].split(',')[-1])
                commune = eq_name(row['comuna_ine_sii'])
                last_segments['terminal_exact'] += int(final == commune)
                last_segments['other_segment'] += int(final != commune)
    check(not preassigned_wrong,
          f'Existen {len(preassigned_wrong)} ubicaciones previas incompatibles: {preassigned_wrong[:5]}')
    for source, counter in groups_found.items():
        for key, row in reports[source].items():
            check(counter[key] == int(row['registros']),
                  f'Cantidad grupo distinta del reporte en {source}: {key} '
                  f'(Delta {counter[key]} vs CSV {row["registros"]})')
    print('Grupos reportados ↔ filas Delta: conciliados sin diferencias.')
    print('Ubicaciones ya asignadas en DW:', preassigned)
    print('Candidatos ChilePropiedades:', states['chilepropiedades', 'candidato_unico'])
    print('Candidatos Portal Inmobiliario:', states['portal_inmobiliario', 'candidato_unico'])
    print('Portal con comuna exacta al final:', last_segments['terminal_exact'])
    print('Portal con comuna en otro segmento:', last_segments['other_segment'])
    print('Pendientes totales:', sum(EXPECTED.values()) -
          states['chilepropiedades', 'candidato_unico'] -
          states['portal_inmobiliario', 'candidato_unico'])
    print('\nAUDITORÍA COMPLETADA SIN ESCRITURAS.')
    print('Siguiente fase: revisar los candidatos antes de actualizar id_ubicacion.')


if __name__ == '__main__':
    main()
