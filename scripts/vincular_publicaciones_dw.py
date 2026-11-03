"""Vinculación geográfica conservadora de publicaciones del DW.

Ejecución desde la raíz del repositorio (con servicios Docker funcionando):
  python scripts/vincular_publicaciones_dw.py --auditar
  python scripts/vincular_publicaciones_dw.py --cargar

Requiere los archivos:
  scripts/verificar_vinculacion_geografica_dw.py
  reports/auditorias/geografia_publicaciones/candidatos_*.csv

Política de vinculación:
  - ChilePropiedades: solo candidato_unico por comuna_slug.
  - Portal: solo candidato_unico cuya comuna sea el ÚLTIMO segmento exacto
    (normalizado) del campo ubicacion. Los otros 170 casos quedan pendientes.
  - Ningún registro ambiguo o sin coincidencia se actualiza.
  - El UPDATE se ejecuta en una única transacción con controles de integridad.
"""
from __future__ import annotations
import argparse
import os
from collections import Counter
from pathlib import Path

import verificar_vinculacion_geografica_dw as v

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_ALL = 35_916
EXPECTED_CHILE = 28_204
EXPECTED_PORTAL_STRICT = 6_984
EXPECTED_ELIGIBLE = EXPECTED_CHILE + EXPECTED_PORTAL_STRICT


def ensure(ok: bool, message: str) -> None:
    if not ok:
        raise RuntimeError(message)


def build_plan():
    reports = v.reportes()
    locations, dw = v.postgres_data()
    delta = v.delta_data()
    ensure(len(delta) == EXPECTED_ALL and len(dw) == EXPECTED_ALL,
           'Delta y DW no tienen los 35.916 IDs esperados')
    ensure(set(delta) == set(dw), 'Los IDs de Delta y DW no coinciden')

    counts_by_group = {source: Counter() for source in v.ARCHIVOS}
    eligible = {}
    counts = Counter()
    failures = []

    for key, fields in delta.items():
        source = key[0]
        counts_by_group[source][fields] += 1
        report = reports[source].get(fields)
        ensure(report is not None, f'Grupo Delta no existe en el reporte: {key}')
        status = report['estado']
        if status != 'candidato_unico':
            counts[f'{source}:pendiente'] += 1
            continue
        if source == 'portal_inmobiliario':
            last_name = v.eq_name(fields[1].split(',')[-1])
            official_name = v.eq_name(report['comuna_ine_sii'])
            if last_name != official_name:
                counts['portal_inmobiliario:segmento_intermedio_excluido'] += 1
                continue

        loc_id = int(report['id_ubicacion'])
        cut = str(report['codigo_comuna'])
        ensure(loc_id in locations and locations[loc_id][0] == cut,
               f'CUT o id_ubicacion incompatible en reporte: {key}')
        prev = dw[key]
        if prev is not None and prev != loc_id:
            failures.append((key, prev, loc_id))
        eligible[key] = loc_id
        counts[f'{source}:elegible'] += 1
        if prev == loc_id:
            counts['ya_vinculado'] += 1

    ensure(not failures, f'Asignaciones ya existentes incompatibles: {failures[:5]}')
    for source, groups in reports.items():
        for fields, row in groups.items():
            ensure(counts_by_group[source][fields] == int(row['registros']),
                   f'Cantidad distinta entre reporte y Delta en {source}: {fields}')
    ensure(counts['chilepropiedades:elegible'] == EXPECTED_CHILE,
           f'ChilePropiedades: candidatos {counts["chilepropiedades:elegible"]}, esperados {EXPECTED_CHILE}')
    ensure(counts['portal_inmobiliario:elegible'] == EXPECTED_PORTAL_STRICT,
           f'Portal: candidatos estrictos {counts["portal_inmobiliario:elegible"]}, esperados {EXPECTED_PORTAL_STRICT}')
    ensure(len(eligible) == EXPECTED_ELIGIBLE, 'Cantidad elegible inesperada')
    print('\n=== PLAN DE VINCULACIÓN CONSERVADORA ===')
    print('Publicaciones totales:', EXPECTED_ALL)
    print('ChilePropiedades con comuna exacta:', counts['chilepropiedades:elegible'])
    print('Portal con comuna exacta al final:', counts['portal_inmobiliario:elegible'])
    print('Portal excluido (comuna en otro segmento):',
          counts['portal_inmobiliario:segmento_intermedio_excluido'])
    print('Otros casos pendientes:', sum(c for k, c in counts.items() if k.endswith(':pendiente')))
    print('Candidatos seguros:', len(eligible))
    print('Pendientes después de vinculación:', EXPECTED_ALL - len(eligible))
    print('Ya vinculados correctamente:', counts['ya_vinculado'])
    print('Por vincular:', len(eligible) - counts['ya_vinculado'])
    return eligible


def apply_plan(eligible: dict):
    import psycopg
    from dotenv import load_dotenv

    load_dotenv(ROOT / '.env')
    needed = ('POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD')
    ensure(all(os.getenv(n) for n in needed), 'Variables PostgreSQL faltantes en .env')

    # Recupera los IDs exactamente como se almacenaron en DW, conservando
    # prefijos históricos de Portal. Nunca supone que el ID normalizado está
    # escrito literalmente en fact_publicaciones.
    with psycopg.connect(host='127.0.0.1', port=5432,
                         dbname=os.environ['POSTGRES_DB'],
                         user=os.environ['POSTGRES_USER'],
                         password=os.environ['POSTGRES_PASSWORD'],
                         connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute('LOCK TABLE dw.fact_publicaciones IN SHARE ROW EXCLUSIVE MODE')
            cur.execute('SELECT COUNT(*) FROM dw.fact_publicaciones')
            ensure(cur.fetchone()[0] == EXPECTED_ALL,
                   'DW cambió: total diferente de 35.916. No se escribe.')
            cur.execute('''SELECT fuente, CAST(id_publicacion AS VARCHAR), id_ubicacion
                        FROM dw.fact_publicaciones
                        WHERE fuente IN ('chilepropiedades', 'portal_inmobiliario')''')
            raw_dw = {}
            for source, raw, prev in cur.fetchall():
                key = v.ident(source, raw)
                ensure(key not in raw_dw, f'ID DW duplicado: {key}')
                raw_dw[key] = (source, raw, int(prev) if prev is not None else None)
            ensure(len(raw_dw) == EXPECTED_ALL and set(eligible).issubset(raw_dw),
                   'IDs del DW cambiaron después de la auditoría')

            stage = []
            for key, new_loc in eligible.items():
                source, raw, prev = raw_dw[key]
                ensure(prev is None or prev == new_loc,
                       f'Conflicto con ubicación ya asignada: {key}')
                stage.append((source, raw, new_loc))

            cur.execute('''CREATE TEMP TABLE _stage_geo_publicaciones (
                             fuente text NOT NULL,
                             id_publicacion text NOT NULL,
                             id_ubicacion bigint NOT NULL,
                             PRIMARY KEY (fuente, id_publicacion)
                           ) ON COMMIT DROP''')
            with cur.copy('''COPY _stage_geo_publicaciones
                             (fuente, id_publicacion, id_ubicacion) FROM STDIN''') as cp:
                for row in stage:
                    cp.write_row(row)

            cur.execute('''SELECT COUNT(*) FROM _stage_geo_publicaciones s
                           JOIN dw.fact_publicaciones f
                           ON s.fuente = f.fuente AND s.id_publicacion = CAST(f.id_publicacion AS text)''')
            ensure(cur.fetchone()[0] == len(eligible), 'La tabla temporal no concilia todos los IDs')
            cur.execute('''SELECT COUNT(*) FROM _stage_geo_publicaciones s
                           JOIN dw.fact_publicaciones f
                           ON s.fuente = f.fuente AND s.id_publicacion = CAST(f.id_publicacion AS text)
                           WHERE f.id_ubicacion IS NOT NULL AND f.id_ubicacion <> s.id_ubicacion''')
            ensure(cur.fetchone()[0] == 0, 'DW tiene vínculos incompatibles. Se revierte.')
            cur.execute('''SELECT COUNT(*) FROM _stage_geo_publicaciones s
                           JOIN dw.fact_publicaciones f
                           ON s.fuente = f.fuente AND s.id_publicacion = CAST(f.id_publicacion AS text)
                           WHERE f.id_ubicacion IS NULL''')
            expected_updates = cur.fetchone()[0]
            cur.execute('''UPDATE dw.fact_publicaciones AS f
                           SET id_ubicacion = s.id_ubicacion
                           FROM _stage_geo_publicaciones AS s
                           WHERE f.fuente = s.fuente
                             AND CAST(f.id_publicacion AS text) = s.id_publicacion
                             AND f.id_ubicacion IS NULL''')
            ensure(cur.rowcount == expected_updates,
                   f'Actualizadas {cur.rowcount}, esperadas {expected_updates}. Se revierte.')
            cur.execute('''SELECT COUNT(*) FROM _stage_geo_publicaciones s
                           JOIN dw.fact_publicaciones f
                           ON s.fuente = f.fuente AND s.id_publicacion = CAST(f.id_publicacion AS text)
                           WHERE f.id_ubicacion = s.id_ubicacion''')
            ensure(cur.fetchone()[0] == len(eligible), 'No quedaron todos los vínculos correctos')
            cur.execute('SELECT COUNT(*) FROM dw.fact_publicaciones')
            ensure(cur.fetchone()[0] == EXPECTED_ALL, 'Se alteró el número de publicaciones')
            cur.execute('SELECT COUNT(*) FROM dw.fact_publicaciones WHERE id_ubicacion IS NULL')
            pending = cur.fetchone()[0]
            print('\nVinculaciones nuevas:', expected_updates)
            print('Publicaciones sin ubicación después de la carga:', pending)
        # Si no hubo excepciones, psycopg confirma la transacción al salir.
    print('CARGA CONFIRMADA. Publicaciones preservadas; casos inseguros no se actualizaron.')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    m = p.add_mutually_exclusive_group(required=True)
    m.add_argument('--auditar', action='store_true')
    m.add_argument('--cargar', action='store_true')
    args = p.parse_args()
    plan = build_plan()
    if args.auditar:
        print('\nAUDITORÍA COMPLETADA SIN ESCRITURAS.')
    else:
        apply_plan(plan)


if __name__ == '__main__':
    main()
