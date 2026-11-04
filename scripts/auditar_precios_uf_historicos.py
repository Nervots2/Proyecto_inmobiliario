"""Audita precios UF sospechosos de ChilePropiedades y exporta propuestas.

No importa el scraper, no accede a Internet, Silo, Delta Lake ni PostgreSQL.
Modo predeterminado: --auditar (sin escrituras).
"""

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path

ORIGEN = Path('datalake_crudo/chilepropiedades')
REPORTE = Path('reports/auditorias/calidad_precios/precios_uf_candidatos_revision.csv')
REFERENCIA_CLP_UF = Decimal('41194.55')  # Resultado de la auditoría previa; no es una cotización oficial
TOLERANCIA = Decimal('0.05')
PATRON = re.compile(
    r'(?:\bUF\s*(\d{1,3}(?:\.\d{3})+(?:,\d+)?)|'
    r'(\d{1,3}(?:\.\d{3})+(?:,\d+)?)\s*UF\b)', re.I,
)


def decimal_seguro(valor):
    try:
        v = Decimal(str(valor if valor is not None else 0))
        return v if v.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def analizar(origen):
    archivos = sorted(origen.glob('*.jsonl'))
    if not archivos:
        raise FileNotFoundError(f'No se encontraron JSONL en: {origen}')

    candidatos = defaultdict(list)
    conteo = Counter()
    for archivo in archivos:
        with archivo.open('r', encoding='utf-8-sig') as lector:
            for n, linea in enumerate(lector, 1):
                if not linea.strip():
                    continue
                try:
                    r = json.loads(linea)
                except json.JSONDecodeError as e:
                    raise ValueError(f'JSON inválido en {archivo}:{n}: {e}') from e
                conteo['observaciones_leidas'] += 1
                if not isinstance(r, dict):
                    raise ValueError(f'Registro no es objeto JSON en {archivo}:{n}')
                titulo = str(r.get('titulo') or '')
                url = str(r.get('url') or '').strip()
                m = PATRON.search(titulo)
                if not m or not url:
                    continue
                valor = (m.group(1) or m.group(2))
                uf_titulo = decimal_seguro(valor.replace('.', '').replace(',', '.'))
                precios = r.get('precios') or {}
                if not isinstance(precios, dict):
                    continue
                uf_original = decimal_seguro(precios.get('uf'))
                clp_original = decimal_seguro(precios.get('clp'))
                if (uf_titulo is None or uf_original is None or clp_original is None
                        or uf_titulo <= 0 or clp_original <= 0 or uf_original < 0):
                    continue
                # Criterio ya auditado: UF guardada al menos 10 % menor que la del título.
                if uf_original >= uf_titulo * Decimal('0.9'):
                    continue
                ratio = clp_original / uf_titulo
                if abs(ratio - REFERENCIA_CLP_UF) / REFERENCIA_CLP_UF > TOLERANCIA:
                    continue

                candidatos[url].append({
                    'url': url,
                    'titulo': titulo,
                    'id_propiedad_bronze': str(r.get('id_propiedad') or ''),
                    'clp_original': str(clp_original),
                    'uf_original': str(uf_original),
                    'uf_en_titulo_candidata': str(uf_titulo),
                    'clp_por_uf_titulo': str(ratio.quantize(Decimal('0.01'))),
                    'criterio': 'UF original < 90% de UF en titulo; CLP/UF titulo a 5% de referencia exploratoria',
                    'estado': 'requiere_revision_no_aplicar',
                    'archivo_bronze': archivo.name,
                    'linea_bronze': n,
                })
    return archivos, candidatos, conteo


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modo = parser.add_mutually_exclusive_group()
    modo.add_argument('--auditar', action='store_true', help='Solo lectura (predeterminado).')
    modo.add_argument('--generar', action='store_true', help='Exportar informe CSV local sin alterar datos fuente.')
    args = parser.parse_args()
    archivos, candidatos, conteo = analizar(ORIGEN)
    nobs = sum(len(x) for x in candidatos.values())
    conflictos = sum(len({(r['uf_original'], r['uf_en_titulo_candidata'], r['clp_original']) for r in filas}) > 1
                    for filas in candidatos.values())
    print('=== CANDIDATOS DE REVISIÓN UF (CHILEPROPIEDADES) ===')
    print('Archivos JSONL:', len(archivos))
    print('Observaciones examinadas:', conteo['observaciones_leidas'])
    print('Observaciones candidatas:', nobs)
    print('URLs únicas candidatas:', len(candidatos))
    print('Repeticiones:', nobs - len(candidatos))
    print('URLs con precios conflictivos:', conflictos)
    if conflictos:
        raise RuntimeError('Hay capturas conflictivas: no se exportará el reporte.')
    if args.generar:
        REPORTE.parent.mkdir(parents=True, exist_ok=True)
        columnas = ['url', 'titulo', 'id_propiedad_bronze', 'clp_original', 'uf_original',
                    'uf_en_titulo_candidata', 'clp_por_uf_titulo', 'criterio', 'estado',
                    'archivo_bronze', 'linea_bronze']
        with REPORTE.open('w', encoding='utf-8-sig', newline='') as salida:
            writer = csv.DictWriter(salida, fieldnames=columnas)
            writer.writeheader()
            for url in sorted(candidatos):
                writer.writerow(candidatos[url][0])
        print('Reporte de propuestas guardado:', REPORTE)
        print('ATENCIÓN: ninguna corrección fue aplicada a las publicaciones.')
    else:
        print('AUDITORÍA SIN ESCRITURAS. Para guardar el CSV: --generar')


if __name__ == '__main__':
    main()
