"""Comparación de comunas INE 2017 (DBF dentro de ZIP) vs SII 2026-S1.

Uso desde la raíz de Proyecto_inmobiliario:
    python scripts/comparar_ine_sii_comunas.py

Auditoría SOLO LECTURA: no modifica PostgreSQL, ZIP ni CSV.
No requiere librerías externas.
"""
from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path
import re
import unicodedata
import zipfile

RAIZ = Path(__file__).resolve().parent.parent
RUTA_INE = RAIZ / 'datalake_crudo' / 'ine' / 'ine_cartografia_comunas_censo2017_shp.zip'
RUTA_SII = RAIZ / 'datalake_procesado' / 'silver' / 'sii' / 'sii_destinos_comuna_2026_s1_nacional.csv'


def decodificar(valor: bytes) -> str:
    valor = valor.strip(b' \x00')
    for encoding in ('utf-8', 'cp1252', 'latin-1'):
        try:
            return valor.decode(encoding).strip()
        except UnicodeDecodeError:
            pass
    return valor.decode('latin-1', errors='replace').strip()


def clave_nombre(texto: str) -> str:
    texto = unicodedata.normalize('NFD', texto.casefold().strip())
    texto = ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
    texto = re.sub(r'[^a-z0-9]+', ' ', texto)
    return ' '.join(texto.split())


def leer_ine(ruta: Path) -> list[dict]:
    with zipfile.ZipFile(ruta) as z:
        dbfs = [n for n in z.namelist() if n.lower().endswith('.dbf')]
        if len(dbfs) != 1:
            raise ValueError(f'Se esperaba un DBF en el ZIP, encontrados: {dbfs}')
        with z.open(dbfs[0]) as f:
            cab = f.read(32)
            if len(cab) != 32:
                raise ValueError('Cabecera DBF incompleta')
            nreg = int.from_bytes(cab[4:8], 'little')
            long_cab = int.from_bytes(cab[8:10], 'little')
            long_reg = int.from_bytes(cab[10:12], 'little')
            des = f.read(long_cab - 32)
            columnas = []
            for i in range(0, len(des), 32):
                d = des[i:i + 32]
                if not d or d[0] == 13:
                    break
                columnas.append((d[:11].split(b'\0')[0].decode('ascii'), d[16]))
            nombres = {n for n, _ in columnas}
            obligatorias = {'REGION', 'NOM_REGION', 'COMUNA', 'NOM_COMUNA'}
            if not obligatorias.issubset(nombres):
                raise ValueError(f'Faltan columnas DBF: {obligatorias - nombres}')
            filas = []
            for _ in range(nreg):
                registro = f.read(long_reg)
                if len(registro) != long_reg:
                    raise ValueError('DBF truncado')
                if registro[:1] == b'*':
                    continue
                pos, datos = 1, {}
                for nombre, largo in columnas:
                    if nombre in obligatorias:
                        datos[nombre] = decodificar(registro[pos:pos + largo])
                    pos += largo
                filas.append({
                    'comuna': datos['NOM_COMUNA'],
                    'region': datos['NOM_REGION'],
                    'codigo_comuna_2017': datos['COMUNA'],
                    'codigo_region_2017': datos['REGION'],
                })
            return filas


def leer_sii(ruta: Path) -> list[dict]:
    with ruta.open(encoding='utf-8-sig', newline='') as f:
        lector = csv.DictReader(f)
        requeridas = {'region', 'comuna', 'periodo', 'predios_total'}
        if not requeridas.issubset(lector.fieldnames or []):
            raise ValueError(f'Faltan columnas CSV: {requeridas - set(lector.fieldnames or [])}')
        filas = list(lector)
    if any(fila['periodo'] != '2026-S1' for fila in filas):
        raise ValueError('El CSV no contiene solo 2026-S1')
    return filas


def indexar(filas: list[dict]) -> dict[str, list[dict]]:
    indice = defaultdict(list)
    for fila in filas:
        indice[clave_nombre(fila['comuna'])].append(fila)
    return indice


def main() -> None:
    for ruta in (RUTA_INE, RUTA_SII):
        if not ruta.is_file():
            raise SystemExit(f'Archivo no encontrado: {ruta}')
    ine = leer_ine(RUTA_INE)
    sii = leer_sii(RUTA_SII)
    idx_ine, idx_sii = indexar(ine), indexar(sii)
    nombres_ine, nombres_sii = set(idx_ine), set(idx_sii)
    comunes = nombres_ine & nombres_sii
    ambiguas = {k for k in comunes if len(idx_ine[k]) != 1 or len(idx_sii[k]) != 1}
    matches = sorted(comunes - ambiguas)
    print('=== COMPARACIÓN INE 2017 vs SII 2026-S1 (SOLO LECTURA) ===')
    print(f'Registros INE 2017: {len(ine)} | SII 2026-S1: {len(sii)}')
    print(f'Coincidencias únicas por nombre normalizado: {len(matches)}')
    print(f'Nombres ambiguos/repetidos: {len(ambiguas)}')
    for clave in sorted(ambiguas):
        print(f'  {clave}: INE={len(idx_ine[clave])}, SII={len(idx_sii[clave])}')
    print('\nComunas SII sin coincidencia exacta en INE:')
    solo_sii = sorted(nombres_sii - nombres_ine)
    if not solo_sii: print('  Ninguna')
    for k in solo_sii:
        for f in idx_sii[k]:
            print(f"  {f['region']} / {f['comuna']} (predios={f['predios_total']})")
    print('\nComunas INE sin coincidencia exacta en SII:')
    solo_ine = sorted(nombres_ine - nombres_sii)
    if not solo_ine: print('  Ninguna')
    for k in solo_ine:
        for f in idx_ine[k]:
            print(f"  {f['region']} / {f['comuna']} (código histórico={f['codigo_comuna_2017']})")
    print('\nCorrespondencias de regiones entre los nombres coincidentes:')
    grupos = Counter((idx_ine[k][0]['region'], idx_sii[k][0]['region']) for k in matches)
    for (region_ine, region_sii), n in sorted(grupos.items(), key=lambda x:(-x[1], x[0])):
        print(f'  INE 2017: {region_ine} -> SII 2026: {region_sii}: {n} comunas')
    invalidos = [f for f in ine if not re.fullmatch(r'\d{5}', f['codigo_comuna_2017'])]
    print(f'\nCódigos comunales INE 2017 sin formato de 5 dígitos: {len(invalidos)}')
    print('\nATENCIÓN: la Región de Ñuble fue creada después del Censo 2017.')
    print('Los códigos y asignaciones regionales de 2017 son históricos;')
    print('NO actualizar dw.dim_ubicacion sin validar contra INE 2024.')


if __name__ == '__main__':
    main()
