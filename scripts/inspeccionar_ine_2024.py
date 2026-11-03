"""Inspección segura de ZIP INE 2024 y códigos originales del DBF INE 2017.
No descomprime el GeoPackage ni cambia archivos o bases de datos.
Ejecutar desde la raíz del repositorio: python scripts/inspeccionar_ine_2024.py
"""
from __future__ import annotations

from collections import Counter
from io import BytesIO
from pathlib import Path
import struct
import sys
import zipfile

ROOT = Path(__file__).resolve().parent.parent
INE = ROOT / 'datalake_crudo' / 'ine'


def seleccionar_zip(terminos: tuple[str, ...]) -> Path:
    archivos = sorted(INE.glob('*.zip'))
    encontrados = [p for p in archivos if all(t in p.name.lower() for t in terminos)]
    if len(encontrados) != 1:
        raise RuntimeError(f'Se esperaba un ZIP con {terminos}; encontrados: {[p.name for p in encontrados]}')
    return encontrados[0]


def revisar_2024():
    ruta = seleccionar_zip(('2024', 'gpkg'))
    print('=== INE CENSO 2024: CONTENIDO DEL ZIP (SIN EXTRAER GPKG) ===')
    print('Archivo:', ruta.name)
    with zipfile.ZipFile(ruta) as z:
        for info in z.infolist():
            print(f'  {info.filename} | comprimido: {info.compress_size / 2**20:,.1f} MB | original: {info.file_size / 2**20:,.1f} MB')
        xlsx = [i for i in z.infolist() if i.filename.lower().endswith('.xlsx')]
        if not xlsx:
            print('No se encontró diccionario XLSX en el ZIP.')
            return
        try:
            from openpyxl import load_workbook
        except ImportError:
            print('Para inspeccionar el diccionario falta openpyxl. El listado del ZIP sí se completó.')
            print('Instalar, si fuera necesario: conda install openpyxl')
            return
        for info in xlsx:
            print(f'\n--- Diccionario: {info.filename} ---')
            if info.file_size > 50 * 2**20:
                print('Diccionario mayor a 50 MB: se omite lectura automática.')
                continue
            wb = load_workbook(BytesIO(z.read(info)), read_only=True, data_only=True)
            for ws in wb.worksheets:
                print(f'Hoja: {ws.title} | filas: {ws.max_row} | columnas: {ws.max_column}')
                coincidencias = 0
                for num, row in enumerate(ws.iter_rows(max_row=min(ws.max_row or 0, 350), values_only=True), 1):
                    vals = [str(v).strip().replace('\n', ' ')[:100] for v in row if v is not None]
                    linea = ' | '.join(vals[:7])
                    if num <= 3 or any(p in linea.upper() for p in ('COMUNA','REGION','REGIÓN','CODIGO','CÓDIGO','DPA','LIMITE','LÍMITE')):
                        print(f'  Fila {num}: {linea[:240]}')
                        coincidencias += 1
                    if coincidencias >= 14:
                        print('  [Vista previa limitada a 14 filas relevantes]')
                        break
            wb.close()


def revisar_codigos_dbf_2017():
    ruta = seleccionar_zip(('2017', 'shp'))
    print('\n=== INE 2017: LONGITUDES ORIGINALES DE CODIGO COMUNA ===')
    with zipfile.ZipFile(ruta) as z:
        nombres = [n for n in z.namelist() if n.lower().endswith('.dbf')]
        if not nombres:
            print('No se encontró archivo DBF.')
            return
        b = z.read(nombres[0])
    total = struct.unpack_from('<I', b, 4)[0]
    tam_encabezado = struct.unpack_from('<H', b, 8)[0]
    tam_registro = struct.unpack_from('<H', b, 10)[0]
    pos = 32
    offset = 1
    campos = {}
    while pos + 32 <= tam_encabezado and b[pos] != 0x0D:
        f = b[pos:pos+32]
        nombre = f[:11].split(b'\x00',1)[0].decode('ascii', 'replace').upper()
        tam = f[16]
        campos[nombre] = (offset, tam)
        offset += tam
        pos += 32
    if 'COMUNA' not in campos:
        print('Campo COMUNA no encontrado. Campos:', list(campos))
        return
    inicio, ancho = campos['COMUNA']
    print('DBF:', nombres[0], '| registros:', total, '| ancho declarado COMUNA:', ancho)
    codigos = []
    for i in range(total):
        p = tam_encabezado + i * tam_registro
        if p + tam_registro > len(b):
            break
        if b[p:p+1] == b'*':
            continue
        valor = b[p+inicio:p+inicio+ancho].decode('ascii', 'replace').strip()
        if valor:
            codigos.append(valor)
    print('Longitudes del código crudo:', dict(sorted(Counter(map(len, codigos)).items())))
    cortos = [c for c in codigos if len(c) != 5]
    print('Ejemplos de códigos de longitud distinta a 5:', cortos[:15])
    print('ATENCIÓN: No se aplican códigos a PostgreSQL ni se rellenan ceros automáticamente.')


if __name__ == '__main__':
    if not INE.is_dir():
        sys.exit(f'No existe la carpeta de datos INE: {INE}')
    try:
        revisar_2024()
        revisar_codigos_dbf_2017()
    except Exception as exc:
        sys.exit(f'Error durante la inspección (sin escrituras): {exc}')
