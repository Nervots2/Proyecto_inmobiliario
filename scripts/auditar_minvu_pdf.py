"""Audita las cinco tablas regionales del informe MINVU (2026).

Solo lectura: NO modifica el PDF, Silo, Delta Lake ni PostgreSQL.
Ejecutar desde cualquier directorio: python scripts/auditar_minvu_pdf.py
Requiere: python -m pip install pdfplumber
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
PDF = ROOT / 'datalake_crudo' / 'minvu' / 'minvu_analisis_suelo_urbano_2026.pdf'
TABLAS = {
    1: (5, 'Crecimiento urbano (2017-2024)'),
    2: (6, 'Suelo urbano dentro/fuera de territorios sanitarios'),
    3: (7, 'Suelo disponible no construido'),
    4: (10, 'Transacciones y valor promedio de suelo'),
    5: (11, 'Transacciones dentro/fuera de territorios sanitarios'),
}
NOMBRES = ('arica', 'tarapaca', 'antofagasta', 'atacama', 'coquimbo',
           'valparaiso', 'metropolitana', 'ohiggins', 'maule', 'nuble',
           'biobio', 'araucania', 'los rios', 'los lagos', 'aysen',
           'magallanes', 'nacional')


def normalizar(valor):
    import unicodedata
    texto = unicodedata.normalize('NFD', str(valor or ''))
    return ''.join(c for c in texto if unicodedata.category(c) != 'Mn').lower()


def compactar_tabla(tabla):
    filas = tabla.extract()
    return [[str(v or '').replace('\n', ' ').strip() for v in f] for f in filas if f]


def puntuar(filas):
    if not filas:
        return 0
    # Preferir la tabla con varias regiones y Nacional, no los encabezados de página.
    texto = ' '.join(' '.join(fila[:2]) for fila in filas)
    limpio = normalizar(texto)
    return sum(nombre in limpio for nombre in NOMBRES)


def buscar_tablas(pagina):
    opciones = [
        dict(vertical_strategy='lines', horizontal_strategy='lines',
             intersection_tolerance=5, snap_tolerance=3),
        dict(vertical_strategy='lines', horizontal_strategy='text',
             intersection_tolerance=7, text_tolerance=3),
    ]
    candidatas = []
    for config in opciones:
        try:
            for tabla in pagina.find_tables(table_settings=config):
                filas = compactar_tabla(tabla)
                if len(filas) >= 5:
                    candidatas.append((puntuar(filas), len(filas), filas))
        except Exception as exc:
            print(f'  Aviso: una estrategia de extracción falló: {exc}')
    return sorted(candidatas, key=lambda x: (x[0], x[1]), reverse=True)


def main():
    if not PDF.is_file():
        sys.exit(f'ERROR: no se encontró el archivo: {PDF}')
    try:
        import pdfplumber
    except ImportError:
        sys.exit('Falta pdfplumber. Instalar: python -m pip install pdfplumber')

    print('=== AUDITORÍA MINVU 2026 - SOLO LECTURA ===')
    print('Archivo:', PDF.name)
    print('Tamaño (MB):', round(PDF.stat().st_size / 1024**2, 2))
    with pdfplumber.open(PDF) as pdf:
        print('Páginas del PDF:', len(pdf.pages))
        for numero, (pagina_pdf, titulo) in TABLAS.items():
            print(f'\nTABLA {numero} - página {pagina_pdf}: {titulo}')
            if pagina_pdf > len(pdf.pages):
                print('  ERROR: página no encontrada')
                continue
            pagina = pdf.pages[pagina_pdf - 1]
            texto = pagina.extract_text() or ''
            print('  Texto seleccionable:', 'sí' if len(texto.strip()) > 40 else 'no / insuficiente')
            candidatas = buscar_tablas(pagina)
            if not candidatas:
                print('  No se detectaron tablas automáticamente. Se requiere ajustar la extracción.')
                continue
            puntos, cantidad, filas = candidatas[0]
            print('  Filas detectadas (incluye encabezados):', cantidad)
            print('  Columnas aproximadas:', max(map(len, filas)))
            print('  Regiones/nombres identificados:', puntos, 'de 17 posibles')
            print('  Primeras 3 filas:')
            for fila in filas[:3]:
                print('   ', ' | '.join(fila)[:220])
            print('  Últimas 2 filas:')
            for fila in filas[-2:]:
                print('   ', ' | '.join(fila)[:220])
            if puntos < 12:
                print('  ATENCIÓN: extracción incompleta o tabla incorrecta. NO cargar automáticamente.')
    print('\nAuditoría completada. No se escribieron archivos ni bases de datos.')


if __name__ == '__main__':
    main()
