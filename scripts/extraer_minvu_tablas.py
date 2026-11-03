"""Extrae las cinco tablas regionales del DT N°10 (MINVU, junio de 2026).

Uso (desde la raíz de Proyecto_inmobiliario):
    python scripts/extraer_minvu_tablas.py

Requiere: python -m pip install pdfplumber

- Lee exclusivamente el PDF original en datalake_crudo/minvu.
- Usa los límites de las tablas PDF (sin OCR).
- Exige exactamente 16 regiones y el total Nacional, con controles de
  columnas, campos numéricos y totales publicados.
- Si falla una tabla, NO escribe CSV parciales.
- No conecta ni modifica PostgreSQL, Trino o Silo.
"""
from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parents[1]
PDF = ROOT / 'datalake_crudo' / 'minvu' / 'minvu_analisis_suelo_urbano_2026.pdf'
DEST = ROOT / 'datalake_procesado' / 'silver' / 'minvu'

# Códigos regionales CUT (no deben confundirse con códigos comunales).
REGIONES = {
    'arica y parinacota': 15,
    'tarapaca': 1,
    'antofagasta': 2,
    'atacama': 3,
    'coquimbo': 4,
    'valparaiso': 5,
    'metropolitana': 13,
    'ohiggins': 6,
    'maule': 7,
    'nuble': 16,
    'biobio': 8,
    'la araucania': 9,
    'los rios': 14,
    'los lagos': 10,
    'aysen': 11,
    'magallanes': 12,
    'nacional': None,
}

@dataclass(frozen=True)
class Tabla:
    numero: int
    pagina: int  # 1-indexada
    nombre: str
    campos: tuple[str, ...]  # los numéricos después del nombre de región
    total_nacional: tuple[float, ...]
    periodo: str

TABLAS = (
    Tabla(1, 5, 'crecimiento_urbano_2017_2024',
          ('superficie_2017_ha', 'superficie_2024_ha', 'crecimiento_bruto_ha',
           'crecimiento_bruto_pct', 'crecimiento_dentro_limite_urbano_ha',
           'crecimiento_dentro_limite_urbano_pct', 'crecimiento_fuera_limite_urbano_ha',
           'crecimiento_fuera_limite_urbano_pct'),
          (186152.6, 212715.2, 26562.5, 14.3, 16137.0, 60.8, 10472.4, 39.4),
          '2017-2024'),
    Tabla(2, 6, 'superficie_urbana_territorios_sanitarios',
          ('area_urbana_normada_ha', 'area_dentro_to_ha', 'area_fuera_to_ha',
           'area_dentro_to_pct', 'area_fuera_to_pct'),
          (749674.0, 276859.37, 472204.0, 37.0, 63.0),
          'cobertura_sanitaria_2025'),
    Tabla(3, 7, 'suelo_urbano_disponible',
          ('suelo_disponible_total_ha', 'suelo_disponible_dentro_to_ha',
           'suelo_disponible_fuera_to_ha', 'suelo_disponible_dentro_to_pct',
           'suelo_disponible_fuera_to_pct'),
          (487455.89, 64964.42, 422491.47, 13.0, 87.0),
          'cobertura_sanitaria_2025'),
    Tabla(4, 10, 'transacciones_valor_suelo',
          ('transacciones_cantidad', 'valor_promedio_uf_m2'),
          (26729.0, 1.8), '2020-2025'),
    Tabla(5, 11, 'transacciones_suelo_territorios_sanitarios',
          ('transacciones_fuera_to_cantidad', 'valor_promedio_fuera_to_uf_m2',
           'transacciones_dentro_to_cantidad', 'valor_promedio_dentro_to_uf_m2'),
          (17778.0, 1.0, 8951.0, 3.4), '2020-2025'),
)


def normalizar(texto: object) -> str:
    s = str(texto or '').strip().replace('\n', ' ')
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.lower().replace('’', "'").replace('`', "'")
    s = s.replace("o'higgins", 'ohiggins').replace('o higgins', 'ohiggins')
    s = re.sub(r'[^a-z0-9 ]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def numero_es(texto: object) -> float:
    """Convierte números del informe en formato CL (1.234,56 / 14,3%)."""
    if texto is None:
        raise ValueError('Celda numérica vacía')
    s = str(texto).replace('\n', '').replace(' ', '').replace('\u00a0', '')
    s = s.replace('%', '').replace('*', '').strip()
    if not s:
        raise ValueError('Celda numérica vacía')
    s = s.replace('.', '').replace(',', '.')
    if not re.fullmatch(r'[-+]?\d+(?:\.\d+)?', s):
        raise ValueError(f'Número inválido: {texto!r}')
    return float(s)


def tablas_candidatas(page, limite_inferior=.74):
    """Usa los trazos de la cuadrícula, no el texto suelto de encabezados."""
    # Recorte configurable para tablas que llegan más abajo en la página.
    zona = page.crop((0, page.height * .065, page.width, page.height * limite_inferior))
    ajustes = (
        {'vertical_strategy': 'lines', 'horizontal_strategy': 'lines',
         'snap_tolerance': 3, 'join_tolerance': 3,
         'intersection_tolerance': 5, 'edge_min_length': 5},
        {'vertical_strategy': 'lines_strict', 'horizontal_strategy': 'lines_strict',
         'snap_tolerance': 4, 'join_tolerance': 4,
         'intersection_tolerance': 6, 'edge_min_length': 5},
        {'vertical_strategy': 'lines', 'horizontal_strategy': 'lines',
         'snap_tolerance': 5, 'join_tolerance': 5,
         'intersection_tolerance': 8, 'edge_min_length': 4},
    )
    for cfg in ajustes:
        for table in zona.find_tables(table_settings=cfg):
            yield table.extract(), cfg



# La fila "Arica y Parinacota" se representa en dos líneas en las tablas
# originales. Algunos ajustes de pdfplumber la fragmentan o la dejan fuera de
# la cuadrícula reconstruida. SOLO se rescata con los valores legibles del PDF.
PATRON_NUMERO = re.compile(
    r"(?<![\w])(?:\d{1,3}(?:\.\d{3})+|\d+)(?:,\d+)?%?(?![\w])"
)


def _numeros_de_celdas(celdas, cantidad):
    """Interpreta una fila solo si contiene todos los indicadores esperados."""
    if len(celdas) != cantidad or any(x is None or not str(x).strip() for x in celdas):
        return None
    try:
        return tuple(numero_es(c) for c in celdas)
    except ValueError:
        return None


def _rescatar_arica_de_matriz(matriz, cantidad):
    """Reconstruye una fila repartida en dos renglones de la cuadrícula.

    La etiqueta puede ocupar dos renglones y los números estar en uno u otro.
    No completa cifras ausentes ni acepta dos valores distintos para una columna.
    """
    hallazgos = []
    for i, raw in enumerate(matriz):
        if not raw or not raw[0] or 'arica' not in normalizar(raw[0]):
            continue
        for num_filas in (1, 2, 3):
            grupo = [list(r) for r in matriz[i:i + num_filas] if r]
            if len(grupo) != num_filas:
                continue
            nombre = normalizar(' '.join(str(r[0] or '') for r in grupo))
            if 'arica y parinacota' not in nombre:
                continue
            if any(len(r) != cantidad + 1 for r in grupo):
                continue
            tokens = []
            for columna in range(1, cantidad + 1):
                opciones = [str(r[columna]).strip() for r in grupo
                            if r[columna] is not None and str(r[columna]).strip()]
                if len(set(opciones)) != 1:
                    break  # columna ausente o discrepancia: no reconstruir
                tokens.append(opciones[0])
            if len(tokens) == cantidad:
                nums = _numeros_de_celdas(tokens, cantidad)
                if nums is not None:
                    hallazgos.append(nums)
    distintos = list(dict.fromkeys(hallazgos))
    return distintos[0] if len(distintos) == 1 else None


def _rescatar_arica_del_texto(page, cantidad):
    """Junta ambas líneas de la primera fila y captura únicamente sus números.

    En el PDF original suele ser:
      Arica y 2.958,7 3.225,9 ... 75,9%
      Parinacota
    y el antiguo extractor únicamente contemplaba el orden inverso.
    """
    zona = page.crop((0, page.height * .08, page.width, page.height * .75))
    texto = zona.extract_text(x_tolerance=2, y_tolerance=3) or ''
    lineas = [re.sub(r'\s+', ' ', linea).strip() for linea in texto.splitlines()]
    hallazgos = []
    for i, linea in enumerate(lineas):
        if not re.match(r'^arica\s+y\b', linea, re.IGNORECASE):
            continue
        # Abarca la etiqueta en el mismo renglón o en los 1-2 siguientes;
        # recoge datos que la disposición visual ubicó en cualquiera de ellos.
        for longitud in (1, 2, 3):
            grupo = lineas[i:i + longitud]
            if len(grupo) != longitud:
                continue
            conjunto = ' '.join(grupo)
            if not re.search(r'\barica\s+y\s+parinacota\b', conjunto,
                             flags=re.IGNORECASE):
                # En el PDF original los números pueden estar entre ambos nombres.
                if not (re.match(r'^arica\s+y\b', grupo[0], re.I) and
                        any(re.match(r'^parinacota\b', l, re.I) for l in grupo[1:])):
                    continue
            # Se eliminan SOLO los fragmentos de nombre que abren las líneas.
            conjunto = ' '.join(
                re.sub(r'^\s*(?:arica\s+y|parinacota)\b', '', l, flags=re.I)
                for l in grupo
            )
            tokens = PATRON_NUMERO.findall(conjunto)
            if len(tokens) != cantidad:
                continue
            nums = _numeros_de_celdas(tokens, cantidad)
            if nums is not None:
                hallazgos.append(nums)
    distintos = list(dict.fromkeys(hallazgos))
    return distintos[0] if len(distintos) == 1 else None


def _rescatar_arica_por_coordenadas(page, cantidad):
    """Último recurso: usa posiciones X/Y de la primera fila real del PDF."""
    words = page.extract_words(x_tolerance=2, y_tolerance=2) or []
    if not words:
        return None
    hallazgos = []
    for word in words:
        if normalizar(word.get('text')) != 'arica':
            continue
        y0 = float(word['top'])
        # Una fila de la tabla no puede exceder ~38 pt de alto.
        siguientes = [w for w in words if
                      8 < float(w['top']) - y0 < 55 and
                      normalizar(w.get('text')) == 'tarapaca']
        if not siguientes:
            continue
        y1 = min(float(w['top']) for w in siguientes)
        fila = [w for w in words if y0 - 4 <= float(w['top']) < y1 - 2]
        nombres = ' '.join(str(w['text']) for w in fila)
        if 'parinacota' not in normalizar(nombres):
            continue
        # En una fila de tabla cada valor se ubica en una columna, de izquierda
        # a derecha. Se descartan las palabras de la primera columna (región).
        numericos = [w for w in fila
                     if PATRON_NUMERO.fullmatch(str(w['text']).strip())]
        numericos.sort(key=lambda w: float(w['x0']))
        tokens = [w['text'] for w in numericos]
        if len(tokens) == cantidad:
            nums = _numeros_de_celdas(tokens, cantidad)
            if nums is not None:
                hallazgos.append(nums)
    distintos = list(dict.fromkeys(hallazgos))
    return distintos[0] if len(distintos) == 1 else None


def diagnostico_arica(page, matriz, cantidad):
    """Evidencia mínima cuando el PDF no permite recuperar la fila."""
    print('  DIAGNÓSTICO Arica: filas iniciales de la cuadrícula:')
    for row in matriz[:5]:
        print('   ', repr(row)[:280])
    texto = page.extract_text(x_tolerance=2, y_tolerance=3) or ''
    lineas = texto.splitlines()
    for i, line in enumerate(lineas):
        if re.search(r'\b(?:Arica|Parinacota)\b', line, re.I):
            print('  DIAGNÓSTICO texto:', repr(line)[:280])
            if i + 1 < len(lineas):
                print('  SIGUIENTE:', repr(lineas[i + 1])[:280])
            break


def procesar_candidato(matriz, definicion, page=None):
    regiones = {}
    errores = []
    ncolumnas = len(definicion.campos) + 1
    for raw in matriz:
        if not raw:
            continue
        values = list(raw)
        if not values:
            continue
        nombre = normalizar(values[0])
        if nombre not in REGIONES:
            continue  # encabezados y notas al pie
        if len(values) != ncolumnas:
            errores.append(f'{nombre}: {len(values)} columnas; se esperan {ncolumnas}')
            continue
        try:
            nums = tuple(numero_es(v) for v in values[1:])
        except ValueError as exc:
            errores.append(f'{nombre}: {exc}')
            continue
        if nombre in regiones:
            errores.append(f'Nombre duplicado: {nombre}')
            continue
        regiones[nombre] = nums

    # Rescate controlado de Arica y Parinacota, que aparece en dos líneas.
    if 'arica y parinacota' not in regiones:
        recuperado = _rescatar_arica_de_matriz(matriz, len(definicion.campos))
        if recuperado is None and page is not None:
            recuperado = _rescatar_arica_del_texto(page, len(definicion.campos))
        if recuperado is None and page is not None:
            recuperado = _rescatar_arica_por_coordenadas(page, len(definicion.campos))
        if recuperado is not None:
            # La fila solo se acepta si coinciden exactamente sus 8 valores
            # con los publicados en la Tabla 1 original del MINVU.
            if definicion.numero == 1:
                esperado_arica = (2958.7, 3225.9, 267.3, 9.0,
                                  64.3, 24.1, 202.9, 75.9)
                if any(abs(a - b) > 0.02 for a, b in
                       zip(recuperado, esperado_arica)):
                    raise RuntimeError('Los valores recuperados de Arica y Parinacota '
                                       'no coinciden con la Tabla 1 del MINVU: '
                                       + str(recuperado))
            regiones['arica y parinacota'] = recuperado
            errores = [e for e in errores if not
                       e.startswith('arica y parinacota:')]
            print(f'  Tabla {definicion.numero}: Arica y Parinacota recuperada '
                  'con todos sus valores del documento.')

    # Acepta la tabla solo si el PDF presenta las 16 regiones + TOTAL nacional.
    faltan = sorted(set(REGIONES) - set(regiones))
    if faltan:
        errores.append('Faltan: ' + ', '.join(faltan))
    if len(regiones) == 17 and not errores:
        nacional = regiones['nacional']
        for campo, valor, esperado in zip(definicion.campos, nacional, definicion.total_nacional):
            # Tolerancia 0,02 por redondeo/representación de céntimos y hectáreas.
            if abs(valor - esperado) > 0.02:
                errores.append(f'Total nacional {campo}: {valor}, se espera {esperado}')

    return regiones, errores


def _filas_desde_texto_completo(page, definicion):
    """Recupera filas de tablas 4/5 sin depender de cuadrículas cortadas.

    Requiere el nombre regional y TODOS los indicadores en el mismo renglón
    extraído. La única excepción es «Arica y Parinacota», que puede ocupar
    dos líneas. No inventa valores ausentes ni usa tablas externas.
    """
    cantidad = len(definicion.campos)
    candidatos_validos = []
    diagnosticos = []
    opciones = (
        {'x_tolerance': 2, 'y_tolerance': 3},
        {'x_tolerance': 1, 'y_tolerance': 5},
        {'x_tolerance': 3, 'y_tolerance': 2},
    )
    for opciones_texto in opciones:
        texto = page.extract_text(**opciones_texto) or ''
        lineas = [re.sub(r'\s+', ' ', l).strip() for l in texto.splitlines()]
        regiones = {}
        duplicados = []
        for i, linea in enumerate(lineas):
            variantes = [linea]
            if re.match(r'^arica\s+y\b', linea, re.IGNORECASE) and i+1 < len(lineas):
                # Las cifras pueden estar antes o después de «Parinacota».
                if re.match(r'^parinacota\b', lineas[i+1], re.IGNORECASE):
                    variantes.append(linea + ' ' + lineas[i+1])
            if re.match(r'^parinacota\b', linea, re.IGNORECASE) and i > 0:
                if re.match(r'^arica\s+y\b', lineas[i-1], re.IGNORECASE):
                    variantes.append(lineas[i-1] + ' ' + linea)

            for candidato in variantes:
                matches = list(PATRON_NUMERO.finditer(candidato))
                if len(matches) != cantidad:
                    continue
                nombre = normalizar(candidato[:matches[0].start()])
                # Si «Parinacota» quedó al final, queda después de las cifras.
                cola = normalizar(candidato[matches[-1].end():])
                if nombre == 'arica y' and cola == 'parinacota':
                    nombre = 'arica y parinacota'
                    cola = ''
                if nombre not in REGIONES or cola:
                    continue
                numeros = _numeros_de_celdas(
                    [m.group(0) for m in matches], cantidad
                )
                if numeros is None:
                    continue
                if nombre in regiones and regiones[nombre] != numeros:
                    duplicados.append((nombre, regiones[nombre], numeros))
                else:
                    regiones[nombre] = numeros
                break

        faltantes = sorted(set(REGIONES).difference(regiones))
        if not faltantes and not duplicados:
            nacional = regiones['nacional']
            if all(abs(x - y) <= 0.02 for x, y in
                   zip(nacional, definicion.total_nacional)):
                candidatos_validos.append(regiones)
                continue
        diagnosticos.append((faltantes, duplicados, lineas))

    # Si hay extracciones alternativas incompatibles, detener y reportar.
    if candidatos_validos:
        firmas = {tuple((r, m[r]) for r in REGIONES) for m in candidatos_validos}
        if len(firmas) == 1:
            return candidatos_validos[0]
        raise RuntimeError(f'Tabla {definicion.numero}: distintas lecturas de texto '
                           'produjeron resultados contradictorios; no se escriben CSV.')
    if diagnosticos:
        faltantes, duplicados, lineas = min(diagnosticos, key=lambda d: len(d[0]))
        print(f'  DIAGNÓSTICO texto Tabla {definicion.numero}: '
              f'faltan {faltantes}; duplicados contradictorios: {duplicados[:2]}')
        for linea in lineas:
            # Muestra solamente la evidencia relevante, sin inundar la terminal.
            if any(normalizar(linea).startswith(x) for x in
                   ('arica', 'parinacota', 'magallanes', 'nacional')):
                print('    ', repr(linea)[:200])
    return None


def _filas_formateadas(regiones, definicion):
    filas = []
    for nombre, codigo_region in REGIONES.items():
        row = {
            'fuente': 'MINVU_DT10_2026',
            'tabla': definicion.numero,
            'pagina_pdf': definicion.pagina,
            'periodo_referencia': definicion.periodo,
            'nivel_geografico': 'nacional' if nombre == 'nacional' else 'regional',
            'codigo_region': '' if codigo_region is None else codigo_region,
            'region': nombre,
        }
        row.update(dict(zip(definicion.campos, regiones[nombre])))
        filas.append(row)
    return filas


def extraer_tabla(page, definicion):
    hallazgos = []
    # Las tablas 4 y 5 están en páginas donde la cuadrícula puede terminar
    # más abajo del 74 % de la página. Preferimos el texto de la página entera.
    if definicion.numero in (4, 5):
        recuperadas = _filas_desde_texto_completo(page, definicion)
        if recuperadas is not None:
            print(f'  Tabla {definicion.numero}: 17 filas extraídas del texto '
                  'completo y validadas contra el total nacional.')
            return _filas_formateadas(recuperadas, definicion)
    for matriz, _ in tablas_candidatas(page, .93 if definicion.numero in (4, 5) else .74):
        regiones, errores = procesar_candidato(matriz, definicion, page=page)
        hallazgos.append((len(regiones), regiones, errores))
        if len(regiones) == 17 and not errores:
            return _filas_formateadas(regiones, definicion)

    resumen = '; '.join(f'{n}/17 coincidencias ({"; ".join(errs[:2])})'
                       for n, _, errs in sorted(hallazgos, key=lambda x: -x[0])[:3])
    if hallazgos and max(n for n, _, _ in hallazgos) == 16:
        # Evita otro ciclo de cambios sin evidencia del texto del PDF.
        for matriz, _ in tablas_candidatas(page, .93 if definicion.numero in (4, 5) else .74):
            diagnostico_arica(page, matriz, len(definicion.campos))
            break
    raise RuntimeError(
        f'Tabla {definicion.numero} (página {definicion.pagina}): no se pudo '
        f'reconstruir con todas las regiones y totales. '
        f'Candidatas: {resumen or "no se detectó cuadrícula"}. '
        'NO se generó ningún CSV. Envíame este mensaje para ajustar el extractor.'
    )


def main():
    if not PDF.is_file():
        raise FileNotFoundError(f'No se encontró el PDF original en: {PDF}')
    resultados = []
    with pdfplumber.open(PDF) as pdf:
        if len(pdf.pages) < 11:
            raise RuntimeError(f'El PDF tiene {len(pdf.pages)} páginas; se esperaban al menos 11')
        for defn in TABLAS:
            rows = extraer_tabla(pdf.pages[defn.pagina-1], defn)
            resultados.append((defn, rows))
            print(f'OK tabla {defn.numero} (pág. {defn.pagina}): '
                  f'{len(rows)-1} regiones y 1 total nacional; '
                  f'{len(defn.campos)} indicadores')

    # Consistencia transversal: tabla 4 = transacciones fuera+dentro TO (tabla 5).
    tabla4 = {r['region']: r for r in resultados[3][1]}
    tabla5 = {r['region']: r for r in resultados[4][1]}
    for nombre in REGIONES:
        cantidad_total = tabla4[nombre]['transacciones_cantidad']
        suma = (tabla5[nombre]['transacciones_fuera_to_cantidad'] +
                tabla5[nombre]['transacciones_dentro_to_cantidad'])
        if abs(cantidad_total - suma) > 0.01:
            raise RuntimeError(
                f'Inconsistencia entre tablas 4 y 5 para {nombre}: '
                f'{cantidad_total} vs {suma}. No se escriben CSV.'
            )
    print('OK coherencia: transacciones por región de la tabla 4 = fuera + dentro de la tabla 5')

    # Solo después de validar las CINCO tablas se escriben los cinco CSV.
    DEST.mkdir(parents=True, exist_ok=True)
    for defn, rows in resultados:
        path = DEST / f'minvu_2026_t{defn.numero}_{defn.nombre}.csv'
        tmp = path.with_suffix('.csv.tmp')
        headers = list(rows[0])
        with tmp.open('w', encoding='utf-8-sig', newline='') as file:
            wr = csv.DictWriter(file, fieldnames=headers, delimiter=';')
            wr.writeheader()
            wr.writerows(rows)
        tmp.replace(path)
        print(f'CSV generado: {path.relative_to(ROOT)}')
    print('\nVALIDACIÓN MINVU COMPLETA: 5 CSV, 17 filas cada uno (16 regiones + Nacional).')
    print('No se modificaron PostgreSQL, Delta Lake, Silo ni el PDF original.')
    print('Advertencia: la fila Nacional es un agregado, no debe sumarse con las regiones.')


if __name__ == '__main__':
    main()
