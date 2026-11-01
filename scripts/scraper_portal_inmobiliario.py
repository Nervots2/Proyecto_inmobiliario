"""Scraper independiente de Portal Inmobiliario para el proyecto académico.

Guarda nuevas publicaciones de forma incremental y conserva JSON anteriores.
Respeta robots.txt y límites HTTP; detiene la ejecución ante bloqueos.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup


# Configuración de búsquedas y funciones de extracción existentes

OPERACION = "venta"

BUSQUEDAS = [
    {
        "tipo_propiedad": "casa",
        "ubicacion": "metropolitana",
        "modalidad": None,
        "max_paginas": 4
    },
    {
        "tipo_propiedad": "departamento",
        "ubicacion": "metropolitana",
        "modalidad": "propiedades-usadas",
        "max_paginas": 4
    },
    {
        "tipo_propiedad": "oficina",
        "ubicacion": "metropolitana",
        "modalidad": None,
        "max_paginas": 4
    },
    {
        "tipo_propiedad": "industrial",
        "ubicacion": "metropolitana",
        "modalidad": None,
        "max_paginas": 4
    },
    {
        "tipo_propiedad": "comercial",
        "ubicacion": "metropolitana",
        "modalidad": None,
        "max_paginas": 4
    },
    {
        "tipo_propiedad": "sitio",
        "ubicacion": "metropolitana",
        "modalidad": None,
        "max_paginas": 4
    },
    {
        "tipo_propiedad": "casa",
        "ubicacion": "biobio",
        "modalidad": None,
        "max_paginas": 4
    },
    {
        "tipo_propiedad": "casa",
        "ubicacion": "antofagasta",
        "modalidad": None,
        "max_paginas": 2
    },
    {
        "tipo_propiedad": "casa",
        "ubicacion": "maule",
        "modalidad": None,
        "max_paginas": 2
    },
    {
        "tipo_propiedad": "casa",
        "ubicacion": "la-araucania",
        "modalidad": None,
        "max_paginas": 2
    }
]

AGENTE = "UTFSM-ProyectoAcademico"

RUTA_PROYECTO = Path(__file__).resolve().parent.parent

RUTA_DATOS = (
    RUTA_PROYECTO
    / "datalake_crudo"
    / "portal_inmobiliario"
)

URL_BASE = "https://www.portalinmobiliario.com"

def construir_url(
    pagina,
    tipo_propiedad,
    ubicacion,
    modalidad=None
):

    if pagina < 1:
        raise ValueError(
            "El número de página debe ser mayor o igual a 1."
        )

    # Componentes de la búsqueda
    componentes = [
        OPERACION,
        tipo_propiedad,
        modalidad,
        ubicacion
    ]

    # Eliminar parámetros opcionales sin valor
    componentes = [
        componente
        for componente in componentes
        if componente is not None
    ]

    ruta = f"{URL_BASE}/{'/'.join(componentes)}"

    if pagina == 1:
        return f"{ruta}/_FULL_true"

    inicio = (pagina - 1) * 48 + 1

    return (
        f"{ruta}/_Desde_{inicio}"
        "_FULL_true_NoIndex_True"
    )

def extraer_texto(elemento, selector):

    encontrado = elemento.select_one(selector)

    if encontrado:
        return encontrado.get_text(" ", strip=True)

    return None

def extraer_publicaciones(
    tarjetas,
    numero_pagina,
    tipo_propiedad,
    ubicacion_busqueda,
    modalidad=None
):

    publicaciones = {}

    fecha = datetime.now(timezone.utc).isoformat()

    for tarjeta in tarjetas:

        # Identificar el enlace de la publicación
        enlace = tarjeta.select_one(
            "a.poly-component__title[href*='MLC-']"
        )

        if enlace is None:
            continue

        url = enlace.get("href", "").split("#")[0]

        # Extraer el identificador único
        coincidencia = re.search(r"MLC-(\d+)", url)

        if coincidencia is None:
            continue

        id_publicacion = coincidencia.group(1)

        # Extraer las características del inmueble
        atributos = [
            elemento.get_text(" ", strip=True)
            for elemento in tarjeta.select(
                ".poly-attributes_list__item"
            )
        ]

        # Obtener el texto completo de la tarjeta
        texto_tarjeta = tarjeta.get_text(" | ", strip=True)

        # Separar las etiquetas del anuncio
        segmentos = [
            segmento.strip().casefold()
            for segmento in texto_tarjeta.split("|")
        ]

        # Identificar proyectos inmobiliarios
        es_proyecto = "proyecto" in segmentos

        # Extraer el título de la publicación
        titulo = extraer_texto(
            tarjeta,
            ".poly-component__title"
)

        # Identificar precios iniciales
        precio_desde = (
            "desde" in segmentos
            or bool(
                re.search(
                    r"\bdesde\b",
                    titulo or "",
                    flags=re.IGNORECASE
                )
            )
        )

        # Buscar la cantidad de unidades disponibles
        coincidencia_unidades = re.search(
            r"\b(\d+)\s+unidades\s+disponibles\b",
            texto_tarjeta,
            flags=re.IGNORECASE
        )

        unidades_disponibles = (
            int(coincidencia_unidades.group(1))
            if coincidencia_unidades
            else None
        )

        # Construir el registro estructurado
        publicaciones[id_publicacion] = {
            "id": id_publicacion,
            "titulo": titulo,
            "precio_actual": extraer_texto(
                tarjeta, ".poly-price__current .poly-price__amount"
            ),
            "precio_anterior": extraer_texto(
                tarjeta, ".poly-price__previous"
            ),
            "ubicacion": extraer_texto(
                tarjeta, ".poly-component__location"
            ),
            "atributos": atributos,
            "tipo_anuncio": (
                "proyecto" if es_proyecto else "no_identificado"
            ),
            "precio_es_desde": precio_desde,
            "unidades_disponibles": unidades_disponibles,
            "tipo_propiedad_busqueda": tipo_propiedad,
            "operacion_busqueda": OPERACION,
            "modalidad_busqueda": modalidad,
            "ubicacion_busqueda": ubicacion_busqueda,
            "pagina_resultados": numero_pagina,
            "url": url,
            "fecha_extraccion": fecha
        }

    return list(publicaciones.values())

# Control incremental de solicitudes y almacenamiento
TIMEOUT = 25
MAX_REDIRECCIONES = 4


def indice_existentes(carpeta: Path) -> set[str]:
    """Lee IDs ya guardados en JSON, sin modificar ningún archivo."""
    ids = set()
    if not carpeta.exists():
        return ids
    archivos = sorted(carpeta.glob('*.json'))
    for ruta in archivos:
        try:
            with ruta.open(encoding='utf-8') as archivo:
                datos = json.load(archivo)
            if not isinstance(datos, list):
                continue
            for fila in datos:
                if isinstance(fila, dict):
                    identificador = fila.get('id') or fila.get('id_publicacion')
                    if identificador is not None:
                        ids.add(str(identificador))
        except (OSError, ValueError, TypeError) as error:
            print(f'ADVERTENCIA: no se pudo leer {ruta.name}: {error}')
    print(f'Publicaciones históricas identificadas: {len(ids)}')
    return ids


def guardar_atomico(ruta: Path, filas: dict[str, dict]) -> None:
    """Guardado temporal + reemplazo, para no dejar JSON truncados."""
    temporal = ruta.with_suffix('.json.tmp')
    with temporal.open('w', encoding='utf-8') as archivo:
        json.dump(list(filas.values()), archivo, ensure_ascii=False, indent=2)
    temporal.replace(ruta)


def dominio_permitido(url: str) -> bool:
    """Evita seguir redirecciones a dominios ajenos al sitio."""
    host = (urlparse(url).hostname or '').lower()
    return host == 'portalinmobiliario.com' or host.endswith('.portalinmobiliario.com')


class AccesoResponsable:
    def __init__(self, pausa: float) -> None:
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': AGENTE, 'Accept-Language': 'es-CL,es;q=0.8'})
        self.pausa = max(5.0, pausa)
        self.robots: dict[str, RobotFileParser | None] = {}
        self.ultima_solicitud: float | None = None

    def comprobar_robots(self, url: str) -> tuple[bool, float]:
        if not dominio_permitido(url):
            print(f'URL ajena al portal: {url}. No se consultará.')
            return False, self.pausa

        parsed = urlparse(url)
        raiz = f'{parsed.scheme}://{parsed.netloc}'
        if raiz not in self.robots:
            robots_url = f'{raiz}/robots.txt'
            try:
                respuesta = self.session.get(robots_url, timeout=TIMEOUT, allow_redirects=False)
                respuesta.raise_for_status()
                if respuesta.status_code in (301, 302, 307, 308):
                    raise ValueError('robots.txt redirigido; verificarlo manualmente')
                parser = RobotFileParser()
                parser.parse(respuesta.text.splitlines())
                self.robots[raiz] = parser
            except (requests.RequestException, ValueError) as error:
                print(f'No se pudo verificar {robots_url}: {error}. Se omite el dominio.')
                self.robots[raiz] = None

        reglas = self.robots[raiz]
        if reglas is None:
            return False, self.pausa
        autorizado = reglas.can_fetch(AGENTE, url)
        demora = reglas.crawl_delay(AGENTE) or reglas.crawl_delay('*') or 0
        return autorizado, max(self.pausa, float(demora))

    def leer_html(self, url: str) -> str | None:
        for paso in range(MAX_REDIRECCIONES + 1):
            autorizado, pausa = self.comprobar_robots(url)
            if not autorizado:
                print(f'OMITIDO por robots.txt / dominio: {url}')
                return None
            if self.ultima_solicitud is not None:
                espera = pausa - (time.monotonic() - self.ultima_solicitud)
                if espera > 0:
                    time.sleep(espera)
            try:
                respuesta = self.session.get(url, timeout=TIMEOUT, allow_redirects=False)
            except requests.RequestException as error:
                print(f'Error de red: {error}. Se detiene esta búsqueda.')
                return None
            finally:
                self.ultima_solicitud = time.monotonic()

            if respuesta.status_code in (301, 302, 303, 307, 308):
                destino = urljoin(url, respuesta.headers.get('Location', ''))
                if not destino or destino == url:
                    print('Redirección inválida o circular.')
                    return None
                url = destino
                continue
            if respuesta.status_code in (403, 429):
                print(f'Acceso restringido HTTP {respuesta.status_code}. Se detiene para respetar el sitio.')
                raise PermissionError(f'HTTP {respuesta.status_code} en {url}')
            if respuesta.status_code != 200:
                print(f'HTTP {respuesta.status_code}; se detiene esta búsqueda.')
                return None
            if 'html' not in respuesta.headers.get('Content-Type', '').lower():
                print('Respuesta sin contenido HTML; se detiene esta búsqueda.')
                return None
            return respuesta.text

        print('Demasiadas redirecciones. Se omite la página.')
        return None


def ejecutar(max_paginas: int, pausa: float, solo_rm: bool, max_busquedas: int | None) -> int:
    if not URL_BASE.startswith('https://'):
        raise RuntimeError('URL_BASE debe ser HTTPS.')

    busquedas = [b for b in BUSQUEDAS if not solo_rm or b['ubicacion'] == 'metropolitana']
    if max_busquedas is not None:
        busquedas = busquedas[:max_busquedas]

    RUTA_DATOS.mkdir(parents=True, exist_ok=True)
    ids_vistos = indice_existentes(RUTA_DATOS)
    acceso = AccesoResponsable(pausa)
    marca = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%SZ')
    total_nuevos = 0

    print(f'Búsquedas configuradas: {len(busquedas)}')
    print('Los JSON existentes y el Data Warehouse permanecerán intactos.')
    print('Ctrl+C detiene el proceso; los avances se guardan después de cada página.')

    for numero_busqueda, busqueda in enumerate(busquedas, start=1):
        tipo = busqueda['tipo_propiedad']
        ubicacion = busqueda['ubicacion']
        modalidad = busqueda.get('modalidad')
        nombre_modalidad = modalidad or 'sin_modalidad'
        nombre = f'portal_ampliado_venta_{tipo}_{ubicacion}_{nombre_modalidad}_{marca}.json'
        ruta = RUTA_DATOS / re.sub(r'[^a-zA-Z0-9_.-]', '_', nombre)
        registros: dict[str, dict] = {}
        pagina_limite = max_paginas

        print(f'\n[{numero_busqueda}/{len(busquedas)}] {tipo} · {ubicacion} · {nombre_modalidad}')
        paginas_repetidas: set[tuple[str, ...]] = set()

        for pagina in range(1, pagina_limite + 1):
            url = construir_url(pagina, tipo, ubicacion, modalidad)
            try:
                html = acceso.leer_html(url)
            except PermissionError:
                # Restricciones de acceso: terminar la ejecución completa.
                print('Ejecución detenida por restricción de acceso.')
                print(f'Nuevos registros guardados hasta ahora: {total_nuevos}')
                return 2
            if html is None:
                break

            tarjetas = BeautifulSoup(html, 'html.parser').select('li.ui-search-layout__item')
            if not tarjetas:
                print('Sin tarjetas: puede no haber resultados o haber cambiado el HTML. Se detiene la búsqueda.')
                break
            filas = extraer_publicaciones(tarjetas, pagina, tipo, ubicacion, modalidad)
            if not filas:
                print('No se pudieron extraer publicaciones válidas. Se detiene la búsqueda.')
                break
            firma = tuple(str(f['id']) for f in filas if 'id' in f)
            if firma in paginas_repetidas:
                print('La página repite publicaciones de otra página; se detiene esta búsqueda.')
                break
            paginas_repetidas.add(firma)

            nuevas_pagina = 0
            for fila in filas:
                identificador = str(fila.get('id', ''))
                if identificador and identificador not in ids_vistos:
                    ids_vistos.add(identificador)
                    registros[identificador] = fila
                    nuevas_pagina += 1
                    total_nuevos += 1

            if registros:
                guardar_atomico(ruta, registros)
            print(f'Página {pagina}/{pagina_limite}: {len(filas)} extraídas, '
                  f'{nuevas_pagina} nuevas; total de esta búsqueda: {len(registros)}')

        print(f'Guardadas {len(registros)} nuevas publicaciones en: {ruta.name}' if registros
              else 'Sin publicaciones nuevas en esta búsqueda; no se generó archivo.')

    print(f'\nFIN. Nuevas publicaciones guardadas: {total_nuevos}.')
    print('Nota: no ejecutar todavía cargar_dw_publicaciones.py con estos lotes; requiere adaptación.')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='Scraper incremental y responsable del Portal Inmobiliario')
    parser.add_argument('--max-paginas', type=int, default=12,
                        help='Máximo de páginas por búsqueda (predeterminado: 12)')
    parser.add_argument('--pausa', type=float, default=8.0,
                        help='Segundos entre solicitudes, mínimo 5 (predeterminado: 8)')
    parser.add_argument('--solo-rm', action='store_true',
                        help='Solo búsquedas de la Región Metropolitana')
    parser.add_argument('--max-busquedas', type=int, default=None,
                        help='Limitar el número de búsquedas para una prueba inicial')
    argumentos = parser.parse_args()
    if argumentos.max_paginas < 1 or (argumentos.max_busquedas is not None and argumentos.max_busquedas < 1):
        parser.error('Los límites deben ser mayores o iguales a 1.')
    try:
        return ejecutar(argumentos.max_paginas, argumentos.pausa,
                        argumentos.solo_rm, argumentos.max_busquedas)
    except KeyboardInterrupt:
        print('\nDetenido por el usuario. Los archivos ya guardados se conservan.')
        return 130


if __name__ == '__main__':
    sys.exit(main())
