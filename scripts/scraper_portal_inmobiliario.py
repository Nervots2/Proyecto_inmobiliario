"""Scraper incremental SOLO de casas y departamentos en venta en la RM.

Consulta dos rutas regionales, sin búsquedas por comuna. Conserva archivos
anteriores y se detiene frente a robots.txt, errores de acceso y bloqueos.
Meta de 20.000 anuncios: aspiracional, no garantiza cobertura del portal.
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

# Dos búsquedas regionales; no se divide por comuna.
BUSQUEDAS = (
    {"tipo_propiedad": "casa", "ubicacion": "metropolitana", "modalidad": None},
    {"tipo_propiedad": "departamento", "ubicacion": "metropolitana", "modalidad": None},
)

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


def normalizar_id(valor: object) -> str | None:
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto or texto.lower() in {"nan", "none", "null"}:
        return None
    coincidencia = re.search(r"MLC[-_ ]?(\d+)", texto, flags=re.IGNORECASE)
    return coincidencia.group(1) if coincidencia else texto


def leer_registros(datos: object) -> list[dict] | None:
    if isinstance(datos, list):
        return datos
    if isinstance(datos, dict):
        for clave in ("publicaciones", "registros", "datos", "items", "results"):
            if isinstance(datos.get(clave), list):
                return datos[clave]
    return None


def indice_historico(carpeta: Path) -> tuple[set[str], set[str]]:
    """IDs existentes (globales) y subset RM, sin alterar ningún JSON.

    El subconjunto RM es un indicador orientativo basado en la información
    guardada de búsqueda; no sustituye la auditoría del notebook.
    """
    ids_globales: set[str] = set()
    ids_rm: set[str] = set()
    if not carpeta.is_dir():
        return ids_globales, ids_rm
    for archivo in sorted(carpeta.rglob("portal*.json")):
        try:
            with archivo.open(encoding="utf-8-sig") as f:
                filas = leer_registros(json.load(f))
            if filas is None:
                print(f"AVISO: formato JSON no reconocido: {archivo.name}")
                continue
        except (OSError, UnicodeError, ValueError) as error:
            print(f"AVISO: no se pudo leer {archivo.name}: {error}")
            continue
        for fila in filas:
            if not isinstance(fila, dict):
                continue
            id_ = normalizar_id(fila.get("id") or fila.get("id_publicacion") or fila.get("url"))
            if id_ is None:
                continue
            ids_globales.add(id_)
            categoria = str(fila.get("tipo_propiedad_busqueda") or fila.get("tipo_propiedad") or "").casefold()
            if not categoria:
                coincidencia = re.search(r"(?:venta|arriendo)_(casa|departamento)_", archivo.name.casefold())
                categoria = coincidencia.group(1) if coincidencia else ""
            ubicacion = " ".join((
                str(fila.get("ubicacion_busqueda") or ""),
                str(fila.get("region_busqueda") or ""),
                str(fila.get("region") or ""),
                archivo.name,
            )).casefold()
            if categoria in {"casa", "departamento"} and "metropolitana" in ubicacion:
                ids_rm.add(id_)
    return ids_globales, ids_rm


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


def ejecutar(args: argparse.Namespace) -> int:
    busquedas = list(BUSQUEDAS)
    print("Búsquedas regionales: casas y departamentos de la Región Metropolitana.")
    print(f"Intervalo: páginas {args.desde_pagina} a {args.max_paginas} por categoría | pausa mínima: {max(5, args.pausa):g} s")
    print("Sin comunas; no modifica otros JSON ni bases de datos.")
    if args.plan:
        for busqueda in busquedas:
            tipo = busqueda["tipo_propiedad"]
            print(f"{tipo}: {construir_url(1, tipo, 'metropolitana')}")
            print(f"    Ejemplo página 2: {construir_url(2, tipo, 'metropolitana')}")
        print("Modo plan: no se realizaron solicitudes ni se escribieron archivos.")
        return 0

    RUTA_DATOS.mkdir(parents=True, exist_ok=True)
    ids_globales, ids_rm = indice_historico(RUTA_DATOS)
    print(f"IDs globales existentes: {len(ids_globales):,}")
    print(f"IDs RM casas/deptos estimados desde metadatos: {len(ids_rm):,} / {args.objetivo:,}")
    print("Usa el notebook de auditoría para el conteo final exacto.")
    if len(ids_rm) >= args.objetivo:
        print("Meta ya cubierta según los metadatos almacenados.")
        return 0

    acceso = AccesoResponsable(args.pausa)
    marca = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    total_nuevos = 0
    total_paginas = 0
    try:
        for i, busqueda in enumerate(busquedas, start=1):
            tipo = busqueda["tipo_propiedad"]
            ruta = RUTA_DATOS / f"portal_regional_venta_{tipo}_metropolitana_{marca}.json"
            registros: dict[str, dict] = {}
            paginas_vistas: set[tuple[str, ...]] = set()
            print(f"\n[{i}/2] {tipo} | toda la Región Metropolitana", flush=True)

            for pagina in range(args.desde_pagina, args.max_paginas + 1):
                url = construir_url(pagina, tipo, "metropolitana")
                try:
                    html = acceso.leer_html(url)
                except PermissionError:
                    print("Bloqueo del portal: se detiene toda la ejecución. No insistir con nuevas solicitudes.")
                    return 2
                if html is None:
                    break
                total_paginas += 1
                tarjetas = BeautifulSoup(html, "html.parser").select("li.ui-search-layout__item")
                if not tarjetas:
                    print("Sin tarjetas: resultados agotados o cambios en HTML; revisar antes de seguir.")
                    break
                filas = extraer_publicaciones(tarjetas, pagina, tipo, "metropolitana")
                if not filas:
                    print("Hay tarjetas, pero no se extrajeron IDs; revisar selectores HTML.")
                    break
                firma = tuple(str(f["id"]) for f in filas if f.get("id"))
                if firma in paginas_vistas:
                    print("Página repetida: se detiene esta categoría para evitar bucles.")
                    break
                paginas_vistas.add(firma)
                nuevos_pagina = 0
                for fila in filas:
                    identificador = normalizar_id(fila.get("id"))
                    if identificador is None or identificador in ids_globales:
                        continue
                    fila["id"] = identificador
                    fila["region_busqueda"] = "metropolitana"
                    ids_globales.add(identificador)
                    ids_rm.add(identificador)
                    registros[identificador] = fila
                    total_nuevos += 1
                    nuevos_pagina += 1
                    if len(ids_rm) >= args.objetivo:
                        break
                # Persist after every page, safely replacing this RUN's file only.
                if registros:
                    guardar_atomico(ruta, registros)
                print(f"Página {pagina}/{args.max_paginas}: {len(filas)} anuncios, "
                      f"{nuevos_pagina} nuevos | RM estimados: {len(ids_rm):,}/{args.objetivo:,}", flush=True)
                if len(ids_rm) >= args.objetivo:
                    print("Meta indicada alcanzada; confirma con el notebook de auditoría.")
                    return 0
            print(f"{tipo}: {len(registros)} nuevas publicaciones guardadas.")
    except KeyboardInterrupt:
        print("\nDetenido con Ctrl+C. Los JSON guardados permanecen intactos.")
        return 130

    print(f"FIN: {total_nuevos:,} IDs nuevos, {total_paginas} páginas HTML descargadas.")
    print("Si no llegamos a 20.000, las dos búsquedas regionales pueden estar limitadas por el portal.")
    print("NO ejecutes todavía el cargador antiguo de DW con estos nuevos JSON; hay que adaptarlo.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Casas y departamentos en venta, toda la RM, sin comunas")
    parser.add_argument("--objetivo", type=int, default=20000, help="Meta total de IDs únicos RM")
    parser.add_argument("--max-paginas", type=int, default=30, help="Páginas máximas por categoría (30 por defecto)")
    parser.add_argument("--desde-pagina", type=int, default=1, help="Página inicial por categoría (1 por defecto)")
    parser.add_argument("--pausa", type=float, default=8.0, help="Segundos entre solicitudes (mínimo 5)")
    parser.add_argument("--plan", action="store_true", help="Mostrar búsquedas sin hacer solicitudes")
    args = parser.parse_args()
    if args.objetivo < 1 or args.desde_pagina < 1 or args.max_paginas < args.desde_pagina or args.pausa < 0:
        parser.error("Objetivo, páginas y pausa deben tener valores válidos.")
    return ejecutar(args)


if __name__ == "__main__":
    sys.exit(main())
