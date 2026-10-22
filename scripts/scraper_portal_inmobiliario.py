# ==========================================
# SCRAPER INMOBILIARIO - CHILE
# Proyecto: Taller de Big Data
# ==========================================

import requests
import re
import json
import time

from bs4 import BeautifulSoup
from datetime import datetime, timezone
from pathlib import Path
from urllib.robotparser import RobotFileParser


# ==========================================
# CONFIGURACIÓN DEL SCRAPER
# ==========================================

# Parámetros de búsqueda
TIPO_PROPIEDAD = "casa"
OPERACION = "venta"
MODALIDAD = None
UBICACION = "santiago-metropolitana"

# Control del almacenamiento
GUARDAR_JSON = False

# Límites de extracción
MAX_PAGINAS = 1
PAUSA_SEGUNDOS = 4
TIMEOUT = 25

# ==========================================
# CONFIGURACIONES DE BÚSQUEDA
# ==========================================

BUSQUEDAS = [
    {
        "tipo_propiedad": "casa",
        "ubicacion": "santiago-metropolitana",
        "modalidad": None,
        "max_paginas": 1
    },
    {
        "tipo_propiedad": "departamento",
        "ubicacion": "santiago-metropolitana",
        "modalidad": "propiedades-usadas",
        "max_paginas": 1
    },
    {
        "tipo_propiedad": "oficina",
        "ubicacion": "valparaiso",
        "modalidad": None,
        "max_paginas": 1
    }
]

# Identificación del programa
AGENTE = "UTFSM-ProyectoAcademico"

# Directorio principal del repositorio
RUTA_PROYECTO = Path(__file__).resolve().parent.parent

# Directorio donde se almacenarán los datos
RUTA_DATOS = (
    RUTA_PROYECTO
    / "datalake_crudo"
    / "portal_inmobiliario"
)

# Dirección principal del portal
URL_BASE = "https://www.portalinmobiliario.com"


# ==========================================
# CONSTRUCCIÓN DE URLs
# ==========================================

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


# ==========================================
# VERIFICACIÓN DE ROBOTS.TXT
# ==========================================

def cargar_reglas_robots():

    url_robots = f"{URL_BASE}/robots.txt"

    try:
        respuesta = requests.get(
            url_robots,
            headers={"User-Agent": AGENTE},
            timeout=TIMEOUT
        )

        respuesta.raise_for_status()

    except requests.RequestException as error:
        print(f"Error al consultar robots.txt: {error}")
        return None

    robots = RobotFileParser()
    robots.parse(respuesta.text.splitlines())

    return robots


# ==========================================
# DESCARGA Y LECTURA DEL HTML
# ==========================================

def obtener_tarjetas(url, robots):

    # Verificar las reglas de acceso
    if not robots.can_fetch(AGENTE, url):
        print("Acceso no permitido por robots.txt.")
        return []

    try:
        # Pausa para limitar la frecuencia de solicitudes
        time.sleep(PAUSA_SEGUNDOS)

        # Solicitar el contenido de la página
        respuesta = requests.get(
            url,
            headers={"User-Agent": AGENTE},
            timeout=TIMEOUT
        )

        respuesta.raise_for_status()

        print("Código HTTP:", respuesta.status_code)
        print("URL final:", respuesta.url)

        # Interpretar el HTML
        soup = BeautifulSoup(
            respuesta.text,
            "html.parser"
        )

        # Identificar las tarjetas inmobiliarias
        tarjetas = soup.select(
            "li.ui-search-layout__item"
        )

        return tarjetas

    except requests.RequestException as error:
        print(f"Error al descargar la página: {error}")
        return []


# ==========================================
# EXTRACCIÓN DE DATOS INMOBILIARIOS
# ==========================================

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

        # Identificar precios iniciales
        precio_desde = "desde" in segmentos

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
            "titulo": extraer_texto(
                tarjeta, ".poly-component__title"
            ),
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



# ==========================================
# EJECUCIÓN PRINCIPAL DEL SCRAPER
# ==========================================

if __name__ == "__main__":

    # Cargar las reglas de acceso una sola vez
    robots = cargar_reglas_robots()

    if robots is None:
        print("No se pudo verificar robots.txt.")

    else:

        # Recorrer las configuraciones de búsqueda
        for busqueda in BUSQUEDAS[2:3]:

            tipo_propiedad = busqueda["tipo_propiedad"]
            ubicacion = busqueda["ubicacion"]
            modalidad = busqueda.get("modalidad")
            max_paginas = busqueda["max_paginas"]

            print("\n========================================")
            print("NUEVA BÚSQUEDA")
            print("========================================")
            print("Tipo:", tipo_propiedad)
            print("Ubicación:", ubicacion)
            print("Modalidad:", modalidad)
            print("Máximo de páginas:", max_paginas)

            # Reiniciar los registros para cada búsqueda
            registros = {}

            for pagina in range(1, max_paginas + 1):

                print(f"\n=== PROCESANDO PÁGINA {pagina} ===")

                # Construir URL según la configuración
                url = construir_url(
                    pagina=pagina,
                    tipo_propiedad=tipo_propiedad,
                    ubicacion=ubicacion,
                    modalidad=modalidad
                )

                # Obtener tarjetas HTML
                tarjetas = obtener_tarjetas(url, robots)

                if not tarjetas:
                    print("No se encontraron tarjetas.")
                    break

                # Extraer publicaciones
                publicaciones = extraer_publicaciones(
                    tarjetas=tarjetas,
                    numero_pagina=pagina,
                    tipo_propiedad=tipo_propiedad,
                    ubicacion_busqueda=ubicacion,
                    modalidad=modalidad
                )

                if not publicaciones:
                    print("No se encontraron publicaciones válidas.")
                    break

                # Eliminar duplicados dentro de la búsqueda
                nuevos = 0

                for publicacion in publicaciones:

                    id_publicacion = publicacion["id"]

                    if id_publicacion not in registros:
                        registros[id_publicacion] = publicacion
                        nuevos += 1

                print("Publicaciones extraídas:", len(publicaciones))
                print("Publicaciones nuevas:", nuevos)
                print("Total acumulado:", len(registros))

                if nuevos == 0:
                    print("No hay registros nuevos.")
                    break

            # ==========================================
            # ALMACENAMIENTO DE LA BÚSQUEDA
            # ==========================================

            dataset = list(registros.values())

            if not dataset:
                print("No hay datos para almacenar.")
                continue

            # Omitir almacenamiento durante las pruebas
            if not GUARDAR_JSON:
                print("\n=== MODO DE PRUEBA ===")
                print("Tipo de propiedad:", tipo_propiedad)
                print("Ubicación:", ubicacion)
                print("Publicaciones únicas:", len(dataset))
                print("No se generó ningún archivo JSON.")

                # Inspeccionar las primeras cinco publicaciones
                print("\n=== MUESTRA DE PUBLICACIONES ===")

                for publicacion in dataset[:5]:
                    print("-" * 45)
                    print("ID:", publicacion["id"])
                    print("Título:", publicacion["titulo"])
                    print("Precio:", publicacion["precio_actual"])
                    print("Ubicación:", publicacion["ubicacion"])
                    print("Atributos:", publicacion["atributos"])
                    print("Tipo de anuncio:", publicacion["tipo_anuncio"])
                    print("Precio desde:", publicacion["precio_es_desde"])
                    print("Unidades disponibles:", publicacion["unidades_disponibles"])

                print("\n=== INSPECCIÓN DE TARJETAS HTML ===")

                for numero, tarjeta in enumerate(tarjetas[:5], start=1):

                    texto_completo = tarjeta.get_text(
                        " | ",
                        strip=True
                    )

                    print(f"\n--- TARJETA {numero} ---")
                    print(texto_completo[:800])
                continue

            RUTA_DATOS.mkdir(
                parents=True,
                exist_ok=True
            )

            fecha_archivo = datetime.now(
                timezone.utc
            ).strftime("%Y%m%d_%H%M%S")

            # Identificar si se aplicó una modalidad
            nombre_modalidad = modalidad or "sin_modalidad"

            nombre_archivo = (
                f"portal_{OPERACION}_{tipo_propiedad}_"
                f"{ubicacion}_{nombre_modalidad}_"
                f"{fecha_archivo}_utc.json"
            )

            ruta_archivo = RUTA_DATOS / nombre_archivo

            with open(
                ruta_archivo,
                "w",
                encoding="utf-8"
            ) as archivo:

                json.dump(
                    dataset,
                    archivo,
                    ensure_ascii=False,
                    indent=2
                )

            print("\n=== RESULTADO DE LA BÚSQUEDA ===")
            print("Publicaciones únicas:", len(dataset))
            print("Archivo guardado:", ruta_archivo)

        print("\n=== FIN DE LA EJECUCIÓN ===")