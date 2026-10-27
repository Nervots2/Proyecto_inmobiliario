import requests
from bs4 import BeautifulSoup
import json

def probar_un_aviso():
    url_lista = "https://chilepropiedades.cl/propiedades/venta/casa/region-metropolitana-de-santiago/0"
    cabeceras = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    extraer_solo_numeros = lambda texto: ''.join([c for c in texto if c in '0123456789'])

    print("1. Buscando una propiedad real en el catálogo (Filtro a prueba de balas)...")
    try:
        res_lista = requests.get(url_lista, headers=cabeceras, timeout=10)
        sopa_lista = BeautifulSoup(res_lista.text, 'html.parser')
        
        # Filtro blindado: SOLO enlaces que sean publicaciones reales de propiedades
        enlaces_validos = []
        for a in sopa_lista.find_all('a', href=True):
            href = a['href']
            if '/ver-publicacion/' in href:
                enlaces_validos.append(href)
                
        enlaces_unicos = list(set(enlaces_validos))
        
        if not enlaces_unicos:
            print("⚠️ No se encontraron enlaces de propiedades con el filtro estricto.")
            return
            
        # Tomamos el primer aviso real de la lista
        enlace_prueba = enlaces_unicos[0]
        url_absoluta = f"https://chilepropiedades.cl{enlace_prueba}" if not enlace_prueba.startswith("http") else enlace_prueba
        print(f"2. Extrayendo datos de:\n   {url_absoluta}\n")
        
        # --- INICIO DE EXTRACCIÓN ---
        res_prop = requests.get(url_absoluta, headers=cabeceras, timeout=10)
        sopa = BeautifulSoup(res_prop.text, 'html.parser')
        
        # 1. Título
        titulo = sopa.title.text.strip() if sopa.title else "Sin título"
        
        # 2. Precios
        precio_clp, precio_uf = 0, 0.0
        for etiqueta in sopa.find_all(['span', 'div', 'h2', 'h3']):
            texto = etiqueta.get_text(separator=" ").strip()
            if len(texto) < 40:
                if '$' in texto and precio_clp == 0:
                    val = extraer_solo_numeros(texto)
                    if val: precio_clp = int(val)
                elif 'UF' in texto and precio_uf == 0.0:
                    val = ''.join([c for c in texto if c in '0123456789.,']).replace(',', '.')
                    if val.count('.') > 1:
                        val = val.replace('.', '', val.count('.') - 1)
                    if val and val != '.': precio_uf = float(val)

        # 3. Características Estructurales
        caracteristicas = {}
        for elemento in sopa.find_all(['div', 'li', 'span']):
            texto = elemento.text.lower().strip()
            if len(texto) < 40:
                if 'habitaci' in texto or 'dormitorio' in texto:
                    nums = extraer_solo_numeros(texto)
                    if nums and len(nums) < 3: caracteristicas['habitaciones'] = int(nums)
                elif 'baño' in texto:
                    nums = extraer_solo_numeros(texto)
                    if nums and len(nums) < 3: caracteristicas['banos'] = int(nums)
                elif 'm2' in texto or 'superficie' in texto:
                    nums = extraer_solo_numeros(texto)
                    if nums and len(nums) < 6: caracteristicas['superficie_m2'] = int(nums)

        # 4. Lugares de Interés (Entorno) estructurado
        entorno = {}
        import re
        categorias_buscar = ['SUPERMERCADOS', 'FARMACIAS', 'COLEGIOS', 'SALUD', 'METRO Y TREN']
        
        for categoria in categorias_buscar:
            header = sopa.find(string=re.compile(f"^{categoria}$", re.IGNORECASE))
            if header:
                lugares_limpios = []
                nombres_vistos = set()
                
                contenedor_padre = header.find_parent('div').find_parent('div')
                if contenedor_padre:
                    bloques_lugar = contenedor_padre.find_all('div')
                    for bloque in bloques_lugar:
                        texto_bloque = bloque.get_text(separator="|").strip()
                        if 'metros' in texto_bloque:
                            partes = [p.strip() for p in texto_bloque.split('|') if p.strip()]
                            if len(partes) >= 3 and "metros" in partes[-1]:
                                nombre_lugar = partes[-3]
                                if nombre_lugar not in nombres_vistos and nombre_lugar.lower() not in categoria.lower():
                                    nombres_vistos.add(nombre_lugar)
                                    try:
                                        distancia = int(extraer_solo_numeros(partes[-1]))
                                    except:
                                        distancia = 0
                                        
                                    lugares_limpios.append({
                                        "nombre": nombre_lugar,
                                        "direccion": partes[-2],
                                        "distancia_metros": distancia
                                    })
                
                if lugares_limpios:
                    entorno[categoria.lower()] = lugares_limpios

        # 5. Descripción Completa
        parrafos = sopa.find_all('p')
        descripcion = " ".join([p.text.strip() for p in parrafos if len(p.text.strip()) > 15])

        documento_prueba = {
            "url": url_absoluta,
            "titulo": titulo,
            "precios": {
                "clp": precio_clp,
                "uf": precio_uf
            },
            "caracteristicas": caracteristicas,
            "entorno": entorno,
            "muestra_descripcion": descripcion[:150] + "..." if len(descripcion) > 150 else descripcion
        }

        print("=== JSON RESULTANTE ===")
        print(json.dumps(documento_prueba, indent=4, ensure_ascii=False))
        print("=======================")

    except Exception as e:
        print(f"Error durante la prueba: {e}")

if __name__ == "__main__":
    probar_un_aviso()