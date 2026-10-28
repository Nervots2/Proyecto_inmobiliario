import requests
from bs4 import BeautifulSoup
import json
import os
import time
import random
import re
from elasticsearch import Elasticsearch

es = Elasticsearch("http://127.0.0.1:9200", request_timeout=30)
extraer_solo_numeros = lambda texto: ''.join([c for c in texto if c in '0123456789'])

def scraping_masivo(tipo_propiedad):
    cabeceras = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    os.makedirs("datalake_crudo", exist_ok=True)
    
    # Nombres dinámicos para separar "tablas"
    ruta_bronce = f"datalake_crudo/chilepropiedades_{tipo_propiedad}.jsonl"
    nombre_indice = f"chilepropiedades_{tipo_propiedad}_plata"
    
    total_bronce, total_plata, pagina = 0, 0, 0
    
    try:
        es_activo = True
        es.info()
    except:
        es_activo = False

    print(f"🚀 Iniciando extracción masiva de: {tipo_propiedad.upper()}")

    while True:
        # URL dinámica según el tipo
        url_pagina = f"https://chilepropiedades.cl/propiedades/venta/{tipo_propiedad}/region-metropolitana-de-santiago/{pagina}"
        print(f"\n--- Escaneando Página {pagina} de {tipo_propiedad} ---")
        
        try:
            respuesta = requests.get(url_pagina, headers=cabeceras, timeout=15)
            sopa = BeautifulSoup(respuesta.text, 'html.parser')
            enlaces_unicos = list(set([a['href'] for a in sopa.find_all('a', href=True) if '/ver-publicacion/' in a['href']]))
            
            if not enlaces_unicos:
                print(f"🏁 Fin del catálogo de {tipo_propiedad}.")
                break
                
            for idx, url_relativa in enumerate(enlaces_unicos):
                url_absoluta = f"https://chilepropiedades.cl{url_relativa}" if not url_relativa.startswith("http") else url_relativa
                
                try:
                    res_prop = requests.get(url_absoluta, headers=cabeceras, timeout=10)
                    sopa_prop = BeautifulSoup(res_prop.text, 'html.parser')
                    
                    titulo = sopa_prop.title.text.strip() if sopa_prop.title else "Sin título"
                    
                    precio_clp, precio_uf = 0, 0.0
                    for etiqueta in sopa_prop.find_all(['span', 'div', 'h2', 'h3']):
                        texto = etiqueta.get_text(separator=" ").strip()
                        if len(texto) < 40:
                            if '$' in texto and precio_clp == 0:
                                val = extraer_solo_numeros(texto)
                                if val: precio_clp = int(val)
                            elif 'UF' in texto and precio_uf == 0.0:
                                val = ''.join([c for c in texto if c in '0123456789.,']).replace(',', '.')
                                if val.count('.') > 1: val = val.replace('.', '', val.count('.') - 1)
                                if val and val != '.': precio_uf = float(val)

                    caracteristicas = {}
                    for elemento in sopa_prop.find_all(['div', 'li', 'span']):
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

                    entorno = {}
                    categorias_buscar = ['SUPERMERCADOS', 'FARMACIAS', 'COLEGIOS', 'SALUD', 'METRO Y TREN']
                    for categoria in categorias_buscar:
                        header = sopa_prop.find(string=re.compile(f"^{categoria}$", re.IGNORECASE))
                        if header:
                            lugares_limpios = []
                            nombres_vistos = set()
                            contenedor_padre = header.find_parent('div').find_parent('div')
                            if contenedor_padre:
                                for bloque in contenedor_padre.find_all('div'):
                                    texto_bloque = bloque.get_text(separator="|").strip()
                                    if 'metros' in texto_bloque:
                                        partes = [p.strip() for p in texto_bloque.split('|') if p.strip()]
                                        if len(partes) >= 3 and "metros" in partes[-1]:
                                            nombre_lugar = partes[-3]
                                            if nombre_lugar not in nombres_vistos and nombre_lugar.lower() not in categoria.lower():
                                                nombres_vistos.add(nombre_lugar)
                                                try: distancia = int(extraer_solo_numeros(partes[-1]))
                                                except: distancia = 0
                                                lugares_limpios.append({"nombre": nombre_lugar, "direccion": partes[-2], "distancia_metros": distancia})
                            if lugares_limpios: entorno[categoria.lower()] = lugares_limpios

                    parrafos = sopa_prop.find_all('p')
                    descripcion = " ".join([p.text.strip() for p in parrafos if len(p.text.strip()) > 15])

                    documento = {
                        "id_propiedad": f"CHP-{tipo_propiedad[:3]}-{pagina}-{idx}",
                        "url": url_absoluta,
                        "titulo": titulo,
                        "precios": {"clp": precio_clp, "uf": precio_uf},
                        "caracteristicas": caracteristicas,
                        "entorno_raw": entorno,
                        "descripcion_completa": descripcion,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }

                    with open(ruta_bronce, "a", encoding="utf-8") as f:
                        f.write(json.dumps(documento, ensure_ascii=False) + "\n")
                    total_bronce += 1
                        
                    if es_activo:
                        es.index(index=nombre_indice, id=documento["id_propiedad"], document=documento)
                        total_plata += 1
                        
                except Exception as e:
                    pass
                    
            pagina += 1
            time.sleep(random.uniform(2.0, 4.0)) 
            print(f"   ↳ Progreso {tipo_propiedad}: {total_plata} en Elasticsearch.")
            
        except Exception as e:
            break

if __name__ == "__main__":
    
    tipos_a_extraer = ["departamento","bodega","estudio","galpon","hotel","local-comercial","loft","oficina","parcela","sitio","terreno","terreno-agricola","terreno-comercial","terreno-industrial"]
    
    for tipo in tipos_a_extraer:
        scraping_masivo(tipo)
        print(f"✅ Extracción de {tipo} finalizada.\n")