import requests
from bs4 import BeautifulSoup
import json
import os
import time
from elasticsearch import Elasticsearch

# Conexión a Elasticsearch en Docker
es = Elasticsearch("http://127.0.0.1:9200")

def scraping_real_industrial():
    url_base = "https://chilepropiedades.cl/propiedades/venta/casa/region-metropolitana-de-santiago/0"
    cabeceras = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    print("1. Escaneando catálogo de Chilepropiedades...")
    respuesta = requests.get(url_base, headers=cabeceras, timeout=10)
    sopa = BeautifulSoup(respuesta.text, 'html.parser')
    
    # Nuevo filtro: Busca '/ver-publicacion/' o URLs largas que contengan el título de la propiedad
    enlaces = []
    for a in sopa.find_all('a', href=True):
        href = a['href']
        if '/ver-publicacion/' in href or ('/propiedades/' in href and len(href) > 60):
            enlaces.append(href)
            
    enlaces_unicos = list(set(enlaces))[:10]
    
    if not enlaces_unicos:
        print("⚠️ No se encontraron enlaces de propiedades. El portal podría estar bloqueando la vista o cambió el HTML.")
        print("Muestra de enlaces encontrados en la página para depurar:")
        todos_los_enlaces = [a['href'] for a in sopa.find_all('a', href=True)][:5]
        print(todos_los_enlaces)
        return

    os.makedirs("datalake_crudo", exist_ok=True)
    ruta_bronce = "datalake_crudo/chilepropiedades_real_bronce.jsonl"
    nombre_indice = "chilepropiedades_plata"
    
    total_indexados = 0

    print(f"2. Extrayendo {len(enlaces_unicos)} propiedades a profundidad...")
    for idx, url_relativa in enumerate(enlaces_unicos):
        url_absoluta = f"https://chilepropiedades.cl{url_relativa}" if not url_relativa.startswith("http") else url_relativa
        
        try:
            res_prop = requests.get(url_absoluta, headers=cabeceras, timeout=10)
            sopa_prop = BeautifulSoup(res_prop.text, 'html.parser')
            
            titulo = sopa_prop.title.text.strip() if sopa_prop.title else "Sin título"
            
            parrafos = sopa_prop.find_all('p')
            descripcion = " ".join([p.text.strip() for p in parrafos if len(p.text.strip()) > 15])
            
            documento = {
                "id_propiedad": f"CHP-REAL-{idx}",
                "url": url_absoluta,
                "titulo": titulo,
                "descripcion_completa": descripcion,
                "fuente": "chilepropiedades.cl",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            with open(ruta_bronce, "a", encoding="utf-8") as f:
                f.write(json.dumps(documento, ensure_ascii=False) + "\n")
                
            if es.ping():
                es.index(index=nombre_indice, id=documento["id_propiedad"], document=documento)
                total_indexados += 1
                
            time.sleep(1.5)
            print(f"   ↳ Propiedad {idx+1} guardada e indexada.")
            
        except Exception as e:
            print(f"   ⚠️ Error en propiedad {idx+1}: {e}")

    print(f"\n✅ Pipeline completado. {total_indexados} registros reales en Elasticsearch ('{nombre_indice}').")

if __name__ == "__main__":
    scraping_real_industrial()