import requests
import os
import time

def ingesta_masiva_tip():
    api_key = "50255ae506c76d7ce35e7a137303b2239de5ff1f"
    años = range(2002, 2013) # Historial desde 2013 hasta 2026
    
    cabeceras = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    
    os.makedirs("datalake_crudo", exist_ok=True)
    print("Iniciando descarga histórica de Tasas de Interés Promedio (TIP)...")
    
    for anio in años:
        url = f"https://api.cmfchile.cl/api-sbifv3/recursos_api/tip/{anio}?apikey={api_key}&formato=json"
        ruta_archivo = f"datalake_crudo/tip_{anio}_bronce.json"
        
        try:
            respuesta = requests.get(url, headers=cabeceras, timeout=10)
            
            # Guardamos solo si la respuesta es exitosa y no es una página HTML de error
            if respuesta.status_code == 200 and 'html' not in respuesta.text.lower()[:20]:
                with open(ruta_archivo, "w", encoding="utf-8") as archivo:
                    archivo.write(respuesta.text)
                print(f"✅ Año {anio} guardado correctamente.")
            else:
                print(f"⚠️ Año {anio} sin datos o bloqueado.")
                
        except Exception as e:
            print(f"Error de conexión en el año {anio}: {e}")
            
        # Pausa vital para no saturar el servidor y cuidar tu IP
        time.sleep(10)
        
    print("\n¡Extracción a Capa Bronce completada!")

if __name__ == "__main__":
    ingesta_masiva_tip()