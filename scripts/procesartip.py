import pandas as pd
import json
import glob
from sqlalchemy import create_engine

# 1. Configurar conexión a tu PostgreSQL en Docker
DB_URL = "postgresql+psycopg2://admin:password123@localhost:5432/inmobiliaria_db"
engine = create_engine(DB_URL)

def procesar_plata_tip():
    print("Buscando archivos JSON en la Capa Bronce...")
    # Busca todos los archivos que coincidan con el patrón de la CMF
    archivos_json = glob.glob("C:\\Users\\extac\\OneDrive\\Escritorio\\Proyecto_inmobiliario\\datalake_crudo\\tip_*.json")
    
    if not archivos_json:
        print("No se encontraron archivos en datalake_crudo/. Ejecuta primero la extracción.")
        return

    datos_consolidados = []

    # 2. Leer y consolidar todos los años
    for ruta in archivos_json:
        try:
            with open(ruta, "r", encoding="utf-8") as archivo:
                datos = json.load(archivo)
                # La CMF guarda los datos bajo esta llave
                if 'TIPs' in datos:
                    datos_consolidados.extend(datos['TIPs'])
        except Exception as e:
            print(f"Error leyendo {ruta}: {e}")

    # 3. Transformación (Limpieza con Pandas)
    if datos_consolidados:
        print(f"Se consolidaron {len(datos_consolidados)} registros. Limpiando datos...")
        df = pd.DataFrame(datos_consolidados)
        
        # Estandarizar columnas a minúsculas
        df.columns = df.columns.str.lower()
        
        # Limpiar el formato del valor (reemplazar coma por punto para que PostgreSQL lo entienda como decimal)
        if 'valor' in df.columns:
            df['valor'] = df['valor'].astype(str).str.replace(',', '.').astype(float)

        # 4. Carga en PostgreSQL
        print("Cargando tabla estructurada en PostgreSQL...")
        df.to_sql(
            name='tip_cmf_plata', 
            con=engine, 
            if_exists='replace', 
            index=False
        )
        print("¡Éxito! La tabla 'tip_cmf_plata' ya está disponible en DBeaver.")
    else:
        print("No se encontraron datos para procesar.")

if __name__ == "__main__":
    procesar_plata_tip()