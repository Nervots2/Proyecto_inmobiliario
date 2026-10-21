import pandas as pd
from sqlalchemy import create_engine

# 1. Configurar la conexión a PostgreSQL (coincide con tu docker-compose)
# Formato: postgresql+psycopg2://usuario:password@host:puerto/base_de_datos
DB_URL = "postgresql+psycopg2://admin:password123@localhost:5432/inmobiliaria_db"
engine = create_engine(DB_URL)

def cargar_csv_a_postgres():
    ruta_archivo = "datalake_crudo/muestra_inmuebles.csv"
    
    try:
        # 2. Leer el archivo con Pandas
        print(f"Leyendo datos de {ruta_archivo}...")
        df = pd.read_csv(ruta_archivo)
        
        # 3. Limpieza básica (opcional, útil para datos del SII)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # 4. Enviar a PostgreSQL
        print("Cargando datos en PostgreSQL...")
        df.to_sql(
            name='inmuebles_sii', # Nombre de la tabla en SQL
            con=engine,
            if_exists='replace',  # 'replace' sobrescribe, 'append' agrega filas
            index=False           # No guardar el índice de pandas
        )
        print("¡Ingesta completada con éxito!")
        
    except Exception as e:
        print(f"Error durante la ingesta: {e}")

if __name__ == "__main__":
    cargar_csv_a_postgres()