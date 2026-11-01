# Proyecto Inmobiliario — Chile | REAL STATE BIG DATA

**Estado:** arquitectura principal implementada; integración de fuentes en desarrollo.

## 1. Descripción y objetivo

Proyecto académico de la carrera **Técnico Universitario en Ciencia de Datos**, Universidad Técnica Federico Santa María (UTFSM), orientado a recopilar, almacenar, procesar e integrar información del **mercado inmobiliario chileno**.

El objetivo es combinar publicaciones de propiedades con estadísticas territoriales, fiscales, habitacionales y económicas para facilitar análisis posteriores. Los precios publicados son **precios de oferta** y no deben confundirse con precios de compraventa ni con avalúos fiscales.

### Flujo de datos

```text
Fuentes (portales, SII, INE, MINVU, CMF, Banco Central, etc.)
       |
       v
Ingesta / extracción -> Data Lake (Silo/S3, Bronze)
       |
       v
Limpieza y transformación -> Silver (CSV / Parquet)
       |                          |
       v                          v
Data Lakehouse (Delta Lake)   Data Warehouse (PostgreSQL/PostGIS)
       |                          |
       +--------> Trino / SQL <---+----> dbt: indicadores y pruebas
                                      |
                                      v
                           Análisis y visualización futura
```

**Aclaración:** el Data Lake y Delta Lake utilizan el mismo almacenamiento de objetos compatible con S3, pero con diferentes formas de organizar y administrar los datos. El Data Warehouse utiliza PostgreSQL. No todas las fuentes han recorrido todavía el flujo completo.

## 2. Fuentes de datos y situación actual

| Fuente | Información | Situación |
|---|---|---|
| **ChilePropiedades** | Publicaciones inmobiliarias | 28.307 publicaciones incorporadas a Delta Lake y PostgreSQL |
| **Portal Inmobiliario** | Publicaciones de venta | Carga inicial de 240 registros en PostgreSQL; scraper ampliado ejecutado, nuevos lotes pendientes de validar e integrar |
| **SII** | Estadísticas comunales, predios, avalúos y contribuciones | Estadísticas RM 2025-S2 cargadas a S3 y Delta Lake; CSV 2026-S1 preparados en Silver, pendientes de integrar |
| **INE** | Estadísticas y cartografía | Archivos recopilados; integración pendiente de revisar |
| **MINVU** | Información habitacional y territorial | Archivos recopilados; integración pendiente de revisar |
| **CMF** | Información financiera | Carpeta y script de ingesta existentes; integración pendiente de verificar |
| **Banco Central** | Indicadores económicos | Archivos recopilados; integración pendiente de revisar |
| **ChileHub y Observatorio** | Fuentes complementarias | Archivos recopilados; contenido y utilidad pendientes de revisar |

**Importante:** la presencia de archivos en `datalake_crudo/` no significa que ya estén normalizados o integrados al Data Warehouse.

## 3. Tres tipos de almacenamiento

| Almacenamiento | Tecnologías | Implementación verificada |
|---|---|---|
| **Data Lake** | **Silo** (servicio Docker `minio`, API S3) | Buckets `lake-crudo` y `lake-procesado`; archivos originales y procesados |
| **Data Lakehouse** | **Delta Lake + Trino + Hive Metastore + Silo/S3** | Tabla Delta de 28.307 publicaciones e historial de transacciones; tabla Delta del SII con 52 comunas |
| **Data Warehouse** | **PostgreSQL + PostGIS + dbt** | 28.547 registros iniciales integrados; 16 grupos de indicadores; 6 pruebas de dbt aprobadas |

### Tablas verificadas

- `lake.inmobiliario.publicaciones`: **28.307** publicaciones consultables en Parquet mediante Trino.
- `delta.inmobiliario.publicaciones_delta_final`: **28.307** publicaciones; historial Delta con versión inicial `CREATE TABLE AS SELECT`.
- `delta.inmobiliario.sii_rm_2025_s2`: **52** comunas; estadísticas que representan **3.690.399** predios (segundo semestre de 2025).
- `dw.fact_publicaciones`: **28.547** registros en la última validación (**28.307** de ChilePropiedades y **240** de Portal Inmobiliario).
- `dbt.resumen_publicaciones`: **16** filas agrupadas por tipo de propiedad y moneda; **6/6** pruebas de calidad aprobadas.

Las cifras corresponden a las ejecuciones validadas hasta esta etapa, no necesariamente al contenido más reciente de los archivos crudos. El scraping ampliado todavía **no** se ha cargado al Data Warehouse.

## 4. Cinco tipos de procesamiento desarrollados

| Tipo | Tecnología | Uso dentro del proyecto |
|---|---|---|
| **1** | **PySpark** | Procesamiento distribuido por lotes de datos inmobiliarios y generación de archivos Silver |
| **3** | **Trino** | Consultas SQL sobre archivos Parquet y tablas registradas en los catálogos |
| **4** | **dbt** | Transformaciones ELT dentro de PostgreSQL y pruebas de calidad |
| **6** | **Hugging Face / Transformers** | NLP Zero-Shot sobre una muestra de 100 descripciones; etiquetas exploratorias no validadas manualmente |
| **8** | **Polars** | Procesamiento y transformación de datos mediante DataFrames |

Notebooks: `notebooks/avance4_procesamiento.ipynb` y `notebooks/procesamiento_nlp.ipynb`.

## 5. Herramientas y tecnologías utilizadas

| Área | Tecnologías / herramientas | Para qué se usan |
|---|---|---|
| **Lenguaje y entorno** | Python, Jupyter Notebook, VS Code, Conda | Desarrollo, exploración y ejecución de scripts y notebooks |
| **Extracción e ingesta** | Requests, Beautiful Soup (`bs4`), `boto3` | Scraping autorizado y carga de archivos a almacenamiento S3 |
| **Procesamiento** | PySpark, Polars, SQL | Transformación por lotes, DataFrames y consultas analíticas |
| **Procesamiento de texto** | Hugging Face Transformers | Clasificación NLP de descripciones inmobiliarias |
| **Data Lake** | Silo/MinIO compatible con S3 | Objetos originales y procesados (Bronze/Silver) |
| **Data Lakehouse** | Delta Lake, Apache Parquet, Trino, Hive Metastore | Tablas transaccionales, almacenamiento columnar, catálogo y consultas SQL |
| **Data Warehouse** | PostgreSQL, PostGIS, dbt, `psycopg` | Modelo dimensional, consultas, cargas y pruebas de calidad |
| **Infraestructura** | Docker Desktop, Docker Compose | Servicios aislados y persistencia mediante volúmenes |
| **Configuración** | `python-dotenv`, YAML, archivos `.properties` | Variables de entorno y configuración de servicios |
| **Versionamiento** | Git, GitHub, GitHub Desktop | Historial de código, revisión y publicación de cambios |

**Servicios adicionales definidos en Docker Compose:** MongoDB, Elasticsearch y Redis. Están contemplados en la infraestructura, pero **no se consideran almacenamientos validados en esta etapa**.

**Formatos de datos utilizados:** PDF, CSV, Excel (`.xlsx`), JSON, JSONL y Parquet, además de información geoespacial. Una parte de los datos masivos se conserva solo en el equipo local.

## 6. Estructura principal del repositorio

```text
Proyecto_inmobiliario/
├── config/                 # Configuración de Trino, Hive, dbt y otros servicios
├── datalake_crudo/         # Fuentes originales, organizadas por organismo
├── datalake_procesado/
│   ├── silver/             # Datos limpios o estructurados
│   └── nlp/                # Resultados del procesamiento de texto
├── dbt_inmobiliario/       # Proyecto dbt: modelos y pruebas
├── notebooks/             # Evidencias y análisis
├── scripts/               # Ingesta, scraping y carga de datos
├── sql/                   # Modelo DW y consultas con Trino
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## 7. Inicio rápido y verificación local

**Requisitos:** Docker Desktop operativo; repositorio local; entorno Python con sus dependencias; variables de `.env` configuradas. Las credenciales **no se versionan**.

Desde la raíz del proyecto:

```powershell
docker compose up -d postgres_gis minio hive_metastore trino
docker compose ps
```

- **Silo/MinIO:** consola local en `http://127.0.0.1:9001` (credenciales en `.env`).
- **Trino:** servicio local en el puerto `8088`.
- **PostgreSQL/PostGIS:** conexión local en el puerto `5432` (usuario, contraseña y base de datos en `.env`).

Verificaciones de solo lectura:

```powershell
docker compose exec trino trino --execute "SELECT COUNT(*) FROM lake.inmobiliario.publicaciones;"
docker compose exec trino trino --execute "SELECT COUNT(*) FROM delta.inmobiliario.publicaciones_delta_final;"
docker compose exec trino trino --execute "SELECT COUNT(*) FROM delta.inmobiliario.sii_rm_2025_s2;"
```

Para actualizar **únicamente** el modelo de indicadores y sus pruebas, cuando PostgreSQL esté disponible:

```powershell
dbt build --project-dir .\dbt_inmobiliario --profiles-dir .\config\dbt --select resumen_publicaciones
```

Para detener los contenedores sin eliminar los volúmenes:

```powershell
docker compose stop
```

**No ejecutar `docker compose down -v`**: podría borrar datos persistidos en volúmenes. Clonar GitHub no restaura automáticamente bases de datos ni objetos almacenados en S3.

## 8. Ingestas y scripts relevantes

- `scripts/scraper_portal_inmobiliario.py`: extrae anuncios, verifica `robots.txt`, aplica pausas, controla duplicados y guarda resultados incrementales. Las búsquedas y páginas se pueden limitar con parámetros CLI.
- `scripts/cargar_dw_publicaciones.py`: cargador inicial de JSON de Portal Inmobiliario; **no ejecutar con los lotes ampliados sin adaptar antes su lectura**.
- `scripts/cargar_dw_chilepropiedades.py`: carga publicaciones de ChilePropiedades al Data Warehouse.
- `scripts/subir_sii_s3.py`: almacena el PDF original SII 2025-S2 y su CSV en buckets S3 separados.
- `scripts/cargar_sii_delta.py`: construye la tabla Delta del SII 2025-S2.
- `scripts/procesamiento_batch_spark.py`: procesamiento por lotes con PySpark.

Ejemplo de prueba acotada del scraper, **solo cuando se decida volver a ejecutarlo**:

```powershell
python scripts/scraper_portal_inmobiliario.py --solo-rm --max-paginas 2 --max-busquedas 2
```

El acceso automatizado a sitios de terceros debe respetar sus restricciones y condiciones. La cantidad de publicaciones obtenibles no está garantizada.

## 9. Datos del SII: alcance y limitaciones

- **2025-S2 (Región Metropolitana):** PDF oficial, CSV derivado y tabla Delta de 52 comunas; contiene predios, avalúos y contribuciones.
- **2026-S1:** Excel original de predios por destino; CSV nacional y CSV de Región Metropolitana preparados en Silver. **Pendiente** de incorporación integral a Delta y PostgreSQL.
- Los montos del PDF 2025-S2 están expresados en **miles de pesos chilenos (M$)**.
- Las estadísticas del SII son **agregadas por comuna**. No se deben asignar directamente a un inmueble individual.
- Las fechas y coberturas deben verificarse antes de comparar con publicaciones extraídas en 2026.

## 10. Próximas etapas

1. Validar el resultado final del scraping ampliado, deduplicar e incorporar los nuevos lotes sin alterar los registros anteriores.
2. Completar la ingesta y transformación de los datasets existentes del SII 2026-S1, INE, MINVU, CMF, Banco Central y otras fuentes útiles.
3. Poblar `dw.dim_ubicacion` con códigos territoriales confiables y relacionar estadísticas comunales con publicaciones **por comuna y período**.
4. Consolidar pipelines de actualización, validaciones de calidad, análisis de valores extremos y documentación reproducible.
5. Preparar los resultados de análisis y visualizaciones de la entrega final.

## 11. Consideraciones y uso académico

Este repositorio se desarrolla con fines educativos. Los precios de oferta no son valores de transacción; los avalúos fiscales cumplen otra finalidad. Algunos anuncios corresponden a proyectos con múltiples unidades y precios iniciales.

Los archivos de gran tamaño, las extracciones de terceros y las credenciales pueden mantenerse fuera de GitHub. Antes de publicar datos, verificar sus condiciones de uso y redistribución. Los resultados NLP son exploratorios y requieren validación adicional.

---

**Proyecto en desarrollo:** la infraestructura y las pruebas enumeradas están verificadas para los conjuntos descritos; la integración total de todas las fuentes continúa pendiente.
