# Proyecto Inmobiliario — Chile | REAL STATE BIG DATA

**Estado:** arquitectura Big Data implementada; fuentes oficiales y modelos analíticos integrados; consumo visual mediante Power BI en preparación.  
**Última actualización de esta documentación:** 4 de noviembre de 2026.

## 1. Descripción y objetivo

Proyecto académico de **Técnico Universitario en Ciencia de Datos**, **Universidad Técnica Federico Santa María (UTFSM)**, desarrollado en la asignatura Taller de Herramientas para el Procesamiento de Datos.

El propósito es **recopilar, organizar, procesar y relacionar datos del mercado inmobiliario chileno**. Las publicaciones de oferta se complementan con estadísticas de predios, geografía, suelo urbano, construcción y variables financieras y económicas.

Los **precios publicados**, los **valores de transacciones de suelo** y los **avalúos fiscales** son medidas distintas; no deben compararse como si fueran equivalentes.

### Arquitectura general

```text
Fuentes originales: ChilePropiedades | Portal Inmobiliario | SII | INE | MINVU | ...
                               |
                               v
                   Ingesta Python / archivos originales
                               |
                               v
          DATA LAKE: Silo/MinIO (S3) — lake-crudo / lake-procesado
                               |
                Limpieza y transformación
                    PySpark / Polars
                               |
               +---------------+----------------+
               |                                |
               v                                v
     DATA LAKEHOUSE                        DATA WAREHOUSE
     Delta Lake + Parquet                   PostgreSQL/PostGIS
     Trino + Hive Metastore                 Dimensiones + hechos
               |                                |
               +----------- SQL / dbt ----------+
                               |
                  Validación y análisis
                               |
                Consumo: SQL / CSV / Power BI
```

**Alcance:** los almacenamientos y procesamientos principales están implementados. El consumo analítico mediante SQL y CSV está disponible; el dashboard Power BI lo desarrolla un integrante del equipo. No existe todavía una ejecución automática única de extremo a extremo ni se han incorporado todas las fuentes crudas.

## 2. Fuentes incorporadas y pendientes

| Fuente | Estado verificado | Cobertura o limitación |
|---|---|---|
| **ChilePropiedades** | Integrada en Delta Lake y PostgreSQL | **28.307** publicaciones |
| **Portal Inmobiliario** | JSON auditados; **7.609** IDs únicos integrados en Delta Lake y PostgreSQL | Incluye distintas categorías y regiones; **4.240** corresponden a casas y departamentos de la Región Metropolitana, según auditoría |
| **SII 2025-S2** | PDF y CSV RM publicados en S3; tabla Delta creada | **52** comunas de la RM; **3.690.399** predios en el agregado del documento |
| **SII 2026-S1** | CSV nacional y RM publicados en S3; tabla Delta nacional y hechos en PostgreSQL | **346** comunas, **17** destinos, **8.560.728** predios a nivel nacional; **52** comunas y **3.700.127** predios en la RM |
| **INE** | Cartografía 2017/2024 auditada; códigos territoriales incorporados a PostgreSQL | **346** códigos CUT; **343** correspondencias directas con la capa comunal 2024 y **3** casos documentados; geometrías no integradas al DW |
| **MINVU (informe 2026)** | PDF crudo + 5 CSV en S3; 5 tablas Delta; 5 hechos y dimensión regional en PostgreSQL | **17** filas por tabla: 16 regiones y un **total nacional** separado; indicadores y períodos diferentes |
| **CMF** | Archivos JSON crudos disponibles | Inventariados los archivos `tip_2002_bronce.json` a `tip_2026_bronce.json`; estructura e integración pendientes |
| **Banco Central** | Integrado en Delta Lake y PostgreSQL | **297** meses (2002–2026) de indicadores financieros de vivienda; **144** valores de flujos no disponibles se conservan como NULL, no se imputan |
| **ChileHub — permisos de edificación** | Integrado en Delta Lake y PostgreSQL | **8.650** registros comuna-año (346 comunas × 25 años, 2002–2026); **2024–2026 provisionales**, no equivalen necesariamente a construcción efectivamente ejecutada |
| **Observatorio — ocupados en construcción** | Integrado en Delta Lake y PostgreSQL | **363** registros del año 2024, con niveles comunal, regional y nacional; existe una incidencia de dato faltante para Río Verde |

Una fuente **presente en `datalake_crudo/`** no se considera automáticamente validada o integrada.

## 3. Los tres tipos de almacenamiento

| Tipo | Tecnologías | Implementación |
|---|---|---|
| **Data Lake** | Silo/MinIO, API compatible con S3 | Buckets `lake-crudo` y `lake-procesado`; PDF, JSON, CSV, Parquet y otros archivos |
| **Data Lakehouse** | Delta Lake, Trino, Hive Metastore, Parquet | Tablas Delta de publicaciones, SII, MINVU, Banco Central, ChileHub y Observatorio; historial transaccional Delta probado |
| **Data Warehouse** | PostgreSQL/PostGIS + dbt | Tablas de hechos y dimensiones de publicaciones, SII, MINVU, Banco Central, ChileHub y Observatorio; modelos analíticos y controles de calidad |

### Tablas y cifras comprobadas

**Publicaciones:**

- `lake.inmobiliario.publicaciones`: **28.307** publicaciones de ChilePropiedades consultables en Parquet.
- `delta.inmobiliario.publicaciones_delta_final`: **28.307** publicaciones de ChilePropiedades.
- `delta.inmobiliario.portal_inmobiliario_publicaciones`: **7.609** filas y **7.609** IDs distintos.
- `dw.fact_publicaciones`: **35.916** registros: 28.307 ChilePropiedades + 7.609 Portal Inmobiliario; sin repetición de ID **dentro de cada fuente**.
- `dbt.resumen_publicaciones`: **22** agrupaciones por tipo de propiedad y moneda; suma de publicaciones **35.916**.
- **35.188** publicaciones tienen `id_ubicacion` vinculado a la dimensión comunal (**aprox. 98 %**); **728** permanecen sin vincular para evitar asignaciones ambiguas.
- `dbt.indicadores_comunales_2026`: **346** filas, con indicadores SII, ChileHub y publicaciones enlazadas por código CUT.
- `dbt.calidad_precios_publicaciones`: **35.916** filas y alertas conservadoras por umbral y por UF.
- `dbt.resumen_precios_comparativo`: **27** agrupaciones por fuente, tipo y moneda, con media y mediana originales y filtradas.
- El proyecto contiene **4 modelos dbt y 20 pruebas**. Las pruebas de integridad del resumen comparativo y de alertas UF se ejecutaron satisfactoriamente; antes de entregar se recomienda ejecutar `dbt build` completo.

**SII e INE:**

- `delta.inmobiliario.sii_rm_2025_s2`: **52** comunas de la RM.
- `delta.inmobiliario.sii_destinos_comuna_2026_s1`: **346** comunas del país.
- `dw.fact_sii_destinos`: **346** registros del período 2026-S1.
- `dw.dim_ubicacion`: **346** comunas con código de región y código comunal completados.
- Los casos **Paiguano (CUT 04105)**, **Treguaco (CUT 16207)** y **Antártica (CUT 12202)** quedaron documentados separadamente. Antártica no tuvo geometría en la capa comunal consultada.

**MINVU:**

- Cinco tablas `delta.inmobiliario.minvu_2026_t1_*` a `minvu_2026_t5_*`: **17** filas cada una, sin duplicados.
- Cinco tablas equivalentes `dw.fact_minvu_2026_t1_*` a `dw.fact_minvu_2026_t5_*`: **17** filas cada una.
- `dw.dim_region`: **16** regiones.
- Las cinco tablas del MINVU contienen crecimiento urbano, superficie urbana dentro/fuera de territorios sanitarios, suelo disponible y transacciones/valores de suelo.
- **«Nacional» es un agregado independiente**: para sumar regiones se debe filtrar `nivel_geografico = 'regional'` y no sumar de nuevo la fila nacional.

**Otras fuentes integradas:**

- `dw.fact_banco_central_vivienda_mensual`: **297** registros mensuales.
- `dw.fact_chilehub_permisos_edificacion`: **8.650** registros comuna-año; los más recientes son provisionales.
- `dw.fact_observatorio_ocupados_construccion_2024`: **363** registros con diferentes niveles geográficos.

**Calidad de precios (sin alterar los precios originales):**

- `dw.alertas_uf_chilepropiedades`: **234** casos candidatos a revisión por discrepancias entre el título y el UF almacenado; ninguna corrección se aplicó automáticamente.
- Umbrales exploratorios sobre ChilePropiedades: **50** precios muy bajos (`<= 10.000 CLP`) y **30** extremadamente altos (`>= 10.000.000.000 CLP`), **80** en total.
- Análisis exploratorio IQR por tipo de propiedad, solo en grupos con al menos 30 registros: **28.254** anuncios evaluados y **2.722** atípicos; **30** coincidieron con alertas por umbral, **2.692** fueron detectados solo por IQR y **50** solo por umbral. Los **53** registros restantes corresponden a tipos con muestras demasiado pequeñas para esa auditoría.
- Un precio estadísticamente atípico **no implica error**. Ni los umbrales ni IQR se usan para borrar o reemplazar automáticamente los valores.

Las cantidades de publicaciones proceden de verificaciones de las fuentes; **no equivalen necesariamente a inmuebles físicos únicos entre portales**. Las cifras pueden cambiar con futuras cargas.

## 4. Cinco tipos de procesamiento desarrollados

| Tipo académico | Tecnología | Uso |
|---|---|---|
| **1** | PySpark | Procesamiento por lotes y generación de archivos Silver |
| **3** | Trino | SQL sobre Parquet y tablas Delta registradas en catálogos |
| **4** | dbt | ELT en PostgreSQL y pruebas de calidad |
| **6** | Hugging Face / Transformers | Clasificación NLP Zero-Shot sobre una muestra de 100 descripciones; resultado exploratorio, pendiente de validación manual |
| **8** | Polars | Transformación eficiente de DataFrames |

**Evidencias de procesamiento:**

- **PySpark 4.0.1**, `scripts/procesamiento_batch_spark.py`: **14** JSONL, **28.603** registros originales, **28.307** registros únicos, **296** duplicados descartados; salida de **14** columnas en **3** archivos Parquet y `reporte_pyspark.json`.
- **Polars**, `notebooks/avance4_procesamiento.ipynb`: lectura y agregación *lazy* de **28.307** publicaciones, **429** grupos, **6** columnas; salida Parquet y `reporte_polars.json`.
- **Trino**: SQL sobre catálogos Delta y Parquet, integraciones y consultas en `sql/02_trino_publicaciones.sql` y `sql/03_trino_analisis.sql`.
- **dbt**: cuatro modelos y veinte pruebas declaradas; controles de integridad con resultados satisfactorios.
- **Hugging Face**: `notebooks/procesamiento_nlp.ipynb`, clasificación zero-shot exploratoria sobre 100 descripciones, desarrollada por otro integrante; requiere revisar sus resultados para explicarla en la defensa.

Notebooks relevantes: `notebooks/avance4_procesamiento.ipynb`, `notebooks/procesamiento_nlp.ipynb` y `notebooks/auditoria_portal_inmobiliario.ipynb`.

## 5. Tecnologías y herramientas utilizadas

| Área | Herramientas | Función |
|---|---|---|
| Desarrollo | Python, VS Code, Jupyter, Conda | Scripts, notebooks, entornos |
| Ingesta web y archivos | Requests, Beautiful Soup (`bs4`), `boto3` | Extracción y publicación de objetos S3 |
| Tratamiento de datos | pandas, PySpark, Polars, PyArrow / Parquet | Limpieza, transformación y procesamiento |
| PDFs y Excel | **pdfplumber**, **openpyxl** | Extracción de tablas MINVU y lectura del diccionario INE |
| Cartografía | Shapefile/DBF, GeoPackage, SQLite | Auditoría de atributos y códigos INE |
| Consultas y Lakehouse | Trino, Delta Lake, Hive Metastore | SQL y gestión de tablas Delta |
| Data Warehouse | PostgreSQL, PostGIS, `psycopg`, dbt | Hechos, dimensiones, cargas y pruebas |
| NLP | Hugging Face Transformers | Clasificación exploratoria de descripciones |
| Infraestructura | Docker Desktop, Docker Compose, Silo/MinIO | Servicios, almacenamiento y persistencia |
| Configuración | `python-dotenv`, YAML, `.properties`, `.env` | Conexiones y servicios |
| Versionamiento | Git, GitHub, GitHub Desktop | Control de cambios y respaldo del código |

MongoDB, Elasticsearch y Redis figuran en la infraestructura Docker, pero **no se cuentan aquí como almacenamientos validados de la solución**.

## 6. Estructura principal del repositorio

```text
Proyecto_inmobiliario/
├── config/                   # Configuración Trino, Hive, dbt y servicios
├── datalake_crudo/           # Archivos originales por fuente
├── datalake_procesado/
│   ├── silver/               # Datos estructurados y controles de trazabilidad
│   └── nlp/                  # Resultados exploratorios de NLP
├── dbt_inmobiliario/         # Modelos y pruebas dbt
├── notebooks/                # Exploración, procesamiento y auditorías
├── scripts/                  # Extracción, validación y cargas
├── sql/                      # Definiciones de DW y consultas
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

No todo el contenido de los directorios de datos necesariamente se versiona en GitHub. Los archivos pesados pueden permanecer en almacenamiento local y en los volúmenes Docker.

## 7. Inicio rápido en Windows / PowerShell

**Requisitos:** Docker Desktop iniciado, repositorio local, Conda y archivo `.env` configurado **sin publicarlo**.

Desde la raíz del proyecto:

```powershell
conda activate inmobiliario
docker compose up -d postgres_gis minio hive_metastore trino
docker compose ps
```

Si Hive Metastore acaba de iniciar, comprobar que se encuentre listo antes de lanzar cargas a Trino.

**Importante:** estos comandos son para un entorno **ya inicializado con sus datos persistidos**. No descargan ni reconstruyen automáticamente el Data Lake, Delta Lake, Hive Metastore ni el Data Warehouse desde una instalación vacía.

### Verificaciones de solo lectura

```powershell
docker compose exec -T trino trino --execute "SELECT COUNT(*) FROM delta.inmobiliario.portal_inmobiliario_publicaciones;"
docker compose exec -T trino trino --execute "SELECT COUNT(*) FROM delta.inmobiliario.sii_destinos_comuna_2026_s1;"
docker compose exec -T trino trino --execute "SELECT COUNT(*) FROM delta.inmobiliario.minvu_2026_t1_crecimiento_urbano_2017_2024;"
```

Para consultar PostgreSQL sin instalar una aplicación gráfica:

```powershell
docker compose exec postgres_gis sh
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
```

Dentro de `psql`:

```sql
SELECT fuente, COUNT(*) AS registros, COUNT(DISTINCT id_publicacion) AS ids_unicos
FROM dw.fact_publicaciones GROUP BY fuente ORDER BY registros DESC;

SELECT SUM(total_publicaciones) FROM dbt.resumen_publicaciones;
```

Salir con `\q` y luego `exit`.

Construir **los cuatro modelos dbt y sus pruebas** sobre una base ya cargada:

```powershell
dbt build --project-dir .\dbt_inmobiliario --profiles-dir .\config\dbt
```

Comprobar cantidad de publicaciones en PostgreSQL sin entrar a la consola interactiva:

```powershell
@'
SELECT COUNT(*) AS publicaciones FROM dw.fact_publicaciones;
'@ | docker compose exec -T postgres_gis sh -c 'psql -U "$POSTGRES_USER" -d inmobiliaria_db'
```

Para compartir el consumo en Power BI se encuentra versionado `reports/indicadores_comunales_2026.csv`: es una **exportación estática**, no una conexión de consulta en tiempo real.

Detener sin eliminar volúmenes:

```powershell
docker compose stop
```

**No utilizar `docker compose down -v` ni borrar volúmenes Docker**. El código versionado en GitHub **no** sustituye un respaldo de PostgreSQL o de los objetos almacenados en Silo.

## 8. Scripts y procedimientos relevantes

### Publicaciones inmobiliarias

- `scripts/scraper_portal_inmobiliario.py`: scraper regional parametrizado, paginación, restricciones `robots.txt` y guardado incremental; sus resultados están sujetos a disponibilidad y condiciones del sitio.
- `notebooks/auditoria_portal_inmobiliario.ipynb`: consolidación y control de IDs y cobertura.
- `scripts/integrar_portal_inmobiliario.py`: auditoría de JSON y cargas a Delta Lake / Data Warehouse, con control de duplicados.
- `scripts/cargar_dw_chilepropiedades.py`: carga de ChilePropiedades desde Delta Lake hacia PostgreSQL.

### Estadísticas oficiales

- `scripts/subir_sii_s3.py` y `scripts/cargar_sii_delta.py`: ingesta del SII 2025-S2 (RM).
- `scripts/integrar_sii_destinos_2026_s1.py` y `scripts/cargar_dw_sii_destinos_2026_s1.py`: ingesta del SII 2026-S1 a Delta Lake y PostgreSQL.
- `scripts/auditar_ine_cartografia.py`, `scripts/comparar_ine_sii_comunas.py`, `scripts/inspeccionar_ine_2024.py`, `scripts/validar_ine_2024_sii.py` y `scripts/completar_codigos_ine_dw.py`: contraste de códigos INE 2017/2024 con SII y actualización territorial del DW.
- `scripts/auditar_minvu_pdf.py`, `scripts/extraer_minvu_tablas.py`, `scripts/integrar_minvu_2026_delta.py` y `scripts/cargar_dw_minvu_2026.py`: extracción de cinco tablas MINVU y cargas a S3, Delta y DW.

### Fuentes complementarias, calidad y geografía

- `scripts/integrar_banco_central_vivienda_delta.py` y `scripts/cargar_dw_banco_central_vivienda.py`: Banco Central.
- `scripts/integrar_chilehub_permisos_delta.py` y `scripts/cargar_dw_chilehub_permisos.py`: ChileHub.
- `scripts/integrar_observatorio_construccion_delta.py` y `scripts/cargar_dw_observatorio_construccion.py`: Observatorio.
- `scripts/vincular_publicaciones_dw.py` y `scripts/verificar_vinculacion_geografica_dw.py`: vinculación comunal y controles.
- `scripts/auditar_precios_uf_historicos.py` y `scripts/integrar_alertas_uf_dw.py`: candidatos a revisión de UF y alertas en el DW.

**Precaución:** los scripts no comparten necesariamente los mismos argumentos; consultar la ayuda y el contenido de cada uno antes de lanzarlos. Algunas ejecuciones escriben, recrean tablas o hacen solicitudes externas. No ejecutar todos los scripts en bloque ni repetir scrapers como parte de la prueba de arranque.

## 9. Precauciones analíticas

- Las publicaciones inmobiliarias son **anuncios de oferta**, no compraventas confirmadas.
- Los valores MINVU de transacciones de suelo se expresan en **UF/m²**; no deben confundirse con el precio total de un inmueble.
- Las estadísticas del SII están agregadas **por comuna y período**; las del MINVU, principalmente **por región** y con distintos períodos de referencia.
- Para comparar fuentes, se requieren claves geográficas y fechas compatibles. No atribuir automáticamente indicadores comunales o regionales a publicaciones individuales.
- Las **monedas UF, CLP y USD** se mantienen diferenciadas en los modelos; cualquier comparación requiere conversión explícita.
- Los 35.916 anuncios tienen IDs únicos **por fuente**; no se ha efectuado una deduplicación de inmuebles publicados en más de un portal.
- Los tres casos geográficos documentados del INE no equivalen a geometrías validadas para todas las comunas.
- Archivos de terceros: respetar licencias, permisos, límites de uso y restricciones de redistribución. Nunca subir contraseñas o `.env`.

## 10. Pendientes y próximos pasos

1. Auditar e integrar los JSON históricos de la **CMF** (`tip_2002_bronce.json` a `tip_2026_bronce.json`), solo si se justifica su utilidad.
2. Revisar las **728** publicaciones sin comuna inequívoca, manteniendo `NULL` cuando no haya evidencia suficiente.
3. Revisar manualmente los **234** candidatos UF; estudiar los atípicos IQR sin confundirlos con errores.
4. Preparar y comprobar el dashboard Power BI, el procedimiento de restauración/reconstrucción de datos y la **demostración presencial de máximo 10 minutos**.

## 11. Contexto académico y reproducibilidad

Este proyecto tiene fines educativos. Se documentan las validaciones realizadas en el entorno local, pero **la infraestructura, las bases de datos y los objetos S3 no se reconstruyen únicamente mediante `git clone`**. Se requieren el `.env` configurado individualmente, dependencias instaladas, archivos de entrada, datos de PostgreSQL, objetos de MinIO y catálogo de Hive Metastore, o bien una ejecución controlada de las ingestas y cargas.

- `requirements.txt` refleja las dependencias conocidas, con versiones fijadas en algunas librerías y otras sin fijar; **no es un entorno totalmente congelado**. Para reproducción exacta conviene registrar las versiones instaladas de Python, Java, paquetes y contenedores.
- No publicar `.env`, credenciales ni respaldos que contengan información restringida.
- No ejecutar `docker compose down -v`: elimina los volúmenes definidos por Compose.
- Se conservarán los datos originales para trazabilidad, y las futuras cargas deberán actualizar cifras y limitaciones de este README.
- Una defensa en vivo puede mostrar consultas sobre resultados ya procesados; la extracción total de portales no es una operación apropiada para ejecutar durante diez minutos.
