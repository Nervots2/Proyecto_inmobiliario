# Proyecto Inmobiliario — Chile

## Descripción

Proyecto académico desarrollado para la asignatura **Taller de Herramientas para el Procesamiento de Datos**, de la carrera Técnico Universitario en Ciencia de Datos de la Universidad Técnica Federico Santa María (UTFSM).

El proyecto busca recopilar, almacenar, procesar y analizar información del **mercado inmobiliario chileno**, considerando propiedades residenciales, comerciales, industriales y terrenos.

Se trabaja con datos de distintas fuentes y formatos, mediante técnicas de ingesta, almacenamiento y procesamiento orientadas a Big Data.

## Objetivo

Construir un flujo de datos que permita integrar información inmobiliaria proveniente de diversas fuentes para facilitar su exploración, análisis y posterior visualización.

Entre las variables de interés se encuentran:

- Precios publicados de propiedades.
- Tipos de inmuebles y posibles usos.
- Ubicación geográfica.
- Superficie y características de los inmuebles.
- Información territorial, económica y de construcción.

## Estructura del repositorio

```text
Proyecto_inmobiliario/
├── datalake_crudo/          # Datos originales de las distintas fuentes
├── notebooks/               # Desarrollo, exploración y validación de datos
├── scripts/                 # Programas de ingesta y automatización
├── .gitignore               # Archivos excluidos del control de versiones
├── docker-compose.yml       # Configuración de servicios
├── requirements.txt         # Dependencias de Python
└── README.md                # Documentación principal del proyecto
```

La estructura puede ampliarse durante el desarrollo del proyecto.

## Inicio rápido: infraestructura Docker

El proyecto utiliza Docker Compose para administrar los servicios de almacenamiento y procesamiento de datos.

### Requisitos previos

- Docker Desktop instalado y en ejecución.
- Repositorio descargado localmente.
- Archivo `.env` configurado con las credenciales necesarias.

**Nota:** el archivo `.env` contiene información sensible y no debe subirse a GitHub.

### 1. Iniciar los servicios

Abrir una terminal en la carpeta principal del proyecto y ejecutar:

```powershell
docker compose up -d minio postgres_gis
```

Este comando inicia los servicios:

- **SILO (servicio `minio`):** almacenamiento de objetos compatible con S3 para el Data Lake.
- **PostgreSQL/PostGIS:** almacenamiento relacional y soporte de consultas geoespaciales para el Data Warehouse.

### 2. Verificar los contenedores

```powershell
docker compose ps
```

Comprobar que ambos servicios se encuentren en ejecución.

### 3. Acceder a los almacenamientos

**Data Lake — SILO**

- Consola: http://localhost:9001
- Credenciales: definidas en `.env`
- Bucket de datos crudos: `lake-crudo`

**Data Warehouse — PostgreSQL**

Puede consultarse mediante DBeaver utilizando:

- Host: `localhost`
- Puerto: `5432`
- Base de datos: `inmobiliaria_db`
- Credenciales: definidas en `.env`
- Esquema analítico: `dw`

### 4. Detener los servicios

```powershell
docker compose stop minio postgres_gis
```

Este comando detiene los contenedores sin eliminar los volúmenes persistentes.

**Importante:** iniciar los contenedores no ejecuta automáticamente todas las ingestas ni los procesos de transformación del proyecto.


## Fuentes de datos

El proyecto contempla información obtenida de fuentes como:

- **Banco Central de Chile:** indicadores económicos y tasas de interés.
- **Servicio de Impuestos Internos (SII):** información de propiedades y contribuciones.
- **Ministerio de Vivienda y Urbanismo (MINVU):** información habitacional y territorial.
- **Instituto Nacional de Estadísticas (INE):** datos estadísticos y cartográficos.
- **Portal Inmobiliario:** anuncios de propiedades en venta obtenidos mediante extracción web.

Se incorporan archivos de distintos formatos, entre ellos CSV, Excel, JSON, PDF, Parquet y datos geoespaciales.

## Ingesta de datos

### Scraper de Portal Inmobiliario

Se desarrolló un scraper en Python para obtener información de anuncios de propiedades en venta.

**Funcionalidades implementadas:**

- Construcción parametrizada de URLs de búsqueda.
- Consulta de reglas de acceso mediante `robots.txt`.
- Extracción de información desde HTML utilizando BeautifulSoup.
- Paginación con límites configurables y pausas entre solicitudes.
- Identificación de publicaciones mediante su ID.
- Detección de anuncios de proyectos inmobiliarios y precios iniciales.
- Almacenamiento opcional de los resultados en formato JSON.

El scraper permite configurar diferentes tipos de propiedades, ubicaciones y cantidades de páginas.

**Ejecutar desde la carpeta principal del repositorio:**

```bash
python scripts/scraper_portal_inmobiliario.py
```

Las configuraciones de búsqueda y la opción `GUARDAR_JSON` se modifican directamente en el script.

## Tecnologías utilizadas

- Python
- Requests
- BeautifulSoup
- Jupyter Notebook
- Git y GitHub

Otras herramientas se incorporarán según las etapas de almacenamiento, procesamiento y consumo de datos.

## Estado del proyecto

**En desarrollo.**

Se han realizado pruebas funcionales de extracción de departamentos, casas y oficinas, además de validaciones iniciales de integridad, campos faltantes e identificadores duplicados.

Las siguientes etapas consideran ampliar las categorías y regiones, integrar las fuentes recopiladas, implementar nuevos procedimientos de ingesta y desarrollar procesos de transformación, análisis y visualización.

## Consideraciones sobre los datos

Los precios extraídos de anuncios inmobiliarios corresponden a **precios publicados de oferta**, no necesariamente a valores finales de compraventa.

Algunos anuncios representan proyectos con múltiples unidades y precios iniciales, por lo que deben distinguirse de publicaciones individuales.

Determinados datasets de gran tamaño y extracciones de terceros se conservan localmente y no se incluyen en el repositorio público. Su utilización y redistribución están sujetas a las condiciones aplicables de cada fuente.

## Contexto académico

Proyecto realizado con fines educativos, orientado a la aplicación práctica de conceptos de ingesta, almacenamiento, procesamiento y análisis de datos.
