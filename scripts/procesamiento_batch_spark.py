import json
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


# Rutas internas del contenedor Docker
RAIZ = Path("/workspace")
RUTA_DATOS = RAIZ / "datalake_crudo" / "chilepropiedades"
RUTA_SILVER = RAIZ / "datalake_procesado" / "silver"

RUTA_SALIDA = RUTA_SILVER / "chilepropiedades_batch_spark"
RUTA_REPORTE = RUTA_SILVER / "reporte_pyspark.json"


def main():

    spark = (
        SparkSession.builder
        .appName("Proyecto_Inmobiliario_Batch")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1. Localizar y leer los archivos originales
        archivos = sorted(RUTA_DATOS.glob("chilepropiedades_*.jsonl"))

        if not archivos:
            raise FileNotFoundError("No se encontraron archivos JSONL.")

        df = spark.read.json([str(a) for a in archivos])
        total_original = df.count()

        print("=== LECTURA BATCH ===")
        print("Archivos:", len(archivos))
        print("Registros originales:", total_original)

        # 2. Normalizar las URLs y fechas
        df = (
            df
            .withColumn(
                "url_normalizada",
                F.regexp_replace(F.trim("url"), r"/+$", "")
            )
            .withColumn(
                "fecha_extraccion_dt",
                F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
            )
        )

        sin_url = df.filter(
            F.col("url_normalizada").isNull()
            | (F.length("url_normalizada") == 0)
        ).count()

        if sin_url:
            raise ValueError(f"Se encontraron {sin_url} registros sin URL.")

        # 3. Deduplicar utilizando funciones de ventana
        ventana = Window.partitionBy("url_normalizada").orderBy(
            F.col("fecha_extraccion_dt").desc_nulls_last(),
            F.col("id_propiedad").asc_nulls_last()
        )

        df_unicos = (
            df
            .withColumn("numero_fila", F.row_number().over(ventana))
            .filter(F.col("numero_fila") == 1)
            .drop("numero_fila")
        )

        # 4. Transformar estructuras anidadas
        df_tabular = df_unicos.select(
            F.concat(
                F.lit("CHP-"),
                F.sha2("url_normalizada", 256)
            ).alias("id_publicacion"),

            F.lit("chilepropiedades").alias("fuente"),
            F.col("url_normalizada").alias("url"),
            F.col("titulo"),

            F.regexp_extract(
                "url_normalizada",
                r"/ver-publicacion/[^/]+/[^/]+/([^/]+)/",
                1
            ).alias("tipo_propiedad"),

            F.regexp_extract(
                "url_normalizada",
                r"/ver-publicacion/[^/]+/([^/]+)/",
                1
            ).alias("comuna_slug"),

            F.when(F.col("precios.clp") > 0, F.col("precios.clp"))
                .alias("precio_clp"),

            F.when(F.col("precios.uf") > 0, F.col("precios.uf"))
                .alias("precio_uf"),

            F.when(
                F.col("caracteristicas.superficie_m2") > 0,
                F.col("caracteristicas.superficie_m2")
            ).alias("superficie_m2"),

            F.col("caracteristicas.habitaciones").alias("habitaciones"),
            F.col("caracteristicas.banos").alias("banos"),
            F.col("fecha_extraccion_dt").alias("fecha_extraccion"),
            F.col("descripcion_completa"),
            F.col("entorno_raw")
        )

        # 5. Guardar Parquet mediante Spark
        RUTA_SILVER.mkdir(parents=True, exist_ok=True)

        df_tabular.write.mode("overwrite").parquet(str(RUTA_SALIDA))

        # 6. Comprobar el resultado
        df_guardado = spark.read.parquet(str(RUTA_SALIDA))

        total_guardado = df_guardado.count()
        ids_distintos = df_guardado.select(
            "id_publicacion"
        ).distinct().count()

        assert total_guardado == ids_distintos
        assert total_guardado <= total_original

        duplicados = total_original - total_guardado

        # 7. Generar reporte de trazabilidad
        reporte = {
            "procesamiento": "Batch con Apache Spark",
            "tipo_pauta": 1,
            "tecnologia": "PySpark",
            "version_spark": spark.version,
            "modo_ejecucion": spark.sparkContext.master,
            "fecha_procesamiento_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "archivos_entrada": len(archivos),
            "registros_originales": total_original,
            "registros_procesados": total_guardado,
            "duplicados_eliminados": duplicados,
            "columnas": len(df_guardado.columns),
            "formato_salida": "Parquet"
        }

        with open(RUTA_REPORTE, "w", encoding="utf-8") as f:
            json.dump(reporte, f, ensure_ascii=False, indent=4)

        print("\n=== RESULTADO DEL PROCESAMIENTO PYSPARK ===")
        print("Registros originales:", total_original)
        print("Registros guardados:", total_guardado)
        print("Duplicados eliminados:", duplicados)
        print("Columnas:", len(df_guardado.columns))
        print("Dataset:", RUTA_SALIDA)
        print("Reporte:", RUTA_REPORTE)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()