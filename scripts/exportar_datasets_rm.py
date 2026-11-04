"""Exporta cuatro datasets analiticos de la Region Metropolitana desde PostgreSQL.

Proyecto REAL STATE BIG DATA. Uso en Windows/PowerShell desde la raiz del proyecto:

    python scripts/exportar_datasets_rm.py

Requisitos: Docker Desktop y el servicio postgres_gis en ejecucion. El script
consulta tablas ya cargadas (DW y dbt); NO ejecuta scrapers ni altera las tablas.

Cada exportacion usa COPY ... TO STDOUT y conserva los bytes UTF-8 originales.
Los CSV se guardan con BOM para facilitar su importacion en Power BI/Excel.

Cobertura del corte validado en noviembre de 2026:
- 52 comunas de la Region Metropolitana.
- Casas y departamentos: 27.600 publicaciones georreferenciadas.
- ChileHub: datos 2024-2026 provisionales.
- Banco Central: tasa de interes nacional; 2026 tiene meses incompletos.
- Observatorio: ocupacion de residentes, no empleados en obras de la comuna.

Los conteos de referencia son controles del corte actual, no supuestos
universales. Si se actualizan las fuentes, revisar y actualizar las validaciones.
"""

from __future__ import annotations

import csv
import io
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


VIVIENDAS_RM = """
WITH viviendas AS (
    SELECT
        id_ubicacion,
        COUNT(*) AS publicaciones_total,
        COUNT(*) FILTER (WHERE tipo_propiedad = 'casa') AS publicaciones_casas,
        COUNT(*) FILTER (WHERE tipo_propiedad = 'departamento') AS publicaciones_departamentos,
        COUNT(*) FILTER (WHERE fuente = 'chilepropiedades') AS publicaciones_chilepropiedades,
        COUNT(*) FILTER (WHERE fuente = 'portal_inmobiliario') AS publicaciones_portal
    FROM dbt.calidad_precios_publicaciones
    WHERE tipo_propiedad IN ('casa', 'departamento')
      AND id_ubicacion IS NOT NULL
    GROUP BY id_ubicacion
)
SELECT
    i.id_ubicacion, i.cut, i.comuna, i.region,
    COALESCE(v.publicaciones_total, 0) AS publicaciones_total,
    COALESCE(v.publicaciones_casas, 0) AS publicaciones_casas,
    COALESCE(v.publicaciones_departamentos, 0) AS publicaciones_departamentos,
    COALESCE(v.publicaciones_chilepropiedades, 0) AS publicaciones_chilepropiedades,
    COALESCE(v.publicaciones_portal, 0) AS publicaciones_portal,
    i.predios_total, i.predios_habitacionales,
    i.unidades_autorizadas_2026, i.estado_permisos_2026,
    i.periodo_sii, i.anio_permisos
FROM dbt.indicadores_comunales_2026 i
LEFT JOIN viviendas v ON i.id_ubicacion = v.id_ubicacion
WHERE i.region = 'Metropolitana'
ORDER BY i.cut
"""


PRECIOS_RM = """
SELECT
    u.cut, u.comuna, p.fuente, p.tipo_propiedad, p.moneda,
    COUNT(*) AS publicaciones,
    ROUND(AVG(p.precio), 2) AS precio_promedio,
    ROUND((PERCENTILE_CONT(0.5)
      WITHIN GROUP (ORDER BY p.precio))::numeric, 2) AS precio_mediano,
    ROUND(AVG(p.precio) FILTER (
        WHERE p.estado_calidad_precio = 'sin_alerta_por_umbral'
    ), 2) AS promedio_sin_extremos,
    COUNT(*) FILTER (
        WHERE p.estado_calidad_precio IN (
          'precio_muy_bajo', 'precio_extremadamente_alto'
        )
    ) AS precios_extremos,
    COUNT(*) FILTER (WHERE p.alerta_uf) AS alertas_uf
FROM dbt.calidad_precios_publicaciones p
JOIN dbt.indicadores_comunales_2026 u
  ON p.id_ubicacion = u.id_ubicacion
WHERE u.region = 'Metropolitana'
  AND p.tipo_propiedad IN ('casa', 'departamento')
  AND p.precio > 0
GROUP BY u.cut, u.comuna, p.fuente, p.tipo_propiedad, p.moneda
ORDER BY u.comuna, p.tipo_propiedad, p.fuente, p.moneda
"""


CONSTRUCCION_TASAS = """
WITH construccion_rm AS (
    SELECT
        c.anio,
        SUM(c.unidades_total) AS unidades_autorizadas_rm,
        SUM(c.unidades_casas) AS unidades_casas_rm,
        SUM(c.unidades_departamentos) AS unidades_departamentos_rm,
        SUM(c.superficie_m2_total) AS superficie_m2_rm,
        COUNT(DISTINCT c.id_ubicacion) AS comunas_registradas,
        STRING_AGG(DISTINCT COALESCE(c.estado_dato, 'sin_estado'), '; ')
          AS estado_datos_chilehub
    FROM dw.fact_chilehub_permisos_edificacion c
    JOIN dw.dim_ubicacion u ON c.id_ubicacion = u.id_ubicacion
    WHERE u.region = 'Metropolitana'
    GROUP BY c.anio
),
banco_anual AS (
    SELECT
        EXTRACT(YEAR FROM periodo)::integer AS anio,
        ROUND(AVG(tasa_interes_vivienda_anual_pct), 3)
          AS tasa_hipotecaria_promedio_pct,
        COUNT(*) FILTER (WHERE tasa_interes_vivienda_anual_pct IS NOT NULL)
          AS meses_con_tasa,
        COUNT(*) AS meses_registrados
    FROM dw.fact_banco_central_vivienda_mensual
    GROUP BY EXTRACT(YEAR FROM periodo)::integer
)
SELECT
    c.anio, c.unidades_autorizadas_rm,
    c.unidades_casas_rm, c.unidades_departamentos_rm,
    c.superficie_m2_rm, c.comunas_registradas,
    c.estado_datos_chilehub, b.tasa_hipotecaria_promedio_pct,
    b.meses_con_tasa, b.meses_registrados
FROM construccion_rm c
LEFT JOIN banco_anual b ON c.anio = b.anio
ORDER BY c.anio
"""


CONSTRUCCION_EMPLEO = """
WITH construccion AS (
    SELECT
        id_ubicacion,
        SUM(unidades_total) AS unidades_autorizadas_2024,
        SUM(unidades_casas) AS unidades_casas_2024,
        SUM(unidades_departamentos) AS unidades_departamentos_2024,
        SUM(superficie_m2_total) AS superficie_m2_2024,
        STRING_AGG(DISTINCT estado_dato, '; ') AS estado_chilehub
    FROM dw.fact_chilehub_permisos_edificacion
    WHERE anio = 2024
    GROUP BY id_ubicacion
),
ocupacion AS (
    SELECT
        id_ubicacion,
        valor AS pct_ocupados_construccion,
        numerador AS ocupados_construccion,
        denominador AS ocupados_totales,
        estado AS estado_observatorio,
        dudoso AS dato_dudoso,
        COUNT(*) OVER (PARTITION BY id_ubicacion) AS registros_observatorio
    FROM dw.fact_observatorio_ocupados_construccion_2024
    WHERE nivel = 'comuna'
      AND periodo = 2024
      AND indicador_id = 'pct_ocupados_construccion'
)
SELECT
    u.cut, u.comuna, u.region,
    c.unidades_autorizadas_2024, c.unidades_casas_2024,
    c.unidades_departamentos_2024, c.superficie_m2_2024,
    c.estado_chilehub,
    o.pct_ocupados_construccion, o.ocupados_construccion,
    o.ocupados_totales, o.estado_observatorio,
    o.dato_dudoso, o.registros_observatorio
FROM dbt.indicadores_comunales_2026 u
LEFT JOIN construccion c ON u.id_ubicacion = c.id_ubicacion
LEFT JOIN ocupacion o ON u.id_ubicacion = o.id_ubicacion
WHERE u.region = 'Metropolitana'
ORDER BY u.cut
"""


QUERIES = {
    "indicadores_viviendas_RM_2026_powerbi.csv": VIVIENDAS_RM,
    "precios_viviendas_RM.csv": PRECIOS_RM,
    "construccion_tasas_RM_2002_2026.csv": CONSTRUCCION_TASAS,
    "construccion_empleo_RM_2024.csv": CONSTRUCCION_EMPLEO,
}

REFERENCE_ROWS = {
    "indicadores_viviendas_RM_2026_powerbi.csv": 52,
    "precios_viviendas_RM.csv": 210,
    "construccion_tasas_RM_2002_2026.csv": 25,
    "construccion_empleo_RM_2024.csv": 52,
}


def export_data(query: str) -> tuple[bytes, list[dict[str, str]]]:
    sql = f"COPY ({query.strip().rstrip(';')}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE)"
    cmd = [
        "docker", "compose", "exec", "-T", "postgres_gis",
        "sh", "-c",
        'PGCLIENTENCODING=UTF8 psql -X -q -v ON_ERROR_STOP=1 '
        '-U "$POSTGRES_USER" -d inmobiliaria_db -c "$1"',
        "sh", sql,
    ]
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace"))
    content = result.stdout.decode("utf-8")
    rows = list(csv.DictReader(io.StringIO(content)))
    if not rows:
        raise ValueError("La consulta devolvio cero filas")
    return result.stdout, rows


def validate(name: str, rows: list[dict[str, str]]) -> None:
    expected = REFERENCE_ROWS[name]
    if len(rows) != expected:
        raise ValueError(f"{name}: {len(rows)} filas; corte de referencia: {expected}")

    if name == "indicadores_viviendas_RM_2026_powerbi.csv":
        total = sum(int(r["publicaciones_total"]) for r in rows)
        casas = sum(int(r["publicaciones_casas"]) for r in rows)
        deptos = sum(int(r["publicaciones_departamentos"]) for r in rows)
        portales = sum(int(r["publicaciones_portal"]) + int(r["publicaciones_chilepropiedades"]) for r in rows)
        if total != 27600 or total != casas + deptos or total != portales:
            raise ValueError("Conteos inconsistentes entre viviendas, tipos y fuentes")
        if len({r["cut"] for r in rows}) != 52:
            raise ValueError("Codigos CUT duplicados o faltantes")
    elif name == "precios_viviendas_RM.csv":
        total = sum(int(r["publicaciones"]) for r in rows)
        if total != 27600 or len({r["cut"] for r in rows}) != 52:
            raise ValueError("Conteos de precios o cobertura comunal inconsistentes")
    elif name == "construccion_tasas_RM_2002_2026.csv":
        years = [int(r["anio"]) for r in rows]
        if years != list(range(2002, 2027)) or any(int(r["comunas_registradas"]) != 52 for r in rows):
            raise ValueError("Cobertura temporal o geografica de ChileHub inesperada")
    elif name == "construccion_empleo_RM_2024.csv":
        if len({r["cut"] for r in rows}) != 52:
            raise ValueError("Codigos CUT duplicados o faltantes")
        if any(r["registros_observatorio"] not in ("", "1") for r in rows):
            raise ValueError("Observatorio tiene duplicados por comuna")


def main() -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    prepared = {}
    print("Validando datasets del corte RM...")
    for name, query in QUERIES.items():
        data, rows = export_data(query)
        validate(name, rows)
        prepared[name] = data
        print(f"  OK: {name} ({len(rows)} filas)")

    # Se escriben los cuatro CSV solo tras completar todas las validaciones.
    for name, data in prepared.items():
        (REPORTS / name).write_bytes(b"\xef\xbb\xbf" + data)

    print("\nExportacion completada: 4 CSV UTF-8 con BOM en reports/")
    print("Fuente: DW PostgreSQL + modelos dbt; sin modificar tablas.")


if __name__ == "__main__":
    main()
