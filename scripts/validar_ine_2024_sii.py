"""Valida los códigos CUT del INE Censo 2024 contra el SII 2026-S1.

Ejecutar desde la raíz de Proyecto_inmobiliario:
    python scripts/validar_ine_2024_sii.py

Lee el ZIP original (extrae temporalmente ~1.2 GB), consulta únicamente las
columnas administrativas de la capa comunal del GeoPackage con sqlite3 y
crea un reporte CSV pequeño de correspondencias verificadas. No modifica
PostgreSQL, el GeoPackage original, los ZIP ni los CSV del SII.

Si el disco C: tiene poco espacio, usar --temp-dir con otra unidad:
    python scripts/validar_ine_2024_sii.py --temp-dir D:\\Temp
"""
from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
import re
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unicodedata
import zipfile

RAIZ = Path(__file__).resolve().parent.parent
CARPETA_INE = RAIZ / "datalake_crudo/ine"
CSV_SII = RAIZ / "datalake_procesado/silver/sii/sii_destinos_comuna_2026_s1_nacional.csv"
SALIDA = RAIZ / "datalake_procesado/silver/ine/ine_comunas_2024_vs_sii_2026_s1.csv"

# Correspondencia de nombres de regiones del SII con los códigos regionales
# vigentes. Sirve para VALIDAR el código que viene del INE, no para crearlo.
CODIGOS_REGION_SII = {
    "arica y parinacota": 15,
    "tarapaca": 1,
    "antofagasta": 2,
    "atacama": 3,
    "coquimbo": 4,
    "valparaiso": 5,
    "metropolitana": 13,
    "o higgins": 6,
    "maule": 7,
    "nuble": 16,
    "biobio": 8,
    "la araucania": 9,
    "los rios": 14,
    "los lagos": 10,
    "aysen": 11,
    "magallanes": 12,
}


def normalizar(texto: object) -> str:
    s = unicodedata.normalize("NFKD", str(texto or "").casefold())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def normalizar_comuna(texto: object) -> str:
    s = normalizar(texto)
    # Denominación histórica usada por el SII: no es una comuna distinta.
    if s == "cabo de hornos ex navarino":
        return "cabo de hornos"
    return s


def validar_codigo(valor: object, largo: int) -> str | None:
    try:
        txt = str(valor).strip()
        if not re.fullmatch(r"\d+(?:\.0+)?", txt):
            return None
        entero = int(float(txt))
        if entero < 0 or len(str(entero)) > largo:
            return None
        return str(entero).zfill(largo)
    except (ValueError, TypeError):
        return None


def identificador_sql(nombre: str) -> str:
    return '"' + nombre.replace('"', '""') + '"'


def leer_sii() -> list[dict]:
    if not CSV_SII.is_file():
        raise FileNotFoundError(f"No existe el CSV del SII: {CSV_SII}")
    with CSV_SII.open("r", encoding="utf-8-sig", newline="") as f:
        lector = csv.DictReader(f)
        cols = set(lector.fieldnames or [])
        if not {"region", "comuna", "periodo", "predios_total"} <= cols:
            raise ValueError(f"Columnas incompletas en SII: {sorted(cols)}")
        filas = list(lector)
    if len(filas) != 346 or any(x["periodo"] != "2026-S1" for x in filas):
        raise ValueError("CSV SII inesperado: se esperaban 346 comunas del periodo 2026-S1")
    if len({(normalizar(x['region']), normalizar_comuna(x['comuna'])) for x in filas}) != 346:
        raise ValueError("Hay comunas/regiones duplicadas en el CSV SII")
    return filas


def extraer_gpkg_temporal(zip_path: Path, directorio: Path) -> Path:
    with zipfile.ZipFile(zip_path) as z:
        gpkg = [f for f in z.infolist() if f.filename.lower().endswith(".gpkg")]
        if len(gpkg) != 1:
            raise ValueError(f"Se esperaba un GeoPackage en el ZIP: {[i.filename for i in gpkg]}")
        info = gpkg[0]
        libre = shutil.disk_usage(directorio).free
        # Reserva adicional para no saturar el disco.
        if libre < info.file_size + 256 * 1024**2:
            raise OSError(
                f"Espacio insuficiente: GeoPackage {info.file_size / 1024**3:.2f} GiB; "
                f"libre {libre / 1024**3:.2f} GiB. Usa --temp-dir en otra unidad."
            )
        destino = directorio / "ine_2024_temporal.gpkg"
        print(f"Extrayendo temporalmente {info.file_size / 1024**2:,.0f} MiB; se eliminará al terminar.", flush=True)
        copiados = 0
        siguiente_aviso = 256 * 1024**2
        with z.open(info) as origen, destino.open("wb") as salida:
            while True:
                bloque = origen.read(8 * 1024**2)
                if not bloque:
                    break
                salida.write(bloque)
                copiados += len(bloque)
                if copiados >= siguiente_aviso:
                    print(f"  Extraídos {copiados / 1024**2:,.0f} MiB...", flush=True)
                    siguiente_aviso += 256 * 1024**2
        if copiados != info.file_size:
            raise IOError("La extracción quedó incompleta")
        return destino


def leer_capa_comunal(gpkg: Path) -> tuple[str, list[dict]]:
    # Se consulta una copia temporal; nunca se modifica el ZIP de origen.
    conexion = sqlite3.connect(str(gpkg))
    try:
        contenidos = conexion.execute(
            "SELECT table_name FROM gpkg_contents WHERE data_type='features'"
        ).fetchall()
        candidatos = []
        for (tabla,) in contenidos:
            columnas = {
                c[1].upper(): c[1]
                for c in conexion.execute(f"PRAGMA table_info({identificador_sql(tabla)})")
            }
            if {"CUT", "COD_REGION", "REGION", "COMUNA"} <= set(columnas):
                candidatos.append((tabla, columnas))
        especificas = [(t, c) for t, c in candidatos if "comunal" in normalizar(t)]
        if len(especificas) == 1:
            tabla, cols = especificas[0]
        elif len(candidatos) == 1:
            tabla, cols = candidatos[0]
        else:
            raise ValueError(
                "No se pudo identificar una sola capa comunal. "
                f"Candidatas: {[t for t, _ in candidatos]}; "
                f"capas del GeoPackage: {[t for (t,) in contenidos]}"
            )
        sel = ", ".join(identificador_sql(cols[x]) for x in ("CUT", "COD_REGION", "REGION", "COMUNA"))
        sql = f"SELECT DISTINCT {sel} FROM {identificador_sql(tabla)}"
        filas = []
        for cut, cod_region, region, comuna in conexion.execute(sql):
            filas.append({
                "cut": validar_codigo(cut, 5),
                "cod_region": validar_codigo(cod_region, 2),
                "region": str(region or "").strip(),
                "comuna": str(comuna or "").strip(),
            })
        return tabla, filas
    finally:
        conexion.close()


def comparar(ine: list[dict], sii: list[dict]) -> list[dict]:
    por_nombre = defaultdict(list)
    codigos = Counter()
    for x in ine:
        por_nombre[normalizar_comuna(x["comuna"])].append(x)
        if x["cut"]:
            codigos[x["cut"]] += 1

    reporte = []
    for x in sii:
        region_esperada = CODIGOS_REGION_SII.get(normalizar(x["region"]))
        clave = normalizar_comuna(x["comuna"])
        candidatas = por_nombre.get(clave, [])
        estado = "sin_coincidencia"
        seleccionada = None
        if region_esperada is None:
            estado = "region_sii_desconocida"
        elif len(candidatas) > 1:
            estado = "coincidencia_ambigua"
        elif len(candidatas) == 1:
            candidata = candidatas[0]
            if not candidata["cut"] or not candidata["cod_region"]:
                estado = "codigo_ine_invalido"
            elif int(candidata["cod_region"]) != region_esperada:
                estado = "region_no_coincide"
            elif candidata["cut"][:2] != candidata["cod_region"]:
                estado = "cut_region_no_coincide"
            elif codigos[candidata["cut"]] != 1:
                estado = "codigo_cut_duplicado"
            else:
                estado = "validado"
                seleccionada = candidata
        reporte.append({
            "region_sii": x["region"],
            "comuna_sii": x["comuna"],
            "predios_total_sii": x["predios_total"],
            "periodo_sii": x["periodo"],
            "estado": estado,
            # Solo asignamos códigos si superaron todas las validaciones.
            "codigo_region_ine": seleccionada["cod_region"] if seleccionada else "",
            "codigo_comuna_ine": seleccionada["cut"] if seleccionada else "",
            "region_ine": seleccionada["region"] if seleccionada else "",
            "comuna_ine": seleccionada["comuna"] if seleccionada else "",
        })
    return reporte


def encontrar_zip_ine() -> Path:
    candidatos = [
        p for p in CARPETA_INE.glob("*.zip")
        if "2024" in p.name.lower() and "gpkg" in p.name.lower()
    ]
    if len(candidatos) != 1:
        raise FileNotFoundError(
            f"Se esperaba un ZIP del Censo 2024 con GPKG en {CARPETA_INE}; "
            f"encontrados: {[p.name for p in candidatos]}"
        )
    return candidatos[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--temp-dir", type=Path, default=None, help="Carpeta existente con >=1.5 GB libres para extracción temporal")
    args = parser.parse_args()
    zip_ine = encontrar_zip_ine()
    sii = leer_sii()
    print("=== INE 2024 vs SII 2026-S1: VALIDACIÓN COMUNAL ===", flush=True)
    print(f"ZIP: {zip_ine.name}", flush=True)
    print("SII: 346 comunas. PostgreSQL NO será modificado.", flush=True)
    if args.temp_dir and not args.temp_dir.is_dir():
        raise SystemExit(f"No existe --temp-dir: {args.temp_dir}")
    with tempfile.TemporaryDirectory(prefix="ine2024_", dir=args.temp_dir) as tmp:
        gpkg = extraer_gpkg_temporal(zip_ine, Path(tmp))
        tabla, ine = leer_capa_comunal(gpkg)
        print(f"Capa encontrada: {tabla} | registros distintos en la capa: {len(ine)}", flush=True)
        reporte = comparar(ine, sii)
    resumen = Counter(x["estado"] for x in reporte)
    print(f"Coincidencias seguras: {resumen.get('validado', 0)} / 346")
    for estado, n in sorted(resumen.items()):
        print(f"  {estado}: {n}")
    pendientes = [x for x in reporte if x["estado"] != "validado"]
    if pendientes:
        print("Pendientes de resolver antes de modificar PostgreSQL:")
        for x in pendientes:
            print(f"  {x['region_sii']} / {x['comuna_sii']} -> {x['estado']}")
    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    with SALIDA.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(reporte[0]))
        writer.writeheader()
        writer.writerows(reporte)
    print(f"Reporte guardado: {SALIDA.relative_to(RAIZ)}")
    print("El GeoPackage temporal se eliminó; PostgreSQL y ZIP originales intactos.")


if __name__ == "__main__":
    main()
