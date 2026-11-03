"""Auditoría conservadora de vinculación geográfica de publicaciones inmobiliarias.

Lee, sin modificarlos:
- delta.inmobiliario.publicaciones_delta_final (ChilePropiedades)
- delta.inmobiliario.portal_inmobiliario_publicaciones (Portal Inmobiliario)
- dw.dim_ubicacion (PostgreSQL; códigos CUT validados con INE)

Ejecutar desde la raíz del proyecto, con Docker y Conda activos:
    python scripts/auditar_vinculacion_geografica_publicaciones.py
    python scripts/auditar_vinculacion_geografica_publicaciones.py --guardar-reportes

Sin --guardar-reportes, no se escribe ningún archivo. Nunca modifica tablas.
Las coincidencias son CANDIDATAS: requieren revisión antes de actualizar el DW.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports" / "auditorias" / "geografia_publicaciones"


def normalizar(texto: object) -> str:
    t = unicodedata.normalize("NFKD", str(texto or "").casefold())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", t).split())


def nombre_equivalente(texto: object) -> str:
    t = normalizar(texto)
    # Solo equivalencias documentadas en la conciliación SII/INE.
    equivalencias = {
        "paihuano": "paiguano",
        "trehuaco": "treguaco",
        "marchigue": "marchihue",
        "cabo de hornos ex navarino": "cabo de hornos",
    }
    return equivalencias.get(t, t)


def conectar_postgres():
    import psycopg
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    variables = ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
    faltantes = [v for v in variables if not os.getenv(v)]
    if faltantes:
        raise RuntimeError("Variables ausentes en .env: " + ", ".join(faltantes))
    return psycopg.connect(
        host="127.0.0.1", port=5432,
        dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"], connect_timeout=10,
    )


def conectar_trino():
    from trino.dbapi import connect
    return connect(host="127.0.0.1", port=8088, user="inmobiliario",
                   catalog="delta", schema="inmobiliario", http_scheme="http")


def catalogo_ubicaciones():
    conexion = conectar_postgres()
    try:
        with conexion.cursor() as cur:
            cur.execute("""SELECT id_ubicacion, codigo_comuna, codigo_region, comuna, region
                        FROM dw.dim_ubicacion ORDER BY codigo_comuna""")
            datos = cur.fetchall()
    finally:
        conexion.close()

    if len(datos) != 346:
        raise RuntimeError(f"Se esperaban 346 comunas en dw.dim_ubicacion; hay {len(datos)}")
    por_nombre = defaultdict(set)
    regiones = {}
    catalogo = {}
    for loc_id, cut, cod_region, comuna, region in datos:
        if not cut or not cod_region:
            raise RuntimeError("La dimensión geográfica contiene códigos CUT/región nulos")
        cut, cod_region = str(cut).strip().zfill(5), str(cod_region).strip().zfill(2)
        if cut[:2] != cod_region:
            raise RuntimeError(f"Región inconsistente para CUT {cut}")
        if cut in catalogo:
            raise RuntimeError(f"Código CUT repetido: {cut}")
        ficha = {"id_ubicacion": int(loc_id), "cut": cut, "codigo_region": cod_region,
                 "comuna": str(comuna), "region": str(region)}
        catalogo[cut] = ficha
        por_nombre[nombre_equivalente(comuna)].add(cut)
        regiones[normalizar(region)] = cod_region
    return catalogo, por_nombre, regiones


def analizar_chileprop(slug, por_nombre, catalogo):
    clave = nombre_equivalente(slug)
    if not clave:
        return "sin_texto", None, ""
    candidatos = por_nombre.get(clave, set())
    if not candidatos:
        return "sin_coincidencia", None, ""
    if len(candidatos) != 1:
        return "ambiguo", None, ";".join(sorted(candidatos))
    cut = next(iter(candidatos))
    return "candidato_unico", catalogo[cut], cut


def analizar_portal(region_buscada, ubicacion, por_nombre, regiones, catalogo):
    if not normalizar(ubicacion):
        return "sin_texto", None, ""
    region_key = normalizar(region_buscada)
    # No usamos una búsqueda regional como prueba suficiente de ubicación.
    # Es solamente una comprobación adicional para una comuna identificada.
    cod_region = regiones.get(region_key)
    if cod_region is None:
        return "region_no_reconocida", None, ""

    partes = [nombre_equivalente(p) for p in str(ubicacion).split(",")]
    candidatos = set()
    for parte in partes:
        candidatos.update(por_nombre.get(parte, set()))
    if not candidatos:
        return "sin_coincidencia", None, ""
    compatibles = {cut for cut in candidatos if catalogo[cut]["codigo_region"] == cod_region}
    if not compatibles:
        return "region_incompatible", None, ";".join(sorted(candidatos))
    if len(compatibles) != 1 or len(candidatos) != 1:
        # Más de una comuna exacta en los segmentos: no elegir arbitrariamente.
        return "ambiguo", None, ";".join(sorted(candidatos))
    cut = next(iter(compatibles))
    return "candidato_unico", catalogo[cut], cut


def consulta_agrupada(sql):
    conexion = conectar_trino()
    try:
        cur = conexion.cursor()
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conexion.close()


def reporte_fuente(fuente, grupos, clasificador):
    salidas = []
    resumen = Counter()
    casos = defaultdict(list)
    for campos, cantidad in grupos:
        cantidad = int(cantidad)
        estado, ficha, candidatos = clasificador(*campos)
        resumen[estado] += cantidad
        fila = {"fuente": fuente, "registros": cantidad, "estado": estado,
                "codigo_comuna": ficha["cut"] if ficha else "",
                "id_ubicacion": ficha["id_ubicacion"] if ficha else "",
                "comuna_ine_sii": ficha["comuna"] if ficha else "",
                "candidatos_cut": candidatos}
        if fuente == "chilepropiedades":
            fila["comuna_slug"] = campos[0]
        else:
            fila["region_busqueda"] = campos[0]
            fila["ubicacion"] = campos[1]
        salidas.append(fila)
        if estado != "candidato_unico":
            casos[estado].append((cantidad, campos))
    total = sum(resumen.values())
    print(f"\n=== {fuente.upper()} ===")
    print(f"Publicaciones: {total:,} | grupos geográficos distintos: {len(grupos):,}")
    for estado, cantidad in resumen.most_common():
        print(f"  {estado:22} {cantidad:>7,} ({cantidad/total*100:.1f} %)" if total else f"  {estado}: 0")
    print("  Ejemplos pendientes (los más frecuentes):")
    for estado in sorted(casos):
        for n, campos in sorted(casos[estado], reverse=True)[:3]:
            print(f"    {estado}: {campos!r} — {n} publicaciones")
    return salidas, resumen


def escribir_csv(destino, filas):
    if not filas:
        return
    columnas = ["fuente", "registros", "estado", "codigo_comuna", "id_ubicacion",
                "comuna_ine_sii", "candidatos_cut", "comuna_slug",
                "region_busqueda", "ubicacion"]
    with destino.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columnas)
        w.writeheader()
        w.writerows(filas)


def main():
    parser = argparse.ArgumentParser(description="Auditoría geográfica (sin cambios en bases de datos)")
    parser.add_argument("--guardar-reportes", action="store_true",
                        help="Exportar reportes CSV locales a reports/auditorias/")
    args = parser.parse_args()

    catalogo, por_nombre, regiones = catalogo_ubicaciones()
    print("=== AUDITORÍA DE VINCULACIÓN GEOGRÁFICA (SIN ESCRITURAS A BD) ===")
    print(f"Comunas oficiales cargadas: {len(catalogo)}")

    chile = consulta_agrupada("""SELECT COALESCE(comuna_slug, ''), COUNT(*)
                            FROM delta.inmobiliario.publicaciones_delta_final
                            GROUP BY 1""")
    portal = consulta_agrupada("""SELECT COALESCE(region_busqueda, ''),
                                      COALESCE(ubicacion, ''), COUNT(*)
                              FROM delta.inmobiliario.portal_inmobiliario_publicaciones
                              GROUP BY 1, 2""")

    salida_chile, resumen_chile = reporte_fuente(
        "chilepropiedades", [((slug,), n) for slug, n in chile],
        lambda slug: analizar_chileprop(slug, por_nombre, catalogo))
    salida_portal, resumen_portal = reporte_fuente(
        "portal_inmobiliario", [((region, ubicacion), n) for region, ubicacion, n in portal],
        lambda region, ubicacion: analizar_portal(region, ubicacion, por_nombre, regiones, catalogo))

    total = sum(resumen_chile.values()) + sum(resumen_portal.values())
    potencial = resumen_chile["candidato_unico"] + resumen_portal["candidato_unico"]
    print("\n=== RESUMEN ===")
    print(f"Total analizado: {total:,} | candidatas de vinculación exacta: {potencial:,} "
          f"| pendientes: {total - potencial:,}")
    print("ATENCIÓN: son coincidencias candidatas; no se actualizó id_ubicacion.")
    if total != 35_916:
        print("AVISO: el total en Delta difiere de 35.916. Revisar antes de avanzar.")
    if args.guardar_reportes:
        OUT.mkdir(parents=True, exist_ok=True)
        escribir_csv(OUT / "candidatos_chilepropiedades.csv", salida_chile)
        escribir_csv(OUT / "candidatos_portal_inmobiliario.csv", salida_portal)
        print(f"Reportes locales guardados en {OUT.relative_to(ROOT)}")
    else:
        print("No se escribieron archivos. Para generar reportes: --guardar-reportes")


if __name__ == "__main__":
    main()
