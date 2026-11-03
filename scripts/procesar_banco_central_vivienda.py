"""Audita y transforma el Excel de tasas y flujos de vivienda del Banco Central.

Ejecutar desde la raíz de Proyecto_inmobiliario:
  python scripts/procesar_banco_central_vivienda.py --auditar
  python scripts/procesar_banco_central_vivienda.py --generar

No necesita Docker. No modifica el Excel original ni ninguna base de datos.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from openpyxl import load_workbook

ORIGEN = Path("datalake_crudo/banco_central/01_BancoCentral_Tasas_Flujos_Vivienda.xlsx")
DESTINO = Path("datalake_procesado/silver/banco_central")
ARCHIVO_CSV = DESTINO / "banco_central_vivienda_mensual_2002_2026.csv"
ARCHIVO_META = DESTINO / "banco_central_vivienda_mensual_metadatos.json"


def a_decimal(valor: object, campo: str, periodo: date, *, opcional: bool = False) -> Decimal | None:
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        if opcional:
            return None
        raise ValueError(f"{periodo}: falta {campo}")
    if isinstance(valor, bool):
        raise ValueError(f"{periodo}: {campo} no puede ser booleano")
    texto = str(valor).strip().replace(",", ".")
    try:
        numero = Decimal(texto)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{periodo}: valor no numérico de {campo}: {valor!r}") from exc
    if not numero.is_finite() or numero < 0:
        raise ValueError(f"{periodo}: {campo} fuera de rango: {valor!r}")
    if campo == "tasa" and numero >= 100:
        raise ValueError(f"{periodo}: tasa sospechosa (>=100): {valor!r}")
    return numero


def siguiente_mes(fecha: date) -> date:
    return date(fecha.year + (fecha.month == 12), 1 if fecha.month == 12 else fecha.month + 1, 1)


def extraer() -> tuple[list[dict[str, object]], dict[str, object]]:
    if not ORIGEN.is_file():
        raise FileNotFoundError(f"No se encontró el Excel: {ORIGEN} (ejecuta desde la raíz del proyecto)")

    libro = load_workbook(ORIGEN, data_only=True, read_only=True)
    try:
        if not {"Cuadro", "Metadatos", "Series"}.issubset(libro.sheetnames):
            raise ValueError(f"Faltan hojas esperadas. Encontradas: {libro.sheetnames}")
        cuadro = libro["Cuadro"]
        registros: list[dict[str, object]] = []
        fechas: set[date] = set()

        for num_fila, fila in enumerate(cuadro.iter_rows(min_row=4, values_only=True), start=4):
            v = list(fila[:3]) + [None] * 3
            periodo, tasa_cruda, flujo_crudo = v[:3]
            if periodo is None:
                continue
            if isinstance(periodo, datetime):
                periodo = periodo.date()
            if not isinstance(periodo, date):
                raise ValueError(f"Fila {num_fila}: fecha inesperada {periodo!r}")
            if periodo.day != 1:
                raise ValueError(f"Fila {num_fila}: período no comienza el día 1: {periodo}")
            if periodo in fechas:
                raise ValueError(f"Período duplicado: {periodo}")
            fechas.add(periodo)
            tasa = a_decimal(tasa_cruda, "tasa", periodo)
            flujo = a_decimal(flujo_crudo, "flujo", periodo, opcional=True)
            registros.append({"periodo": periodo, "tasa": tasa, "flujo": flujo})

        registros.sort(key=lambda r: r["periodo"])
        if not registros:
            raise ValueError("No se encontraron observaciones mensuales")
        for previo, actual in zip(registros, registros[1:]):
            if actual["periodo"] != siguiente_mes(previo["periodo"]):
                raise ValueError(f"Salto temporal entre {previo['periodo']} y {actual['periodo']}")

        disponibles = sum(r["flujo"] is not None for r in registros)
        if (len(registros), disponibles, registros[0]["periodo"], registros[-1]["periodo"]) != (
            297, 153, date(2002, 1, 1), date(2026, 9, 1)
        ):
            raise ValueError(
                f"El Excel cambió respecto de la auditoría: meses={len(registros)}, "
                f"flujos={disponibles}, intervalo={registros[0]['periodo']} a {registros[-1]['periodo']}. "
                "Revisar el archivo antes de generar el CSV."
            )

        series = []
        for fila in libro["Series"].iter_rows(min_row=2, values_only=True):
            vals = [str(x).strip() for x in fila[:2] if x is not None]
            if len(vals) == 2:
                series.append({"codigo": vals[0], "descripcion_original": vals[1]})

        meta = {
            "fuente": "Banco Central de Chile",
            "archivo_original": ORIGEN.name,
            "frecuencia": "mensual",
            "periodo_inicial": str(registros[0]["periodo"]),
            "periodo_final": str(registros[-1]["periodo"]),
            "observaciones": len(registros),
            "tasas_disponibles": len(registros),
            "flujos_disponibles": disponibles,
            "flujos_ausentes": len(registros) - disponibles,
            "series_del_excel": series,
            "nota_unidad_flujo": "La escala/unidad exacta del valor de flujo debe corroborarse en la metodología oficial antes de interpretarlo.",
            "nota_faltantes": "Los flujos ausentes se conservan como celdas vacías; no se imputan con cero.",
        }
        return registros, meta
    finally:
        libro.close()


def main() -> None:
    cli = argparse.ArgumentParser(description=__doc__)
    modos = cli.add_mutually_exclusive_group(required=True)
    modos.add_argument("--auditar", action="store_true", help="Validar sin escribir")
    modos.add_argument("--generar", action="store_true", help="Generar CSV y metadatos")
    argumentos = cli.parse_args()
    registros, meta = extraer()

    print("=== BANCO CENTRAL: TASAS Y FLUJOS DE VIVIENDA ===")
    print(f"Período: {meta['periodo_inicial']} a {meta['periodo_final']}")
    print(f"Observaciones mensuales: {meta['observaciones']}")
    print(f"Tasas disponibles: {meta['tasas_disponibles']}")
    print(f"Flujos disponibles: {meta['flujos_disponibles']}")
    print(f"Flujos vacíos (sin imputar): {meta['flujos_ausentes']}")
    print("Sin meses omitidos ni fechas duplicadas.")

    if argumentos.auditar:
        print("AUDITORÍA COMPLETA SIN ESCRITURAS.")
        return

    DESTINO.mkdir(parents=True, exist_ok=True)
    with ARCHIVO_CSV.open("w", encoding="utf-8-sig", newline="") as archivo:
        writer = csv.writer(archivo)
        writer.writerow(("periodo", "tasa_interes_vivienda_anual_pct", "flujo_vivienda_valor_original"))
        for r in registros:
            writer.writerow((r["periodo"].isoformat(), str(r["tasa"]), "" if r["flujo"] is None else str(r["flujo"])))
    ARCHIVO_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"CSV generado: {ARCHIVO_CSV}")
    print(f"Metadatos generados: {ARCHIVO_META}")
    print("No se modificaron el Excel original, Silo/MinIO, Delta Lake ni PostgreSQL.")


if __name__ == "__main__":
    main()
