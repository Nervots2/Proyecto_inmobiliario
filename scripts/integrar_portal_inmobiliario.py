"""Consolida JSON historicos de Portal Inmobiliario e integra en Delta y DW.

Ejecutar desde la raiz del proyecto:
  python scripts/integrar_portal_inmobiliario.py --auditar
  python scripts/integrar_portal_inmobiliario.py --cargar-delta
  python scripts/integrar_portal_inmobiliario.py --cargar-dw

El archivo NO ejecuta requests ni scraping; nunca elimina los JSON.
Cada publicacion (ID MLC) se carga a lo sumo una vez en DW y Delta.
Una nueva extraccion del mismo ID no actualiza sus valores en la tabla de hechos;
las capturas historicas se conservan en los JSON crudos.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "datalake_crudo" / "portal_inmobiliario"
SOURCE = "portal_inmobiliario"
STAGE_BUCKET = "lake-procesado"
DELTA_NAME = "delta.inmobiliario.portal_inmobiliario_publicaciones"
DELTA_LOCATION = "s3a://lake-procesado/delta/portal_inmobiliario_publicaciones/"
DELTA_PREFIX = "delta/portal_inmobiliario_publicaciones/"
MAX_PRICE = Decimal("9999999999999999.99")
MAX_AREA = Decimal("9999999999.99")


def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def as_string(value):
    return str(value).strip() if value is not None else ""


def get_records(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for key in ("publicaciones", "registros", "datos", "items", "results"):
            if isinstance(obj.get(key), list):
                return obj[key]
    return None


def record_id(item):
    raw = as_string(item.get("id") or item.get("id_publicacion"))
    url = as_string(item.get("url"))
    match = re.search(r"MLC[-_ ]?(\d+)", raw, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    if raw.isdecimal():
        return raw
    match = re.search(r"MLC[-_ ]?(\d+)", url, flags=re.IGNORECASE)
    return match.group(1) if match else None


def property_type(item, filename):
    s = strip_accents(as_string(item.get("tipo_propiedad_busqueda") or item.get("tipo_propiedad"))).lower()
    if not s:
        m = re.search(r"(?:venta|arriendo)[_-](casa|casas|departamento|departamentos|oficina|industrial|comercial|sitio)", filename.lower())
        s = m.group(1) if m else ""
    if s in ("casas",):
        s = "casa"
    if s in ("departamentos", "depto", "deptos"):
        s = "departamento"
    return s or None


def region(item, filename):
    raw = " ".join(as_string(item.get(k)) for k in ("ubicacion_busqueda", "region_busqueda", "region"))
    s = strip_accents(raw + " " + filename).lower()
    if "metropolitana" in s:
        return "metropolitana"
    return strip_accents(raw).lower() or None


def parse_date(value, fallback):
    if value:
        try:
            dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc), False
        except (TypeError, ValueError, OverflowError):
            pass
    return fallback, True


def positive_decimal(raw, maximum):
    try:
        v = Decimal(str(raw))
        if not v.is_finite() or v <= 0 or v > maximum:
            return None
        return v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError):
        return None


def parse_local_number(s):
    s = s.replace("\u00a0", "").replace(" ", "").strip()
    if not re.fullmatch(r"\d[\d.,]*", s):
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".") if s.rfind(",") > s.rfind(".") else s.replace(",", "")
    elif s.count(".") > 1 or s.count(",") > 1:
        mark = "." if "." in s else ","
        parts = s.split(mark)
        if all(len(x) == 3 for x in parts[1:]):
            s = "".join(parts)
        else:
            return None
    elif "." in s or "," in s:
        mark = "." if "." in s else ","
        a, b = s.split(mark)
        s = (a + b) if len(b) == 3 else (a + "." + b)
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def parse_price(item):
    raw = as_string(item.get("precio_actual") or item.get("precio_original"))
    upper = raw.upper()
    if re.search(r"\bUF\b", upper):
        currency = "UF"
    elif "US$" in upper or re.search(r"\bUSD\b", upper):
        currency = "USD"
    elif "$" in upper or re.search(r"\bCLP\b", upper):
        currency = "CLP"
    else:
        return None, None, raw
    # Precio extraido del listado HTML: primera cantidad tras el simbolo de moneda.
    pattern = {"UF": r"\bUF\s*([\d.,\s]+)",
               "USD": r"(?:US\$|\bUSD\b)\s*([\d.,\s]+)",
               "CLP": r"(?:\bCLP\b|(?<![A-Za-z])\$)\s*([\d.,\s]+)"}[currency]
    m = re.search(pattern, upper)
    if not m:
        return None, None, raw
    v = parse_local_number(m.group(1))
    if v is None:
        return None, None, raw
    price = positive_decimal(v, MAX_PRICE)
    return price, currency if price is not None else None, raw


def parse_area(item):
    v = item.get("superficie_m2")
    if v is not None:
        if isinstance(v, (int, float, Decimal)):
            return positive_decimal(v, MAX_AREA)
        m = re.search(r"\d[\d.,]*", str(v))
        if m:
            return positive_decimal(parse_local_number(m.group()), MAX_AREA)
    attrs = item.get("atributos") or []
    if not isinstance(attrs, list):
        attrs = [str(attrs)]
    candidates = []
    for s in attrs:
        m = re.search(r"(\d[\d.,]*)\s*(?:m²|m2|metros cuadrados)", str(s), re.I)
        if m:
            v = parse_local_number(m.group(1))
            if v is not None:
                candidates.append(("total" in strip_accents(str(s)).lower(), v))
    if candidates:
        candidates.sort(reverse=True)
        return positive_decimal(candidates[0][1], MAX_AREA)
    return None


def consolidate():
    if not RAW.is_dir():
        raise FileNotFoundError(f"Falta carpeta: {RAW}")
    paths = sorted(RAW.rglob("portal*.json"))
    if not paths:
        raise FileNotFoundError(f"No hay JSON portal*.json en {RAW}")
    chosen, count = {}, Counter()
    invalid_files = []
    for path in paths:
        try:
            data = get_records(json.loads(path.read_text(encoding="utf-8-sig")))
            if data is None:
                invalid_files.append(f"Formato desconocido: {path.name}")
                continue
        except (OSError, ValueError, UnicodeError) as exc:
            invalid_files.append(f"No se pudo leer {path.name}: {exc}")
            continue
        modified = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        count["archivos"] += 1
        for item in data:
            if not isinstance(item, dict):
                count["no_diccionario"] += 1
                continue
            count["observaciones"] += 1
            identifier = record_id(item)
            if not identifier:
                count["sin_id"] += 1
                continue
            date, imputed = parse_date(item.get("fecha_extraccion"), modified)
            category = property_type(item, path.name)
            zone = region(item, path.name)
            operation = as_string(item.get("operacion_busqueda")) or ("arriendo" if "arriendo" in path.name.lower() else "venta")
            price, currency, original = parse_price(item)
            candidate = {
                "id_publicacion": identifier, "tipo_propiedad": category, "region_busqueda": zone,
                "operacion": operation.lower(), "titulo": as_string(item.get("titulo")) or None,
                "ubicacion": as_string(item.get("ubicacion")) or None,
                "precio_texto": original or None, "precio": price, "moneda": currency,
                "superficie_m2": parse_area(item),
                "fecha_extraccion": date,
                "fecha_imputada": imputed,
                "es_proyecto": bool(item.get("es_proyecto") or as_string(item.get("tipo_anuncio")).lower() == "proyecto"),
                "precio_desde": bool(item.get("precio_es_desde") or item.get("precio_desde")),
                "url": as_string(item.get("url")) or None,
                "archivo_origen": path.name,
            }
            # Semantica de la auditoria: conservar ultima observacion por ID.
            # En empate, elegir la que tenga mas atributos informativos.
            quality = sum(bool(candidate.get(x)) for x in ("titulo", "ubicacion", "precio", "tipo_propiedad", "url"))
            rank = (date, quality, path.name)
            if identifier not in chosen or rank > chosen[identifier][0]:
                chosen[identifier] = (rank, candidate)
    return [v[1] for v in sorted(chosen.values(), key=lambda x: x[1]["id_publicacion"])], count, invalid_files


def audit(rows, stats, errors):
    ventas = [r for r in rows if r["operacion"] == "venta"]
    valid_dw = [r for r in ventas if r["precio"] is not None and r["tipo_propiedad"]]
    rm = [r for r in rows if r["region_busqueda"] == "metropolitana" and r["tipo_propiedad"] in {"casa", "departamento"}]
    print("=== AUDITORÍA PORTAL INMOBILIARIO (SIN ESCRITURAS) ===")
    print(f"Archivos leídos: {stats['archivos']} | observaciones: {stats['observaciones']}")
    print(f"IDs únicos globales: {len(rows)} | sin ID: {stats['sin_id']}")
    print(f"Casas/deptos RM: {len(rm)} (la auditoria anterior indicaba 4.240)")
    print(f"Publicaciones en venta: {len(ventas)} | aptas para DW (precio y tipo): {len(valid_dw)}")
    print(f"Excluidas de DW por precio/tipo/operación: {len(rows)-len(valid_dw)}")
    print("Tipos:", dict(Counter(r["tipo_propiedad"] or "sin_tipo" for r in rows).most_common()))
    print("Monedas en venta aptas:", dict(Counter(r["moneda"] for r in valid_dw)))
    if errors:
        print("ADVERTENCIAS (primeras 10):", *errors[:10], sep="\n - ")
    return valid_dw


def load_env():
    try:
        from dotenv import load_dotenv
    except ImportError as exc:
        raise RuntimeError("Falta python-dotenv; instala dependencias antes de cargar") from exc
    load_dotenv(ROOT / ".env")


def s3_client():
    import boto3
    user = os.getenv("MINIO_ROOT_USER")
    password = os.getenv("MINIO_ROOT_PASSWORD")
    if not user or not password:
        raise RuntimeError("Faltan MINIO_ROOT_USER y MINIO_ROOT_PASSWORD en .env")
    return boto3.client("s3", endpoint_url=os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000"),
                        aws_access_key_id=user, aws_secret_access_key=password, region_name="us-east-1")


# Esquema analitico de Parquet (una fila por ID, conserva tambien registros sin precio).
STAGE_TYPES = [
    ("id_publicacion", "VARCHAR"), ("tipo_propiedad", "VARCHAR"),
    ("region_busqueda", "VARCHAR"), ("operacion", "VARCHAR"),
    ("titulo", "VARCHAR"), ("ubicacion", "VARCHAR"),
    ("precio_texto", "VARCHAR"), ("precio", "DECIMAL(18,2)"),
    ("moneda", "VARCHAR"), ("superficie_m2", "DECIMAL(12,2)"),
    ("fecha_extraccion", "VARCHAR"), ("fecha_imputada", "BOOLEAN"),
    ("es_proyecto", "BOOLEAN"), ("precio_desde", "BOOLEAN"),
    ("url", "VARCHAR"), ("archivo_origen", "VARCHAR"),
]


def parquet_bytes(rows):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("Falta pyarrow: instala 'python -m pip install pyarrow'") from exc
    types = {
        "VARCHAR": pa.string(), "BOOLEAN": pa.bool_(),
        "DECIMAL(18,2)": pa.decimal128(18, 2),
        "DECIMAL(12,2)": pa.decimal128(12, 2),
    }
    arrays = []
    for name, typ in STAGE_TYPES:
        vals = [r[name] if name != "fecha_extraccion" else r[name].isoformat() for r in rows]
        arrays.append(pa.array(vals, type=types[typ]))
    table = pa.Table.from_arrays(arrays, names=[c for c, _ in STAGE_TYPES])
    with tempfile.TemporaryFile() as f:
        pq.write_table(table, f, compression="snappy")
        f.seek(0)
        return f.read()


def trino_connection():
    from trino.dbapi import connect
    return connect(host="127.0.0.1", port=8088, user="inmobiliario", catalog="delta",
                   schema="inmobiliario", http_scheme="http")


def loading_delta(rows):
    if not rows:
        raise ValueError("No hay IDs para cargar a Delta")
    print("Preparando Parquet con IDs únicos (sin afectar los JSON originales)...")
    data = parquet_bytes(rows)
    sha = hashlib.sha256(data).hexdigest()[:16]
    prefix = f"silver/portal_inmobiliario/snapshot_{sha}/"
    key = prefix + "parte-00000.parquet"
    stage_name = f"lake.inmobiliario.portal_stage_{sha}"
    stage_url = f"s3a://{STAGE_BUCKET}/{prefix}"
    s3 = s3_client()
    s3.head_bucket(Bucket=STAGE_BUCKET)
    from botocore.exceptions import ClientError
    try:
        meta = s3.head_object(Bucket=STAGE_BUCKET, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        s3.put_object(Bucket=STAGE_BUCKET, Key=key, Body=data,
                      ContentType="application/octet-stream")
        print(f"Parquet publicado: s3://{STAGE_BUCKET}/{key}")
    else:
        if meta["ContentLength"] != len(data):
            raise RuntimeError("Existe un Parquet de nombre igual pero tamaño distinto; se aborta.")
        print("Parquet ya estaba publicado; se conserva.")
    conn = trino_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES FROM lake.inmobiliario LIKE 'portal_stage_{sha}'")
        if not cur.fetchall():
            cols = ", ".join(f"{name} {typ}" for name, typ in STAGE_TYPES)
            cur.execute(f"CREATE TABLE {stage_name} ({cols}) "
                        f"WITH (format = 'PARQUET', external_location = '{stage_url}')")
            cur.fetchall()
        cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT id_publicacion) FROM {stage_name}")
        n, unique = cur.fetchone()
        if n != len(rows) or unique != n:
            raise RuntimeError(f"Stage inválido: filas={n}, IDs únicos={unique}, esperado={len(rows)}")
        cur.execute("SHOW TABLES FROM delta.inmobiliario LIKE 'portal_inmobiliario_publicaciones'")
        columns = ", ".join(name for name, _ in STAGE_TYPES)
        if not cur.fetchall():
            if s3.list_objects_v2(Bucket=STAGE_BUCKET, Prefix=DELTA_PREFIX, MaxKeys=1).get("KeyCount", 0):
                raise RuntimeError("Hay objetos en la ruta Delta sin tabla registrada: revisar sin borrar.")
            cur.execute(f"CREATE TABLE {DELTA_NAME} WITH (location = '{DELTA_LOCATION}') "
                        f"AS SELECT {columns} FROM {stage_name}")
            cur.fetchall()
            print(f"Delta creado: {DELTA_NAME}")
        else:
            cur.execute(f"SELECT COUNT(*) FROM {DELTA_NAME}")
            before = cur.fetchone()[0]
            cur.execute(f"INSERT INTO {DELTA_NAME} ({columns}) "
                        f"SELECT {', '.join('s.' + name for name, _ in STAGE_TYPES)} FROM {stage_name} s "
                        f"LEFT JOIN {DELTA_NAME} d ON s.id_publicacion = d.id_publicacion "
                        "WHERE d.id_publicacion IS NULL")
            cur.fetchall()
            print(f"Delta: {before} filas antes de la carga.")
        cur.execute(f"SELECT COUNT(*), COUNT(DISTINCT id_publicacion) FROM {DELTA_NAME}")
        count, uniques = cur.fetchone()
        if uniques != count:
            raise RuntimeError(f"Delta contiene IDs duplicados: {count} filas, {uniques} únicos")
        print(f"Delta validado: {count} filas, {uniques} IDs únicos.")
    finally:
        conn.close()


def loading_dw(rows):
    import psycopg
    needed = [x for x in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD") if not os.getenv(x)]
    if needed:
        raise RuntimeError("Faltan en .env: " + ", ".join(needed))
    # Se carga la version mas reciente de cada ID, con fecha original de captura.
    # Solo anuncios de venta con tipo y precio válidos son hechos monetarios.
    facts = [r for r in rows if r["operacion"] == "venta" and r["tipo_propiedad"] and r["precio"] is not None]
    with psycopg.connect(host="127.0.0.1", port=5432, dbname=os.environ["POSTGRES_DB"],
                         user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"]) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dw.fact_publicaciones")
            before = cur.fetchone()[0]
            cur.execute("SELECT DISTINCT fuente FROM dw.fact_publicaciones WHERE LOWER(fuente) LIKE '%portal%'")
            prev_sources = [x[0] for x in cur.fetchall()]
            if prev_sources and prev_sources != [SOURCE]:
                raise RuntimeError(f"La fuente previa en DW es {prev_sources}. "
                                   f"Verifica su nombre antes de evitar duplicados; esperado: {SOURCE}")
            cur.execute("""CREATE TEMP TABLE portal_stage (
                            id_publicacion TEXT PRIMARY KEY, tipo_propiedad TEXT NOT NULL,
                            precio NUMERIC(18,2) NOT NULL, moneda VARCHAR(3) NOT NULL,
                            superficie_m2 NUMERIC(12,2), fecha_extraccion TIMESTAMPTZ NOT NULL,
                            fecha DATE NOT NULL, es_proyecto BOOLEAN, precio_desde BOOLEAN
                           ) ON COMMIT DROP""")
            with cur.copy("COPY portal_stage (id_publicacion, tipo_propiedad, precio, moneda, "
                          "superficie_m2, fecha_extraccion, fecha, es_proyecto, precio_desde) FROM STDIN") as writer:
                for r in facts:
                    writer.write_row((r["id_publicacion"], r["tipo_propiedad"], r["precio"], r["moneda"],
                                      r["superficie_m2"], r["fecha_extraccion"], r["fecha_extraccion"].date(),
                                      r["es_proyecto"], r["precio_desde"]))
            cur.execute("""INSERT INTO dw.dim_fecha (fecha, anio, mes, dia)
                           SELECT DISTINCT fecha, EXTRACT(YEAR FROM fecha)::int,
                           EXTRACT(MONTH FROM fecha)::int, EXTRACT(DAY FROM fecha)::int
                           FROM portal_stage ON CONFLICT (fecha) DO NOTHING""")
            cur.execute("""INSERT INTO dw.dim_propiedad (tipo_propiedad)
                           SELECT DISTINCT tipo_propiedad FROM portal_stage
                           ON CONFLICT (tipo_propiedad) DO NOTHING""")
            cur.execute("""INSERT INTO dw.fact_publicaciones
                           (id_publicacion, fuente, fecha, id_propiedad, precio, moneda,
                            superficie_m2, fecha_extraccion, es_proyecto, precio_desde)
                           SELECT e.id_publicacion, %s, e.fecha, p.id_propiedad,
                                  e.precio, e.moneda, e.superficie_m2, e.fecha_extraccion,
                                  e.es_proyecto, e.precio_desde
                           FROM portal_stage e
                           JOIN dw.dim_propiedad p ON p.tipo_propiedad = e.tipo_propiedad
                           WHERE NOT EXISTS (
                               SELECT 1 FROM dw.fact_publicaciones f
                               WHERE f.fuente = %s AND
                               regexp_replace(f.id_publicacion, '^MLC[-_ ]?', '', 'i') = e.id_publicacion
                           )
                           ON CONFLICT (fuente, id_publicacion, fecha_extraccion) DO NOTHING""", (SOURCE, SOURCE))
            inserted = cur.rowcount
            cur.execute("SELECT COUNT(*) FROM dw.fact_publicaciones")
            after = cur.fetchone()[0]
        # Confirmacion automatica solo al salir sin error de este bloque.
    print(f"DW: {before} -> {after} hechos; insertados ahora: {inserted}")
    print(f"Registros Portal aptos para DW: {len(facts)}. No se insertan IDs ya existentes.")
    print("La columna id_ubicacion permanece NULL hasta integrar la dimension geografica.")


def main():
    parser = argparse.ArgumentParser(description="Consolida Portal Inmobiliario; cargas incrementales seguras")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--auditar", action="store_true", help="Solo leer JSON y contar, sin conexiones externas")
    grp.add_argument("--cargar-delta", action="store_true", help="Publicar Parquet y agregar IDs nuevos a Delta")
    grp.add_argument("--cargar-dw", action="store_true", help="Insertar IDs no existentes en PostgreSQL")
    args = parser.parse_args()
    records, stats, errors = consolidate()
    audit(records, stats, errors)
    if errors and not args.auditar:
        raise RuntimeError("Hay archivos con errores; corregirlos antes de cargar, no ignorarlos.")
    if args.cargar_delta or args.cargar_dw:
        load_env()
        if args.cargar_delta:
            loading_delta(records)
        if args.cargar_dw:
            loading_dw(records)


if __name__ == "__main__":
    main()
