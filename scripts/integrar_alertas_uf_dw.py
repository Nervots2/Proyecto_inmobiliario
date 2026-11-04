"""Integra alertas historicas UF de ChilePropiedades sin corregir precios.

Desde la raiz del proyecto:
  python scripts/integrar_alertas_uf_dw.py --auditar
  python scripts/integrar_alertas_uf_dw.py --cargar

--auditar: solo SELECT (cero escrituras).
--cargar: crea/actualiza exclusivamente dw.alertas_uf_chilepropiedades,
           tras verificar el CSV frente a Delta Lake y a fact_publicaciones.
Se exige la fotografia de 234 candidatas y 28.307 publicaciones en ChilePropiedades.
"""

from __future__ import annotations

import argparse
import csv
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
REPORTE = RAIZ / 'reports/auditorias/calidad_precios/precios_uf_candidatos_revision.csv'
N_CANDIDATAS = 234
N_DELTA = 28_307
N_DW = 28_307
TOLERANCIA_UF = Decimal('0.000001')


def exigir(condicion, detalle):
    if not condicion:
        raise RuntimeError(detalle)


def decimal(valor, etiqueta):
    try:
        salida = Decimal(str(valor))
    except (ValueError, InvalidOperation, TypeError) as exc:
        raise RuntimeError(f'{etiqueta} no es un numero valido: {valor!r}') from exc
    exigir(salida.is_finite(), f'{etiqueta}: valor no finito')
    return salida


def leer_reporte():
    exigir(REPORTE.is_file(), f'No existe el reporte: {REPORTE}')
    with REPORTE.open('r', encoding='utf-8-sig', newline='') as f:
        lector = csv.DictReader(f)
        obligatorias = {'url', 'clp_original', 'uf_original', 'uf_en_titulo_candidata',
                        'titulo', 'criterio', 'estado', 'archivo_bronze', 'linea_bronze'}
        exigir(obligatorias.issubset(lector.fieldnames or []),
               f'Columnas faltantes en CSV: {obligatorias - set(lector.fieldnames or [])}')
        filas = list(lector)
    exigir(len(filas) == N_CANDIDATAS,
           f'El reporte tiene {len(filas)} registros; se esperaban {N_CANDIDATAS}. Revisar antes de cargar.')
    urls = {}
    for fila in filas:
        url = (fila['url'] or '').strip()
        exigir(url and url not in urls, f'URL vacia o duplicada en reporte: {url!r}')
        uf_original = decimal(fila['uf_original'], f'uf_original {url}')
        uf_titulo = decimal(fila['uf_en_titulo_candidata'], f'uf titulo {url}')
        clp = decimal(fila['clp_original'], f'CLP original {url}')
        exigir(clp > 0 and uf_original >= 0 and uf_titulo > uf_original,
               f'No cumple la condicion basica de candidata: {url}')
        exigir(fila['estado'] == 'requiere_revision_no_aplicar',
               f'Estado inesperado para {url}: {fila["estado"]!r}')
        urls[url] = (fila, clp, uf_original, uf_titulo)
    return urls


def consultar_delta():
    from trino.dbapi import connect
    conexion = connect(host='127.0.0.1', port=8088, user='inmobiliario',
                       catalog='delta', schema='inmobiliario', http_scheme='http')
    try:
        cur = conexion.cursor()
        cur.execute('''SELECT id_publicacion, url, precio_clp, precio_uf
                       FROM delta.inmobiliario.publicaciones_delta_final''')
        por_url = {}
        ids = set()
        n = 0
        while True:
            lote = cur.fetchmany(1000)
            if not lote:
                break
            for identificador, url, clp, uf in lote:
                n += 1
                exigir(url and url not in por_url, f'URL ausente o duplicada en Delta: {url!r}')
                identificador = str(identificador)
                exigir(identificador not in ids, f'ID duplicado en Delta: {identificador}')
                ids.add(identificador)
                por_url[url] = (identificador, clp, uf)
        exigir(n == N_DELTA, f'Delta tiene {n} publicaciones; se esperaban {N_DELTA}')
        return por_url
    finally:
        conexion.close()


def conectar_postgres():
    import psycopg
    from dotenv import load_dotenv
    load_dotenv(RAIZ / '.env')
    variables = ('POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD')
    exigir(all(os.getenv(k) for k in variables), 'Falta POSTGRES_DB/USER/PASSWORD en .env')
    return psycopg.connect(host='127.0.0.1', port=5432,
                           dbname=os.getenv('POSTGRES_DB'),
                           user=os.getenv('POSTGRES_USER'),
                           password=os.getenv('POSTGRES_PASSWORD'), connect_timeout=10)


def cruzar(urls, delta, conexion):
    with conexion.cursor() as cur:
        cur.execute('''SELECT id_registro, id_publicacion, precio, moneda
                       FROM dw.fact_publicaciones WHERE fuente = 'chilepropiedades' ''')
        por_id = {}
        for id_registro, id_publicacion, precio, moneda in cur.fetchall():
            identificador = str(id_publicacion)
            exigir(identificador not in por_id, f'ID duplicado en DW: {identificador}')
            por_id[identificador] = (id_registro, precio, moneda)
    exigir(len(por_id) == N_DW,
           f'DW tiene {len(por_id)} publicaciones ChilePropiedades; esperadas {N_DW}')
    exigir(set(por_id) == {fila[0] for fila in delta.values()},
           'Los conjuntos de ID Delta y DW son diferentes')

    plan = []
    for url, (fila, clp_original, uf_original, uf_propuesta) in urls.items():
        exigir(url in delta, f'URL del reporte no encontrada en Delta: {url}')
        identificador, clp_delta, uf_delta = delta[url]
        exigir(clp_delta is not None and uf_delta is not None,
               f'Precio incompleto en Delta para {url}')
        exigir(decimal(clp_delta, 'CLP Delta') == clp_original,
               f'CLP CSV != Delta en {url}')
        exigir(abs(decimal(uf_delta, 'UF Delta') - uf_original) <= TOLERANCIA_UF,
               f'UF CSV != Delta en {url}')
        id_registro, precio_dw, moneda = por_id[identificador]
        exigir(moneda == 'CLP' and decimal(precio_dw, 'precio DW') == clp_original,
               f'CLP Delta != DW o moneda inesperada en {url}')
        plan.append((int(id_registro), identificador, url, fila['titulo'],
                     clp_original, uf_original, uf_propuesta, fila['criterio'],
                     fila['archivo_bronze'], int(fila['linea_bronze'])))
    exigir(len(plan) == N_CANDIDATAS and len({fila[0] for fila in plan}) == N_CANDIDATAS,
           'Plan no contiene 234 registros unicos del DW')
    return sorted(plan)


def auditar_tabla_existente(conexion):
    with conexion.cursor() as cur:
        cur.execute("SELECT to_regclass('dw.alertas_uf_chilepropiedades')")
        existente = cur.fetchone()[0] is not None
        if existente:
            cur.execute('SELECT count(*) FROM dw.alertas_uf_chilepropiedades')
            print('Alertas previamente registradas en DW:', cur.fetchone()[0])
        else:
            print('Tabla de alertas DW aun no existe.')
    return existente


def cargar(conexion, plan):
    # Se transacciona solo la tabla de alertas; no se edita dw.fact_publicaciones.
    with conexion.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS dw.alertas_uf_chilepropiedades (
            id_registro BIGINT PRIMARY KEY REFERENCES dw.fact_publicaciones(id_registro),
            id_publicacion TEXT NOT NULL UNIQUE,
            url TEXT NOT NULL UNIQUE,
            titulo TEXT NOT NULL,
            precio_clp_original NUMERIC(18,2) NOT NULL,
            precio_uf_original NUMERIC(20,6) NOT NULL,
            precio_uf_titulo_candidata NUMERIC(20,6) NOT NULL,
            criterio TEXT NOT NULL,
            archivo_bronze TEXT NOT NULL,
            linea_bronze INTEGER NOT NULL,
            estado TEXT NOT NULL DEFAULT 'requiere_revision_no_aplicar'
                CHECK (estado = 'requiere_revision_no_aplicar'),
            registrado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )''')
        cur.executemany('''INSERT INTO dw.alertas_uf_chilepropiedades (
            id_registro, id_publicacion, url, titulo, precio_clp_original,
            precio_uf_original, precio_uf_titulo_candidata, criterio,
            archivo_bronze, linea_bronze)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id_registro) DO NOTHING''', plan)
        cur.execute('''SELECT id_registro, id_publicacion, url, titulo,
                             precio_clp_original, precio_uf_original,
                             precio_uf_titulo_candidata, criterio,
                             archivo_bronze, linea_bronze
                      FROM dw.alertas_uf_chilepropiedades''')
        registradas = sorted(cur.fetchall())
        exigir(registradas == plan,
               f'Las alertas existentes no coinciden con el reporte: {len(registradas)} vs {len(plan)}')
        cur.execute('SELECT count(*) FROM dw.fact_publicaciones')
        exigir(cur.fetchone()[0] == 35916,
               'La cantidad de publicaciones se desvio del total esperado')
    print('CARGA CONFIRMADA: 234 alertas en dw.alertas_uf_chilepropiedades.')
    print('No se cambiaron precios ni se altero dw.fact_publicaciones.')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--auditar', action='store_true')
    g.add_argument('--cargar', action='store_true')
    args = p.parse_args()

    print('=== ALERTAS UF CHILEPROPIEDADES: REPORTE / DELTA / DW ===')
    urls = leer_reporte()
    print('Candidatas CSV:', len(urls))
    delta = consultar_delta()
    print('Registros Delta ChilePropiedades:', len(delta))

    with conectar_postgres() as conn:
        plan = cruzar(urls, delta, conn)
        print('Conciliadas por URL, ID y precios originales:', len(plan))
        auditar_tabla_existente(conn)
        if args.cargar:
            cargar(conn, plan)
        else:
            print('AUDITORIA COMPLETADA SIN ESCRITURAS. Para integrar: --cargar')


if __name__ == '__main__':
    main()
