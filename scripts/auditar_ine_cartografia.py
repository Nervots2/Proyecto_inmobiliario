"""Auditoría de solo lectura del DBF de la cartografía comunal INE 2017.

Ejecutar desde la raíz de Proyecto_inmobiliario:
    python scripts/auditar_ine_cartografia.py

No necesita geopandas, fiona ni descomprimir el ZIP.
"""
from pathlib import Path
import zipfile

ZIP_PATH = Path('datalake_crudo/ine/ine_cartografia_comunas_censo2017_shp.zip')


def main():
    if not ZIP_PATH.is_file():
        raise SystemExit(f'No se encontró el archivo: {ZIP_PATH}\nEjecuta desde la raíz del proyecto.')

    with zipfile.ZipFile(ZIP_PATH) as archivo:
        dbfs = [n for n in archivo.namelist() if n.lower().endswith('.dbf')]
        if not dbfs:
            raise SystemExit('El ZIP no contiene archivos .dbf.')
        print(f'ZIP: {ZIP_PATH.name}')
        for dbf in dbfs:
            with archivo.open(dbf) as f:
                cabecera = f.read(32)
                if len(cabecera) != 32:
                    print(f'  {dbf}: cabecera DBF incompleta; se omite.')
                    continue
                registros = int.from_bytes(cabecera[4:8], 'little')
                largo_cabecera = int.from_bytes(cabecera[8:10], 'little')
                largo_fila = int.from_bytes(cabecera[10:12], 'little')
                descriptores = f.read(largo_cabecera - 32)
                campos = []
                for i in range(0, len(descriptores) - 1, 32):
                    d = descriptores[i:i + 32]
                    if len(d) < 32 or d[0] == 13:
                        break
                    nombre = d[:11].split(b'\0')[0].decode('latin1').strip()
                    tipo = chr(d[11])
                    longitud = d[16]
                    campos.append((nombre, tipo, longitud))
                print(f'\nDBF: {dbf}')
                print(f'Cantidad de registros: {registros}')
                print(f'Campos ({len(campos)}):')
                for nombre, tipo, largo in campos:
                    print(f'  {nombre} | tipo={tipo} | longitud={largo}')
                print('\nPrimeras 3 filas (valores sin transformar):')
                encontrados = 0
                for _ in range(min(registros, 10)):
                    fila = f.read(largo_fila)
                    if len(fila) != largo_fila:
                        break
                    if fila[:1] == b'*':
                        continue
                    pos = 1
                    valores = []
                    for nombre, _, largo in campos:
                        valor = fila[pos:pos + largo].decode('cp1252', errors='replace').strip()
                        valores.append(f'{nombre}={valor}')
                        pos += largo
                    print('  ' + ' | '.join(valores))
                    encontrados += 1
                    if encontrados == 3:
                        break


if __name__ == '__main__':
    main()
