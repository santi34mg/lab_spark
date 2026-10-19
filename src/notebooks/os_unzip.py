# Databricks notebook source
# MAGIC %md
# MAGIC # OS Unzip
# MAGIC Descomprime los `.zip` de un Volume dentro del mismo Volume.
# MAGIC Lo usan `os_api_ingest` (viajes), `paradas_unzip` y `horarios_unzip`.
# MAGIC
# MAGIC Entrada: `/Volumes/<catalog>/<schema>/<volume>/<zips_subdir>/*.zip` (`zips_subdir` por defecto `zips`)
# MAGIC Salida: `/Volumes/<catalog>/<schema>/<volume>/csv/<nombre_zip>/<archivo>`
# MAGIC
# MAGIC Extrae todos los archivos del zip, no solo los CSVs (algunos zips, como el de paradas, no traen CSVs).
# MAGIC Idempotente: salta los archivos que ya existen con el mismo tamaño.

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
# viajes guarda los zips en "zips/"; paradas y horarios en "zip/"
dbutils.widgets.text("zips_subdir", "zips")
zips_subdir = dbutils.widgets.get("zips_subdir")
base_dir = f"/Volumes/{catalog}/{schema}/{volume}"
zips_dir = f"{base_dir}/{zips_subdir}"
csv_dir = f"{base_dir}/csv"

# COMMAND ----------

import os
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

zip_files = sorted(f for f in os.listdir(zips_dir) if f.endswith(".zip"))
print(f"{len(zip_files)} zips en {zips_dir}")

# COMMAND ----------


def unzip(zip_name: str) -> list[tuple[str, str]]:
    out_dir = f"{csv_dir}/{zip_name.removesuffix('.zip')}"
    os.makedirs(out_dir, exist_ok=True)
    results = []

    with zipfile.ZipFile(f"{zips_dir}/{zip_name}") as zf:
        for info in zf.infolist():
            # Se usa el basename para evitar rutas fuera de out_dir (zip-slip) y carpetas internas
            name = os.path.basename(info.filename)
            if info.is_dir():
                continue

            dest = f"{out_dir}/{name}"
            if os.path.exists(dest) and os.path.getsize(dest) == info.file_size:
                results.append((f"{zip_name}/{name}", "skipped (ya existe)"))
                continue

            tmp = dest + ".part"
            with zf.open(info) as src, open(tmp, "wb") as dst:
                shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)
            os.rename(tmp, dest)
            results.append((f"{zip_name}/{name}", f"ok ({info.file_size / 1e6:.0f} MB)"))

    if not results:
        raise ValueError(f"{zip_name} está vacío")
    return results


done, failures = [], []
with ThreadPoolExecutor(max_workers=4) as pool:
    futures = {pool.submit(unzip, z): z for z in zip_files}
    for fut in as_completed(futures):
        try:
            results = fut.result()  # relanza aquí las excepciones del worker
        except (zipfile.BadZipFile, OSError, ValueError) as e:
            failures.append((futures[fut], repr(e)))
            print(f"{futures[fut]} -> FAILED {e!r}")
            continue
        for r in results:
            done.append(r)
            print(*r, sep=" -> ")

print(f"\nArchivos procesados: {len(done)}, zips fallidos: {len(failures)}")
if failures:
    raise RuntimeError(f"Fallaron {len(failures)} zips: {failures}")
