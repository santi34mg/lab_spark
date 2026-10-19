# Databricks notebook source
# MAGIC %md
# MAGIC # OS API Ingest
# MAGIC One Shot job: descarga todos los `.zip` de "Viajes en ómnibus STM" del portal de datos abiertos
# MAGIC de la Intendencia de Montevideo hacia un Volume de Unity Catalog.
# MAGIC
# MAGIC Fuente: catálogo CKAN de la IM (`ckan.montevideo.gub.uy`), dataset `viajes-realizados-en-los-omnibus-del-sistema-de-transporte-metropolitano-stm`.
# MAGIC Los recursos se descubren por API, por lo que el job cubre todos los años publicados. Es idempotente: salta archivos ya descargados.

# COMMAND ----------

import os
import re
import time
import unicodedata
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
target_dir = f"/Volumes/{catalog}/{schema}/{volume}/zips"

CKAN_PACKAGE_URL = (
    "https://ckan.montevideo.gub.uy/api/3/action/package_show"
    "?id=viajes-realizados-en-los-omnibus-del-sistema-de-transporte-metropolitano-stm"
)

# COMMAND ----------
os.makedirs(target_dir, exist_ok=True)


# El WAF del portal devuelve 403 (HTML) al User-Agent por defecto de python-requests
MAX_ATTEMPTS = 5
http = requests.Session()
http.headers["User-Agent"] = "Mozilla/5.0 (compatible; lab-databricks-ingest/1.0)"


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def is_zip(url: str) -> bool:
    """Los links de imnube no tienen extensión; se consulta el Content-Type con un range de 1 byte."""
    with http.get(url, headers={"Range": "bytes=0-0"}, stream=True, timeout=60) as r:
        r.raise_for_status()
        return "zip" in r.headers.get("Content-Type", "").lower()


resp = http.get(CKAN_PACKAGE_URL, timeout=60)
resp.raise_for_status()
resources = resp.json()["result"]["resources"]
# Solo recursos de viajes (excluye el diccionario de atributos)
resources = [r for r in resources if r["name"].lower().startswith("viajes")]
print(f"{len(resources)} recursos de viajes en el catálogo")

# COMMAND ----------


def download(res: dict) -> tuple[str, str]:
    name = slugify(res["name"]) + ".zip"  # ej: viajes_abril_2024.zip
    dest = f"{target_dir}/{name}"
    url = res["url"]

    if not is_zip(url):
        return name, "skipped (no es zip)"
    if os.path.exists(dest):
        return name, "skipped (ya existe)"

    tmp = dest + ".part"
    # El servidor corta conexiones largas: se reintenta reanudando con Range desde lo ya descargado
    for attempt in range(1, MAX_ATTEMPTS + 1):
        done = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        headers = {"Range": f"bytes={done}-"} if done else {}
        try:
            with http.get(url, headers=headers, stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                resumed = r.status_code == 206
                with open(tmp, "ab" if resumed else "wb") as f:
                    f.writelines(r.iter_content(chunk_size=8 * 1024 * 1024))
            break
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError):
            if attempt == MAX_ATTEMPTS:
                raise
            time.sleep(5 * attempt)
    os.rename(tmp, dest)
    return name, f"ok ({os.path.getsize(dest) / 1e6:.0f} MB)"


results, failures = [], []
with ThreadPoolExecutor(max_workers=4) as pool:
    futures = {pool.submit(download, r): r["name"] for r in resources}
    for fut in as_completed(futures):
        try:
            result = fut.result()
        except (zipfile.BadZipFile, OSError, ValueError) as e:
            failures.append((futures[fut], repr(e)))
            print(f"{futures[fut]} -> FAILED {e!r}")
            continue
        results.append(result)
        print(*results[-1], sep=" -> ")

print(f"\nDescargados/revisados: {len(results)}, fallidos: {len(failures)}")
if failures:
    raise RuntimeError(f"Fallaron {len(failures)} descargas: {failures}")
