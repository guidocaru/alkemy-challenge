import pandas as pd
import requests
import io
from decouple import config
import numpy as np
import logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s - %(message)s')

# --GET DATA--
logging.info("Obteniendo los datos")
# resources links
resources = [config("RESOURCE_MUSEO"), config(
    "RESOURCE_CINE"), config("RESOURCE_BIBLIO")]

# data requests
reqs = []
for idx, url in enumerate(resources):
    try:
        reqs.append(requests.get(url, allow_redirects=True))
    except Exception:
        print("Ocurrió un error al obtener los datos")
        raise
    reqs[idx].encoding = "utf-8"

# data to df
dfs = []
for idx, request in enumerate(reqs):
    dfs.append(pd.read_csv(io.StringIO(reqs[idx].text)))


# --FORMAT DATA--
logging.info("Formateando los datos")
# delete unnecessary columns
df_museo = dfs[0].iloc[:, [0, 1, 2, 4, 6, 7, 8, 9, 11, 13, 14, 15, 20]].copy()
df_cine = dfs[1].iloc[:, [0, 1, 2, 4, 5, 7, 8,
                          9, 11, 13, 14, 15, 20, 22, 23, 24]].copy()
df_biblio = dfs[2].iloc[:, [0, 1, 2, 4, 6,
                            8, 9, 10, 12, 14, 15, 16, 21]].copy()

# set new column names
museo_biblio_columns = ["cod_localidad", "id_provincia", "id_departamento", "categoria",
                        "provincia", "localidad", "nombre", "domicilio", "cod_postal", "telefono", "mail", "web", "fuente"]
cine_columns = ["cod_localidad", "id_provincia", "id_departamento", "categoria", "provincia", "localidad",
                "nombre", "domicilio", "cod_postal", "telefono", "mail", "web", "fuente", "pantallas", "butacas", "espacio_incaa"]

df_museo.columns = museo_biblio_columns
df_biblio.columns = museo_biblio_columns
df_cine.columns = cine_columns

# --TABLE CREATION--
logging.info("Guardando los datos localmente")
now = datetime.now()
year = now.strftime('%Y')
month = now.strftime('%m')
day = now.strftime('%d')

outdir = f'files/museos/{year}-{month}'
if not os.path.exists(outdir):
    os.makedirs(outdir)
df_museo.to_csv(
    f'{outdir}/museos-{day}-{month}-{year}.csv', index=False)

outdir = f'files/bibliotecas/{year}-{month}'
if not os.path.exists(outdir):
    os.makedirs(outdir)
df_biblio.to_csv(
    f'{outdir}/bibliotecas-{day}-{month}-{year}.csv', index=False)

outdir = f'files/cines/{year}-{month}'
if not os.path.exists(outdir):
    os.makedirs(outdir)
df_cine.to_csv(
    f'{outdir}/cines-{day}-{month}-{year}.csv', index=False)

# --TABLE CREATION--

# concats dfs vertically
df = pd.concat([df_museo.iloc[:, 0:13], df_cine.iloc[:, 0:13],
               df_biblio.iloc[:, 0:13]], ignore_index=True)

# qty of registers per categoria
df_categoria = df.groupby(["categoria"]).size().to_frame(
    name="qty_per_categoria")

# qty of registers per fuente
df_fuente = df.groupby(["fuente", "categoria"]
                       ).size().to_frame(name="qty_per_fuente")

# qty of registers per provincia and categoria
df_prov_cat = df.groupby(["provincia", "categoria"]
                         ).size().to_frame(name="qty_per_prov_cat")

# get categories table
categories_table = df_categoria.merge(
    df_fuente, how='outer', left_index=True, right_index=True)
categories_table = categories_table.merge(
    df_prov_cat, how='outer', left_index=True, right_index=True)
categories_table.reset_index(inplace=True)

# format cine
df_cine.loc[df_cine["espacio_incaa"] == "0", "espacio_incaa"] = np.NaN
df_cine.loc[df_cine["espacio_incaa"] == "SI", "espacio_incaa"] = "si"

# get cine table
cines_table = df_cine.groupby(['provincia'], as_index=False).sum()
cines_table = cines_table[["provincia", "pantallas", "butacas"]]

espacios_incaa = df_cine.groupby(
    ["provincia"]).size().to_frame(name="espacios_incaa")
espacios_incaa.reset_index(inplace=True)

cines_table = cines_table.merge(espacios_incaa)
