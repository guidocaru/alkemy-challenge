from decouple import config
from data import df, categories_table, cines_table
from sqlalchemy_utils import create_database
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s - %(message)s')

# --DB CONNECTION--

db_uri = f'{config("DBMS")}://{config("USER")}:{config("PASS")}@{config("HOST")}:{config("PORT")}/{config("DB_NAME")}'

logging.info("Creando la base de datos")
try:
    create_database(db_uri)
except Exception:
    print("No se pudo crear la base de datos")
    raise
try:
    engine = create_engine(db_uri, pool_size=50, echo=False)
except:
    print("Ocurrió un error al obtener el engine")
    raise


# --TABLE CREATION--
logging.info("Creando las tablas en la base de datos")

df.to_sql("main", engine, if_exists='replace', index=False)
df.to_sql("categories", engine, if_exists='replace', index=False)
df.to_sql("cines", engine, if_exists='replace', index=False)

logging.info("Tarea finalizada")
