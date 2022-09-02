

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base,relationship

#from limpieza_produccion import*
#from creardata import*
"""DBMS = 'postgresql'
DRIVER = 'psycopg2'
USER = 'postgres'
PASSWORD = 'postgres'
HOST = 'localhost'
PORT = '5432'
DB_NAME = 'energia'

conecciondb(DBMS, USER, PASSWORD, HOST, PORT, DB_NAME)"""

conexion = create_engine('postgresql://postgres:3013A2018d@energia:5432/postgres')
Session=sessionmaker(bind=conexion)
session=Session()
Base = declarative_base()