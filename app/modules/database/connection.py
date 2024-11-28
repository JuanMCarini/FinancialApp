import json
import os

# Change working directory to the project root
os.chdir(r"c:\Users\juanm_8qa8lav\Documents\Proyectos_Personales\FinancialApp")

# Loading settings
with open("docs/settings.json", "r") as file:
    settings = json.load(file)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine

# Connection to MySQL
user = settings['user']
password = settings['password']
host = settings['host']
charset = settings['charset']

# Crea la cadena de conexión
database = 'credit_portfolio'
connection_string = f'mysql+pymysql://{user}:{password}@{host}/{database}?charset={charset}'

# Crear un motor SQLAlchemy
engine = create_engine(connection_string)