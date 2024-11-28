import os
import pandas as pd

# Change working directory to the project root
os.chdir(r"c:\Users\juanm_8qa8lav\Documents\Proyectos_Personales\FinancialApp")

# Import your module
from app.modules.database.connection import engine
from app.modules.database.companies import add_company, add_bussines_plan

prov = pd.DataFrame(columns=["ID", "Name"])
prov.set_index("ID", inplace=True)
for i, j in enumerate(['Buenos Aires', 'Chubut', 'Ciudad Autónoma de Buenos Aires', 'Catamarca', 'Chaco', 'Córdoba', 'Corrientes', 'Entre Ríos', 'Formosa', 'Jujuy', 'La Pampa', 'La Rioja', 'Mendoza', 'Misiones', 'Neuquén', 'Río Negro', 'Salta', 'San Juan', 'San Luis', 'Santa Cruz', 'Santa Fe', 'Santiago del Estero', 'Tierra del Fuego', 'Tucumán']):
    prov.loc[i+1] = j
prov.sort_values(by=['Name'], inplace=True)
prov.to_sql('provinces', engine, index=True, if_exists='append')

add_company('NeoCrediT S.A.', 30717558142)
add_company('Onoyen S.R.L.', 30711188069)
add_company('Asociación Mutual Union Federal', 30708721561, True)
add_bussines_plan(3,
                  input('Detalle del plan comercial.'),
                  float(input('Comisión por fondeo:')),
                  float(input('Comisión por cobranza (plazo 6):')),
                  float(input('Comisión por cobranza (plazo 9):')),
                  float(input('Comisión por cobranza (plazo 12):')),
                  float(input('Comisión por cobranza (plazo 15):')),
                  float(input('Comisión por cobranza (plazo 18):')),
                  float(input('Comisión por cobranza (plazo 24):')),
                  float(input('Comisión por cobranza (plazo 36):')),
                  float(input('Comisión por cobranza (plazo 48):'))
                )