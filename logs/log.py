import os
import pandas as pd

# Change working directory to the project root
os.chdir(r"../")

# Import your module
from app.modules.database.connection import engine
from app.modules.database.companies import add_company, add_bussines_plan
from app.modules.database.customers import add_customer
from app.modules.database.credit_manager import new_credit

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

setts = pd.read_sql('settings', engine, index_col='ID')

setts.loc[0, ['Detail', 'Type', 'Value']] = {'Detail': 'Día de Vto. Predeterminado', 'Type': 'I', 'Value': "28"}
setts.loc[1, ['Detail', 'Type', 'Value']] = {'Detail': 'Periodos de Gracia', 'Type': 'I', 'Value': "2"}
setts.loc[2, ['Detail', 'Type', 'Value']] = {'Detail': 'Tolerancia Cobranzas', 'Type': 'F', 'Value': '0.05'}
setts.to_sql('settings', engine, if_exists='append', index = False)

add_customer(
    20363297588,
    36329758,
    'Carini',
    'Juan Martín',
    'male',
    '1992/04/01',
    32,
    1,
    'Bahía Blanca',
    "D'Orbigny",
    312,
    8000,
    '0291',
    '+54929143811',
    2,
    900000,
    None,
    'Fideisa',
    'Fideisa',
    'Fideisa',
    30709706736,
    1,
    'Bahía Blanca',
    'Alem 250')

nc, inst = new_credit(
    id_customer = 1,
    Date_Settlement = pd.Timestamp("2024/11/28"),
    ID_BP = 4,
    Cap_Requested = 10**6,
    Cap_Grant = 10**6,
    N_Inst = 6,
    TEM_W_IVA = 1.88/365*30,
    V_Inst = None)

nc, inst = new_credit(
    id_customer = 1,
    Date_Settlement = pd.Timestamp("2024/11/28"),
    ID_BP = 4,
    Cap_Requested = 10**6,
    Cap_Grant = 0.5*10**6,
    N_Inst = 12,
    TEM_W_IVA = 1.96/365*30,
    V_Inst = None)

nc, inst = new_credit(
    id_customer = 1,
    Date_Settlement = pd.Timestamp("2024/10/15"),
    ID_BP = 2,
    Cap_Requested = 10**6,
    Cap_Grant = 0.8*10**6,
    N_Inst = 18,
    TEM_W_IVA = 2.15/365*30,
    V_Inst = None,
    First_Inst_Purch = 5)

