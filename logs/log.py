import pandas as pd
import numpy as np

# Import your module
from app.modules.database.connection import engine
from sqlalchemy import text
from app.modules.database.companies import add_company, add_bussines_plan
from app.modules.database.portfolio_manager import portfolio_buyer, add_portfolio_purchase
from app.modules.database.credit_manager import credits_balance
from app.modules.database.collection import resource_collection, charging, collection_w_early_cancel, delete_collection_by_id, TypeDataCollection
from sqlalchemy import Enum

sql_script_path = 'docs/AppStructure.sql'

try:
    with engine.connect() as connection:
        # Read the SQL script
        with open(sql_script_path, 'r') as file:
            sql_script = file.read()

        # Split script into individual statements
        for statement in sql_script.split(';'):  # Handle multiple statements
            if statement.strip():  # Skip empty statements
                # Use the `text` function to execute raw SQL
                connection.execute(text(statement))

    print("SQL script executed successfully.")

except Exception as e:
    print(f"Error: {e}")

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
                  pd.Timestamp(input('Fecha:')),
                  float(input('Comisión por fondeo:')),
                  float(input('Comisión por colocación (plazo 6):')),
                  float(input('Comisión por colocación (plazo 9):')),
                  float(input('Comisión por colocación (plazo 12):')),
                  float(input('Comisión por colocación (plazo 15):')),
                  float(input('Comisión por colocación (plazo 18):')),
                  float(input('Comisión por colocación (plazo 24):')),
                  float(input('Comisión por colocación (plazo 36):')),
                  float(input('Comisión por colocación (plazo 48):')),
                  float(input('Comisión por cobranza:')),
                  float(input('Comisión sobre cada crédito:'))
                )

add_bussines_plan(3,
                  input('Detalle del plan comercial.'),
                  pd.Timestamp(input('Fecha:')),
                  float(input('Comisión por fondeo:')),
                  float(input('Comisión por colocación (plazo 6):')),
                  float(input('Comisión por colocación (plazo 9):')),
                  float(input('Comisión por colocación (plazo 12):')),
                  float(input('Comisión por colocación (plazo 15):')),
                  float(input('Comisión por colocación (plazo 18):')),
                  float(input('Comisión por colocación (plazo 24):')),
                  float(input('Comisión por colocación (plazo 36):')),
                  float(input('Comisión por colocación (plazo 48):')),
                  float(input('Comisión por cobranza:')),
                  float(input('Comisión sobre cada crédito:'))
                )

setts = pd.read_sql('settings', engine, index_col='ID')

setts.loc[0, ['Detail', 'Type', 'Value']] = {'Detail': 'Día de Vto. Predeterminado', 'Type': 'I', 'Value': "28"}
setts.loc[1, ['Detail', 'Type', 'Value']] = {'Detail': 'Periodos de Gracia', 'Type': 'I', 'Value': "2"}
setts.loc[2, ['Detail', 'Type', 'Value']] = {'Detail': 'Tolerancia Cobranzas', 'Type': 'F', 'Value': '0.05'}
setts.to_sql('settings', engine, if_exists='append', index = False)

path = 'inputs/Onoyen - Cartera Nro. 1.xlsx'
portfolio_buyer(path, 2, 2, 0.885, True, False, date=pd.Period("2022-12-21"), save=True, model=True)

path = 'inputs/Onoyen - Cartera Nro. 2.xlsx'
portfolio_buyer(path, 2, 2, 0.87, True, False, date=pd.Period("2023-02-22"), save=True, model=False)

path = 'inputs/Onoyen - Cartera Nro. 3.xlsx'
portfolio_buyer(path, 2, 2, 0.67, True, False, date=pd.Period("2023/03/21"), save=True, model=False)

path = 'inputs/Onoyen - Cartera Nro. 4.xlsx'
portfolio_buyer(path, 2, 2, 0.62, True, False, date=pd.Period("2023/04/18"), save=True, model=False)

path = 'inputs/Onoyen - Cartera Nro. 5.xlsx'
portfolio_buyer(path, 2, 2, 0.59, True, False, date=pd.Period("2023/05/18"), save=True, model=False)

path = 'inputs/Onoyen - Cartera Nro. 6.xlsx'
portfolio_buyer(path, 2, 2, 0.48, True, False, date=pd.Period("2023/06/15"), save=True, model=False)

balance = credits_balance()
fecha = pd.Timestamp("2024/12/01")
for d in balance['D_Due'].unique():
    if d < fecha:
        resource_collection(2, balance.loc[balance['D_Due'] == d, 'Total'].sum(), date=d, save=True)

balance = credits_balance()
fecha = pd.Timestamp("2024/12/30")
resource_collection(2, balance.loc[balance['D_Due'] <= fecha, 'Total'].sum(), date=fecha, save=True)

print('Cartera de créditos Onoyen migrada, correctamente.')

class MaritalStatus(Enum):
    SINGLE = 'SINGLE'
    COHABITATION = 'COHABITATION'
    MARRIED = 'MARRIED'
    WIDOW = 'WIDOW'
    DIVORCE = 'DIVORCE'


df = pd.read_excel('inputs/Reporte - Inv. Créditos - 24-12-31.xlsx')
df['ID'] = df['Clave Externa'].str.lstrip('CH_')
df.set_index('ID', inplace=True)
df.dropna(axis=1, how='all', inplace=True)
df = df[df['Fondeador'] == 'NEOCREDIT']
unique_columns = [col for col in df.columns if len(df[col].unique())<=1]
df.drop(columns=unique_columns, inplace=True)
df.drop(columns=['Id. Op.', 'Clave Externa', 'Sit. Op.', 'Nro Prestamo', 'Id. Cliente', 'Id. Org.', 'Org.', 'Clave Org.', 'Hab. 1er Vto', 'F. Ult. Vto', 'Monto', 'Val. Cuota del Periodo', 'Val. Cuota S/IVA del Periodo', 'Cant. Cuotas Cob.', 'Imp. Total Cob.', 'Cap. Cuotas Cob.', 'Int. Cuotas Cob.', 'IVA Cuotas Cob.', 'Cant. Cuotas Imp. (V/aV)', 'Imp. Cuotas Imp. (V/aV)', 'Cap. Cuotas Imp. (V/aV)', 'Int. Cuotas Imp. (V/aV)', 'Iva Cuotas Imp. (V/aV)', 'Cant. Cuotas Imp. Vencidas', 'Imp. Cuotas Imp. Vencidas', 'Cap. Cuotas Imp. Vencidas', 'Int. Cuotas Imp. Vencidas', 'IVA Cuotas Imp. Vencidas', 'Cant. Cuotas Imp. a Vencer', 'Imp. Cuotas Imp. a Vencer', 'Cap. Cuotas Imp. a Vencer', 'Int. Cuotas Imp. a Vencer', 'Iva. Cuotas Imp. a Vencer', 'F. Vto ult. Cuota Cob. Completa', 'F. ult. Vto Operado', 'Nro 1er Cuota Imp.', 'F. Vto 1er Cuota Imp.', 'F. Cob. 4 Meses Atras', 'F. Cob. 3 Meses Atras', 'F. Cob. 2 Meses Atras', 'F. Cob. 1 Meses Atras', 'Cob. 4 Meses Atras', 'Cob. 3 Meses Atras', 'Cob. 2 Meses Atras', 'Cob. 1 Meses Atras', 'F. ult. Cob.', 'Imp. ult. Cob.', 'Dias de Mora', 'Dias dde F. ult. Cob.', 'Tel. Lab.'], inplace=True)

df.rename(columns={
    'F. Liq.': 'Date_Settlement',
    'F. 1er Vto': 'D_F_Due',
    'TEM': 'TEM_W_IVA',
    'Cap. Solicitado': 'Cap_Requested',
    'Cap. Otrogado': 'Cap_Grant',
    'Cant. Cuotas': 'N_Inst',
    'Val. Cuota': 'V_Inst',
    'Nro Doc.': 'DNI',
    'Apellido': 'Last_Name',
    'Nombre': 'Name',
    'Edad al Alta': 'Age_at_Discharge',
    'Provincia': 'Province',
    'Domicilio': 'Street',
    'Localidad': 'Locality',
    'Tel. Predeterminado': 'Telephone',
    'Sueldo Liquido': 'Salary',
    'CBU Cliente': 'CBU',
    'Línea': 'Collection_Entity',
    'Empleador': 'Employer',
    'CUIT': 'CUIT_Employer'
}, inplace=True)

df['First_Inst_Purch'] = 1

for c in ['Date_Birth', 'Gender', 'Marital_Status', 'Nro', 'CP', 'Email', 'Country', 'Feature', 'Seniority', 'Dependence', 'Empl_Prov', 'Empl_Loc', 'Empl_Adress']:
    df[c] = pd.NA


df = df[['CUIL', 'DNI', 'Last_Name', 'Name', 'Date_Birth', 'Gender', 'Marital_Status', 'Age_at_Discharge', 'Country', 'Province', 'Locality', 'Street', 'Nro', 'CP', 'Feature', 'Email', 'Telephone', 'Seniority', 'Salary', 'CBU', 'Collection_Entity', 'Employer', 'Dependence', 'CUIT_Employer', 'Empl_Prov', 'Empl_Loc', 'Empl_Adress', 'Date_Settlement', 'Cap_Requested', 'Cap_Grant', 'N_Inst', 'First_Inst_Purch', 'TEM_W_IVA', 'V_Inst', 'D_F_Due']]

clients = pd.read_excel('inputs/Clientes - 24-12-31.xlsx', index_col='C.U.I.L.')
clients.rename(columns={
    'FECHA NACIMIENTO': 'Date_Birth',
    'SEXO': 'Gender',
    'ESTADO CIVIL': 'Marital_Status',
    'NACIONALIDAD': 'Country',
    'E-MAIL': 'Email',
    'CALLE': 'Street',
    'CALLE NÚMERO': 'Nro',
    'LOCALIDAD': 'Locality',
    'CÓDIGO POSTAL': 'CP',
    'PROVINCIA': 'Province'
}, inplace=True)

clients = clients[['Date_Birth', 'Gender', 'Marital_Status', 'Country', 'Nro', 'CP', 'Email']]

marital_status_map = {
    'SOLTERO/A':    MaritalStatus.SINGLE,
    'CONCUBINATO':  MaritalStatus.COHABITATION,
    'CASADO/A':     MaritalStatus.MARRIED,
    'VIUDO/A':      MaritalStatus.WIDOW,
    'DIVORCIADO/A': MaritalStatus.DIVORCE
}

# Now replace the values in the 'Marital_Status' column
clients['Marital_Status'] = clients['Marital_Status'].replace(marital_status_map)


df = df.merge(clients, left_on='CUIL', right_index=True, how='inner')

# Rename columns ending with '_y' to remove the '_y' suffix
df.rename(columns=lambda x: x.rstrip('_y') if x.endswith('_y') else x, inplace=True)
df.drop(columns=[col for col in df.columns if '_x' in col], inplace=True)
df.rename(columns={'Countr': 'Country'}, inplace=True)

df = df[['CUIL', 'DNI', 'Last_Name', 'Name', 'Date_Birth', 'Gender', 'Marital_Status', 'Age_at_Discharge', 'Country', 'Province', 'Locality', 'Street', 'Nro', 'CP', 'Feature', 'Email', 'Telephone', 'Seniority', 'Salary', 'CBU', 'Collection_Entity', 'Employer', 'Dependence', 'CUIT_Employer', 'Empl_Prov', 'Empl_Loc', 'Empl_Adress', 'Date_Settlement', 'Cap_Requested', 'Cap_Grant', 'N_Inst', 'First_Inst_Purch', 'TEM_W_IVA', 'V_Inst', 'D_F_Due']]

df['Empl_Prov'] = df['Empl_Prov'].fillna(df['Province'], axis=0)
df['Empl_Loc'] = df['Empl_Prov'].fillna(df['Locality'], axis=0)
df['TEM_W_IVA'] /= 100

bp = pd.read_sql('business_plan', engine, index_col='ID')
for i in range(3,6):
    path = f'outputs/Cartera - AMUF - 24-12-30 - {i}.xlsx'
    if i == 3:
        df.loc[(df['Date_Settlement'] >= bp.loc[i, 'Date']) & (df['Date_Settlement'] < bp.loc[i+1, 'Date'])].to_excel(path, index=True)
        if df.empty:
            break
    elif i == 4:
        df.loc[(df['Date_Settlement'] >= bp.loc[i, 'Date']) & (df['Date_Settlement'] < bp.loc[i+1, 'Date'])].to_excel(path, index=True)
    else:
        df.loc[df['Date_Settlement'] > bp.loc[i, 'Date']].to_excel(path, index=True)
    portfolio_buyer(path, 3, i, 0.0, False, True, False, save=True)

coll = pd.read_excel('inputs/Reporte - Cobranzas - 24-12-31.xlsx')
coll = coll[coll['Fondeador'] != 'ONOYEN']
coll.drop(columns=['GS', 'PU'], inplace=True)

inv = pd.read_excel('inputs/Reporte - Inv. Créditos - 24-12-31.xlsx', index_col='Id. Op.')
inv = inv.loc[inv['Fondeador'] != 'ONOYEN']

credits = pd.read_sql('credits', engine, index_col='ID')
coll['ID_Externo'] = coll['Crédito'].apply(lambda x: inv.loc[x, 'Clave Externa'].strip("CH_") if x in inv.index else None)

coll['ID'] = coll['ID_Externo'].apply(lambda x: credits.loc[credits['ID_External'] == x].index.values[0] if x in credits['ID_External'].values else None)

# Replace NaN values with a placeholder, e.g., -1
coll['ID_Externo'] = coll['ID_Externo'].fillna(-1)
coll['ID'] = coll['ID'].fillna(-1)

# Convert to integers
coll['ID'] = coll['ID'].astype(int)
coll['ID_Externo'] = coll['ID_Externo'].astype(int)

coll[['ID', 'ID_Externo']] = coll[['ID', 'ID_Externo']].replace(-1, None)
coll['Tipo Cobranza'] = coll['Tipo Cobranza'].replace('ANTICIPO', 'COBRANZA')
coll['Tipo Cobranza'] = coll.apply(lambda row: row['Tipo Cobranza'] if row['Línea'] != 'PENALTY' else 'PENALTY', axis=1)

missing_ids = coll.loc[(coll['ID'].isna()) & (coll['Tipo Cobranza'] == 'PENALTY')]
coll.loc[coll['ID'].isna() & (coll['Tipo Cobranza'] == 'PENALTY'), 'ID'] = [credits.index.max() + i + 1 for i in range(len(missing_ids))]


coll_org_id = coll.loc[~coll['ID'].isna()].copy()
coll = coll.groupby(['Emisión', 'CUIL', 'ID', 'Tipo Cobranza'])[['CA', 'IN', 'IV', 'TOTAL']].sum().reset_index().sort_values(by=['Emisión', 'ID'])

penalties = coll.loc[coll['Tipo Cobranza'] == 'PENALTY'].sort_values(by=['Emisión', 'ID']).set_index('ID')
new_penalties = credits.iloc[0:0].copy()
new_installments = pd.read_sql('installments', engine, index_col='ID').iloc[0:0]

add_portfolio_purchase(pd.Timestamp.now(), 3, 0.0, False, False, True, True)
pp = pd.read_sql('portfolio_purchases', engine, index_col='ID')
last_pp = pp.index.max()

customers = pd.read_sql('customers', engine, index_col='ID')
for i in penalties.index:
    new_penalties.loc[i] = {
        'ID_External': None,
        'ID_Client': customers.loc[customers['CUIL'] == penalties.loc[i, 'CUIL']].index.values[0],
        'Date_Settlement': penalties.loc[i, 'Emisión'],
        'ID_BP': 1,
        'Cap_Requested': 0.0,
        'Cap_Grant': 0.0,
        'N_Inst': 1,
        'First_Inst_Purch': 1,
        'TEM_W_IVA': 0.0,
        'V_Inst': -penalties.loc[i, 'TOTAL'],
        'First_Inst_Sold': None,
        'D_F_Due': penalties.loc[i, 'Emisión'],
        'ID_Purch': last_pp,
        'ID_Sale': None
    }
    new_installments.loc[i] = {
        'ID_Op': i,
        'Nro_Inst': 1,
        'D_Due': penalties.loc[i, 'Emisión'],
        'Capital': 0.0,
        'Interest': -penalties.loc[i, 'IN'],
        'IVA': -penalties.loc[i, 'IV'],
        'Total': -penalties.loc[i, 'TOTAL'],
       'ID_Owner': 1
    }

new_penalties.to_sql('credits', engine, index=False, if_exists='append')
new_installments.to_sql('installments', engine, index=False, if_exists='append')

cobranzas = pd.DataFrame(coll.loc[coll['Tipo Cobranza'] != 'COBRANZA X CANCEL ANT'].groupby(['Emisión', 'ID'])['TOTAL'].sum())

for date, id in cobranzas.index:
    if 'COBRANZA X CANCEL ANT' not in coll.loc[coll['Emisión'] == date, 'Tipo Cobranza'].values:
        charging(TypeDataCollection.ID_Op, id, -cobranzas.loc[(date, id), 'TOTAL'], 3, date, True)
    else:
        collection_w_early_cancel(TypeDataCollection.ID_Op, id, -cobranzas.loc[(date, id), 'TOTAL'], 3, date, True)

collections = pd.read_sql('collection', engine, index_col='ID')
installments = pd.read_sql('installments', engine, index_col='ID')
for i in collections.loc[collections['ID_Inst'].isin(installments.loc[installments['ID_Op'] == 301].index.values)].index:
    delete_collection_by_id(i)

charging(TypeDataCollection.ID_Op, 301, 57859.57, 3, pd.Timestamp("2024-09-11"), True)
collection_w_early_cancel(TypeDataCollection.ID_Op, 301, 458.91, 3, pd.Timestamp("2024-09-11"), True)
charging(TypeDataCollection.ID_Op, 301, 9949.52, 3, pd.Timestamp("2024-10-09"), True)
charging(TypeDataCollection.ID_Op, 301, 47451.14, 3, pd.Timestamp("2024-10-31"), True)
charging(TypeDataCollection.ID_Op, 301, 764.85, 3, pd.Timestamp("2024-10-31"), True)

new_collections = collections.iloc[0:0].copy()
new_collections.loc[0] = {
    'ID_Inst': 4385,
    'D_Emission': pd.Timestamp("2024-10-31"),
    'Type_Collection': 'ANTICIPADA',
    'Capital': 50765.87 - (38351.16 + 8053.74),
    'Interest': 38351.16,
    'IVA': 8053.74,
    'Total': 50765.87
}

new_collections.to_sql('collection', engine, index=False, if_exists='append')
print('Cartera de créditos AMUF migrada.')

balance = credits_balance()
inv = pd.read_excel('inputs/Reporte - Inv. Créditos - 24-12-31.xlsx', index_col='Id. Op.')
inv = inv.loc[inv['Fondeador'] != 'ONOYEN']
credits = pd.read_sql('credits', engine, index_col='ID')
credits.loc[~credits['ID_External'].isna(), 'ID_External'] = credits.loc[~credits['ID_External'].isna(), 'ID_External'].astype(np.int64)
inv['Clave Externa'] = inv['Clave Externa'].str.split("CH_")
inv['Clave Externa'] = inv['Clave Externa'].apply(lambda x: x[1])
inv['Clave Externa'] = inv['Clave Externa'].astype(np.int64)
inv['ID'] = inv['Clave Externa'].apply(lambda x: credits.loc[credits['ID_External'] == x].index if not credits.loc[credits['ID_External'] == x].empty else None)
coll_org_id['ID'] = coll_org_id['ID'].astype(np.int64)

for id in coll_org_id['ID'].unique():
    imp = inv.loc[inv['ID'] == id, 'Cap. Cuotas Imp. (V/aV)'].sum()
    bal = balance.loc[balance['ID_Op'] == id, 'Capital'].sum()
    if abs(imp - bal) > 1:
        print(f'Capital - {id}: $ {imp:,.2f} - $ {bal:,.2f}')

    imp = inv.loc[inv['ID'] == id, 'Int. Cuotas Imp. (V/aV)'].sum()
    bal = balance.loc[balance['ID_Op'] == id, 'Interest'].sum()
    if abs(imp - bal) > 1:
        print(f'Interés - {id}: $ {imp:,.2f} - $ {bal:,.2f}')

    imp = inv.loc[inv['ID'] == id, 'Iva Cuotas Imp. (V/aV)'].sum()
    bal = balance.loc[balance['ID_Op'] == id, 'IVA'].sum()
    if abs(imp - bal) > 1:
        print(f'IVA - {id}: $ {imp:,.2f} - $ {bal:,.2f}')

    imp = inv.loc[inv['ID'] == id, 'Imp. Cuotas Imp. (V/aV)'].sum()
    bal = balance.loc[balance['ID_Op'] == id, 'Total'].sum()
    if abs(imp - bal) > 1:
        print(f'Total - {id}: $ {imp:,.2f} - $ {bal:,.2f}')

imp = inv['Imp. Cuotas Imp. (V/aV)'].sum()
if abs(imp - balance['Total'].sum()) < 10:
    print('Cartera de AMUF migrada correctamente.')

add_bussines_plan(3,
                  input('Detalle del plan comercial.'),
                  pd.Timestamp(input('Fecha:')),
                  float(input('Comisión por fondeo:')),
                  float(input('Comisión por colocación (plazo 6):')),
                  float(input('Comisión por colocación (plazo 9):')),
                  float(input('Comisión por colocación (plazo 12):')),
                  float(input('Comisión por colocación (plazo 15):')),
                  float(input('Comisión por colocación (plazo 18):')),
                  float(input('Comisión por colocación (plazo 24):')),
                  float(input('Comisión por colocación (plazo 36):')),
                  float(input('Comisión por colocación (plazo 48):')),
                  float(input('Comisión por cobranza:')),
                  float(input('Comisión sobre cada crédito:'))
                )