import os
import pandas as pd
import numpy as np
import sqlparse
from sqlalchemy import text, Enum
from sqlalchemy.exc import IntegrityError as alIE
from pymysql.err import IntegrityError as myIE

# Database connection
from app.modules.database.connection import engine

# Import functions for company management
from app.modules.database.companies import add_company, add_bussines_plan

# Import functions for portfolio management
from app.modules.database.portfolio_manager import portfolio_buyer, add_portfolio_purchase

# Import functions for credit management
from app.modules.database.credit_manager import credits_balance

# Import functions for collection management
from app.modules.database.collection import (
    resource_collection,
    charging,
    collection_w_early_cancel,
    delete_collection_by_id,
    TypeDataCollection
)

# Confirm all modules imported successfully
print(f'✅ The system correctly imports all modules.')

# Define the SQL script path
sql_script_path = 'docs/AppStructure.sql'

# Check if the file exists
if not os.path.exists(sql_script_path):
    print(f"❌ Error: SQL script file '{sql_script_path}' not found.")
else:
    try:
        with open(sql_script_path, 'r', encoding='utf-8') as file:
            sql_script = file.read()

        # Parse and split SQL script into individual statements correctly
        statements = sqlparse.split(sql_script)

        with engine.connect() as connection:
            with connection.begin():  # Ensures transactional integrity
                for statement in statements:
                    statement = statement.strip()
                    if statement:  # Avoid executing empty statements
                        connection.execute(text(statement))

        print("✅ SQL script executed successfully.")

    except Exception as e:
        print(f"❌ Error executing SQL script: {e}")


# Lista de provincias
province_list = [
    'Buenos Aires', 'Chubut', 'Ciudad Autónoma de Buenos Aires', 'Catamarca', 'Chaco',
    'Córdoba', 'Corrientes', 'Entre Ríos', 'Formosa', 'Jujuy', 'La Pampa', 'La Rioja',
    'Mendoza', 'Misiones', 'Neuquén', 'Río Negro', 'Salta', 'San Juan', 'San Luis',
    'Santa Cruz', 'Santa Fe', 'Santiago del Estero', 'Tierra del Fuego', 'Tucumán'
]

# Crear DataFrame con índice desde 1
prov = pd.DataFrame.from_dict({i: {'Name': name} for i, name in enumerate(province_list, start=1)}, orient='index')
prov.index.name = 'ID'

# Insertar en la base de datos
try:
    prov.to_sql('provinces', engine, index=True, if_exists='append')
    print(f"✅ {len(prov)} provinces successfully inserted into the database.")
except Exception as e:
    print(f"❌ Error inserting provinces: {e}")

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

# Define new settings as a list of dictionaries
new_settings = [
    {'Detail': 'Día de Vto. Predeterminado', 'Type': 'I', 'Value': 28},
    {'Detail': 'Periodos de Gracia', 'Type': 'I', 'Value': 2},
    {'Detail': 'Tolerancia Cobranzas', 'Type': 'F', 'Value': 0.05}
]

# Convert list to DataFrame
setts_new = pd.DataFrame.from_records(new_settings)

# Insert into SQL (appends or replaces)
try:
    setts_new.to_sql('settings', engine, if_exists='append', index=False)
    print("✅ Settings successfully updated in the database.")
except Exception as e:
    print(f"❌ Error updating settings: {e}")


# List of portfolios with their parameters
portfolios = [
    ("inputs/Onoyen - Cartera Nro. 1.xlsx", 2, 2, 0.885, "2022-12-21", True),
    ("inputs/Onoyen - Cartera Nro. 2.xlsx", 2, 2, 0.87, "2023-02-22", False),
    ("inputs/Onoyen - Cartera Nro. 3.xlsx", 2, 2, 0.67, "2023-03-21", False),
    ("inputs/Onoyen - Cartera Nro. 4.xlsx", 2, 2, 0.62, "2023-04-18", False),
    ("inputs/Onoyen - Cartera Nro. 5.xlsx", 2, 2, 0.59, "2023-05-18", False),
    ("inputs/Onoyen - Cartera Nro. 6.xlsx", 2, 2, 0.48, "2023-06-15", False),
]

# Process each portfolio file
for path, supplier_id, business_plan_id, tna, date, model in portfolios:
    portfolio_buyer(
        path=path,
        id_supplier=supplier_id,
        id_bp=business_plan_id,
        tna=tna,
        resource=True,
        iva=False,
        date=pd.Period(date),
        save=True,
        model=model
    )

# Process early resource collections before the specified date
balance = credits_balance()
fecha = pd.Timestamp("2025-02-24")

# Collect all amounts due before the specified date
early_balance = balance[balance["D_Due"] < fecha].groupby("D_Due")["Total"].sum()
early_balance = early_balance[early_balance != 0]

# Process each due date
for due_date, total_amount in early_balance.items():
    resource_collection(2, total_amount, date=due_date, save=True)
    print(f"✅ Processed resource collection for due date {due_date}, Amount: {total_amount}")

# Confirm successful migration
print("✅ Onoyen credit portfolio successfully migrated.")

print("We are starting to migrate AMUF's portfolio.")

class MaritalStatus(Enum):
    SINGLE = 'SINGLE'
    COHABITATION = 'COHABITATION'
    MARRIED = 'MARRIED'
    WIDOW = 'WIDOW'
    DIVORCE = 'DIVORCE'

# ✅ Load and process the credit report
date = "25-03-01"
df = pd.read_excel(f"inputs/Reporte - Inv. Créditos - {date}.xlsx")

# ✅ Clean and structure data
df["ID"] = df["Clave Externa"].str.lstrip("CH_")
df.set_index("ID", inplace=True)
df.dropna(axis=1, how="all", inplace=True)
df = df.query("Fondeador == 'NEOCREDIT'")

# ✅ Remove columns with only one unique value
df = df.loc[:, df.nunique() > 1]

# ✅ Drop unnecessary columns
columns_to_drop = [
    "Id. Op.", "Clave Externa", "Sit. Op.", "Nro Prestamo", "Id. Cliente", "Id. Org.", "Org.", "Clave Org.",
    "Hab. 1er Vto", "F. Ult. Vto", "Monto", "Val. Cuota del Periodo", "Val. Cuota S/IVA del Periodo",
    "Cant. Cuotas Cob.", "Imp. Total Cob.", "Cap. Cuotas Cob.", "Int. Cuotas Cob.", "IVA Cuotas Cob.",
    "Cant. Cuotas Imp. (V/aV)", "Imp. Cuotas Imp. (V/aV)", "Cap. Cuotas Imp. (V/aV)", "Int. Cuotas Imp. (V/aV)", 
    "Iva Cuotas Imp. (V/aV)", "Cant. Cuotas Imp. Vencidas", "Imp. Cuotas Imp. Vencidas", "Cap. Cuotas Imp. Vencidas", 
    "Int. Cuotas Imp. Vencidas", "IVA Cuotas Imp. Vencidas", "Cant. Cuotas Imp. a Vencer", "Imp. Cuotas Imp. a Vencer", 
    "Cap. Cuotas Imp. a Vencer", "Int. Cuotas Imp. a Vencer", "Iva. Cuotas Imp. a Vencer", "F. Vto ult. Cuota Cob. Completa", 
    "F. ult. Vto Operado", "Nro 1er Cuota Imp.", "F. Vto 1er Cuota Imp.", "F. Cob. 4 Meses Atras", "F. Cob. 3 Meses Atras", 
    "F. Cob. 2 Meses Atras", "F. Cob. 1 Meses Atras", "Cob. 4 Meses Atras", "Cob. 3 Meses Atras", "Cob. 2 Meses Atras", 
    "Cob. 1 Meses Atras", "F. ult. Cob.", "Imp. ult. Cob.", "Dias de Mora", "Dias dde F. ult. Cob.", "Tel. Lab."
]
df.drop(columns=columns_to_drop, inplace=True)

# ✅ Rename columns
df.rename(columns={
    "F. Liq.": "Date_Settlement",
    "F. 1er Vto": "D_F_Due",
    "TEM": "TEM_W_IVA",
    "Cap. Solicitado": "Cap_Requested",
    "Cap. Otrogado": "Cap_Grant",
    "Cant. Cuotas": "N_Inst",
    "Val. Cuota": "V_Inst",
    "Nro Doc.": "DNI",
    "Apellido": "Last_Name",
    "Nombre": "Name",
    "Edad al Alta": "Age_at_Discharge",
    "Provincia": "Province",
    "Domicilio": "Street",
    "Localidad": "Locality",
    "Tel. Predeterminado": "Telephone",
    "Sueldo Liquido": "Salary",
    "CBU Cliente": "CBU",
    "Línea": "Collection_Entity",
    "Empleador": "Employer",
    "CUIT": "CUIT_Employer"
}, inplace=True)

df["First_Inst_Purch"] = 1

# ✅ Initialize missing columns
missing_columns = [
    "Date_Birth", "Gender", "Marital_Status", "Nro", "CP", "Email", "Country", "Feature",
    "Seniority", "Dependence", "Empl_Prov", "Empl_Loc", "Empl_Adress"
]
df[missing_columns] = pd.NA

# ✅ Reorder columns
df = df[
    ["CUIL", "DNI", "Last_Name", "Name", "Date_Birth", "Gender", "Marital_Status", "Age_at_Discharge",
     "Country", "Province", "Locality", "Street", "Nro", "CP", "Feature", "Email", "Telephone",
     "Seniority", "Salary", "CBU", "Collection_Entity", "Employer", "Dependence", "CUIT_Employer",
     "Empl_Prov", "Empl_Loc", "Empl_Adress", "Date_Settlement", "Cap_Requested", "Cap_Grant",
     "N_Inst", "First_Inst_Purch", "TEM_W_IVA", "V_Inst", "D_F_Due"]
]

# ✅ Load and process client data
clients = pd.read_excel(f'inputs/Clientes - {date}.xlsx', index_col='C.U.I.L.')

# ✅ Rename client columns
clients.rename(columns={
    "FECHA NACIMIENTO": "Date_Birth",
    "SEXO": "Gender",
    "ESTADO CIVIL": "Marital_Status",
    "NACIONALIDAD": "Country",
    "E-MAIL": "Email",
    "CALLE": "Street",
    "CALLE NÚMERO": "Nro",
    "LOCALIDAD": "Locality",
    "CÓDIGO POSTAL": "CP",
    "PROVINCIA": "Province"
}, inplace=True)

clients = clients[["Date_Birth", "Gender", "Marital_Status", "Country", "Nro", "CP", "Email"]]

# ✅ Map marital status values
marital_status_map = {
    "SOLTERO/A": MaritalStatus.SINGLE,
    "CONCUBINATO": MaritalStatus.COHABITATION,
    "CASADO/A": MaritalStatus.MARRIED,
    "VIUDO/A": MaritalStatus.WIDOW,
    "DIVORCIADO/A": MaritalStatus.DIVORCE
}
clients["Marital_Status"] = clients["Marital_Status"].replace(marital_status_map)

# ✅ Merge client data with credit data
df = df.merge(clients, left_on='CUIL', right_index=True, how='inner')

# ✅ Clean column names after merge
df.rename(columns=lambda x: x.rstrip("_y") if x.endswith("_y") else x, inplace=True)
df.drop(columns=[col for col in df.columns if "_x" in col], inplace=True)

# ✅ Ensure "Country" is correctly named
df.rename(columns={"Countr": "Country"}, inplace=True)

df = df[['CUIL', 'DNI', 'Last_Name', 'Name', 'Date_Birth', 'Gender', 'Marital_Status', 'Age_at_Discharge', 'Country', 'Province', 'Locality', 'Street', 'Nro', 'CP', 'Feature', 'Email', 'Telephone', 'Seniority', 'Salary', 'CBU', 'Collection_Entity', 'Employer', 'Dependence', 'CUIT_Employer', 'Empl_Prov', 'Empl_Loc', 'Empl_Adress', 'Date_Settlement', 'Cap_Requested', 'Cap_Grant', 'N_Inst', 'First_Inst_Purch', 'TEM_W_IVA', 'V_Inst', 'D_F_Due']]

# ✅ Final adjustments
df["Empl_Prov"] = df["Empl_Prov"].combine_first(df["Province"])
df["Empl_Loc"] = df["Empl_Loc"].combine_first(df["Locality"])
df["TEM_W_IVA"] /= 100

# ✅ Load business plan data
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

print("✅ Credit migration completed. Now starting collections migration.")
'''
# ✅ Load collections report
coll = pd.read_excel(f'inputs/Reporte - Cobranzas - {date}.xlsx')
coll = coll.query("Fondeador != 'ONOYEN'")
coll.drop(columns=["GS", "PU"], inplace=True)

# ✅ Load investment report
inv = pd.read_excel(f'inputs/Reporte - Inv. Créditos - {date}.xlsx', index_col="Id. Op.")
inv = inv.query("Fondeador != 'ONOYEN'")

# ✅ Load existing credits from the database
credits = pd.read_sql("SELECT * FROM credits", engine, index_col="ID")

# ✅ Map external IDs
coll["ID_External"] = coll["Crédito"].map(lambda x: inv.loc[x, "Clave Externa"].strip("CH_") if x in inv.index else None)

# ✅ Map internal IDs
coll["ID"] = coll["ID_External"].map(
    lambda x: credits.loc[credits["ID_External"] == x].index[0] 
    if x in credits["ID_External"].values and not credits.loc[credits["ID_External"] == x].empty 
    else None
)

# ✅ Replace NaN values with placeholder (-1), then convert to integer safely
coll[["ID_External", "ID"]] = coll[["ID_External", "ID"]].fillna(-1).astype(int, errors="ignore")
coll.replace({"ID_External": -1, "ID": -1}, None, inplace=True)

# ✅ Standardize "Tipo Cobranza" values
coll["Tipo Cobranza"] = coll["Tipo Cobranza"].replace("ANTICIPO", "COBRANZA")
coll["Tipo Cobranza"] = coll.apply(lambda row: "PENALTY" if row["Línea"] == "PENALTY" else row["Tipo Cobranza"], axis=1)

# ✅ Ensure correct indexing for missing 'PENALTY' IDs
penalty_index = coll.index[coll["ID"].isna() & (coll["Tipo Cobranza"] == "PENALTY")]

# ✅ Assign unique IDs only to the required rows
coll.loc[penalty_index, "ID"] = range(credits.index.max() + 1, credits.index.max() + 1 + len(penalty_index))

# ✅ Organize and group collection data
coll_org_id = coll.dropna(subset=["ID"]).copy()
coll = coll.groupby(["Emisión", "CUIL", "ID", "Tipo Cobranza"])[["CA", "IN", "IV", "TOTAL"]].sum().reset_index()
coll = coll.sort_values(by=["Emisión", "ID"])

# ✅ Process penalties
penalties = coll.loc[coll['Tipo Cobranza'] == 'PENALTY'].sort_values(by=["Emisión", "ID"]).set_index("ID")
new_penalties = credits.iloc[:0].copy()
new_installments = pd.read_sql("SELECT * FROM installments WHERE 1=0", engine)  # Load an empty DataFrame structure

# ✅ Importing function for handling portfolio purchases
from app.modules.database.portfolio_manager import add_portfolio_purchase

# ✅ Registering a new portfolio purchase
add_portfolio_purchase(pd.Timestamp.now(), 3, 0.0, False, False, True, True)

# ✅ Retrieving the last recorded portfolio purchase ID
pp = pd.read_sql('portfolio_purchases', engine, index_col='ID')
last_pp = pp.index.max()

# ✅ Loading customer data from the database
customers = pd.read_sql('customers', engine, index_col='ID')

# ✅ Processing penalties, creating new credit and installment records
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

# ✅ Saving new penalty credits and installments to the database
new_penalties.to_sql('credits', engine, index=False, if_exists='append')
new_installments.to_sql('installments', engine, index=False, if_exists='append')

# ✅ Generating a DataFrame summarizing collections
cobranzas = pd.DataFrame(
    coll.loc[coll['Tipo Cobranza'] != 'COBRANZA X CANCEL ANT']
    .groupby(['Emisión', 'ID'])['TOTAL']
    .sum()
)

for date, id in cobranzas.index:
    if 'COBRANZA X CANCEL ANT' not in coll.loc[coll['Emisión'] == date, 'Tipo Cobranza'].values:
        try:
            charging(TypeDataCollection.ID_Op, id, -cobranzas.loc[(date, id), 'TOTAL'], 3, date, True)
        except myIE or alIE:
            raise print(f'ID Op.: {id} - {date} - $ {-cobranzas.loc[(date, id), 'TOTAL']:,.2f}')
    else:
        try:
            collection_w_early_cancel(TypeDataCollection.ID_Op, id, -cobranzas.loc[(date, id), 'TOTAL'], 3, date, True)
        except myIE or alIE:
            raise print(f'ID Op.: {id} - {date} - $ {-cobranzas.loc[(date, id), 'TOTAL']:,.2f}')

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
# Leer y transformar datos
inv = pd.read_excel('inputs/Reporte - Inv. Créditos - 24-12-31.xlsx', index_col='Id. Op.')
inv = inv.loc[inv['Fondeador'] != 'ONOYEN']

credits = pd.read_sql('credits', engine, index_col='ID')
credits.loc[~credits['ID_External'].isna(), 'ID_External'] = credits.loc[~credits['ID_External'].isna(), 'ID_External'].astype(np.int64)

# Manejo robusto de Clave Externa
inv['Clave Externa'] = inv['Clave Externa'].str.split("CH_").apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
inv['Clave Externa'] = pd.to_numeric(inv['Clave Externa'], errors='coerce').astype('Int64')

coll_org_id['ID'] = pd.to_numeric(coll_org_id['ID'], errors='coerce').astype('Int64')

# Validar discrepancias
for id in coll_org_id['ID_Externo'].dropna().unique():
    imp = inv.loc[inv['Clave Externa'] == id, 'Cap. Cuotas Imp. (V/aV)'].sum()
    bal = balance.loc[balance['ID_Op'] == credits.loc[credits['ID_External'] == id].index.values[0], 'Capital'].sum()
    if abs(imp - bal) > 1:
        print(f'Discrepancia en Capital para ID {id}: Calculado $ {imp:,.2f} - Reportado $ {bal:,.2f}')

    imp = inv.loc[inv['Clave Externa'] == id, 'Int. Cuotas Imp. (V/aV)'].sum()
    bal = balance.loc[balance['ID_Op'] == credits.loc[credits['ID_External'] == id].index.values[0], 'Interest'].sum()
    if abs(imp - bal) > 1:
        print(f'Discrepancia en Interés para ID {id}: Calculado $ {imp:,.2f} - Reportado $ {bal:,.2f}')

    imp = inv.loc[inv['Clave Externa'] == id, 'Iva Cuotas Imp. (V/aV)'].sum()
    bal = balance.loc[balance['ID_Op'] == credits.loc[credits['ID_External'] == id].index.values[0], 'IVA'].sum()
    if abs(imp - bal) > 1:
        print(f'Discrepancia en IVA para ID {id}: Calculado $ {imp:,.2f} - Reportado $ {bal:,.2f}')

    imp = inv.loc[inv['Clave Externa'] == id, 'Imp. Cuotas Imp. (V/aV)'].sum()
    bal = balance.loc[balance['ID_Op'] == credits.loc[credits['ID_External'] == id].index.values[0], 'Total'].sum()
    if abs(imp - bal) > 1:
        print(f'Discrepancia en Total para ID {id}: Calculado $ {imp:,.2f} - Reportado $ {bal:,.2f}')

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
'''