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
fecha = balance['D_Due'].max()

# Collect all amounts due before the specified date
early_balance = balance[balance["D_Due"] <= fecha].groupby("D_Due")["Total"].sum()
early_balance = early_balance[early_balance != 0]

# Process each due date
for due_date, total_amount in early_balance.items():
    resource_collection(2, total_amount, date=due_date, save=True)
    print(f"✅ Processed resource collection for due date {due_date}, Amount: $ {total_amount:,.2f}")

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
df = pd.read_excel(f"inputs/Reporte - Inv. Créditos.xlsx")
print(f'Last issue of a credit: {df['F. Liq.'].max()}')

# ✅ Clean and structure data
df["ID"] = df["Clave Externa"].str.lstrip("CH_")
df.set_index("ID", inplace=True)
df.dropna(axis=1, how="all", inplace=True)
df = df.query("Fondeador == 'NEOCREDIT'")
inv = df.copy()

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
clients = pd.read_excel(f'inputs/Clientes.xlsx', index_col='C.U.I.L.')

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
    path = f'outputs/Cartera - AMUF - {pd.Period.now('D')} - {i}.xlsx'
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

# Load collection Excel file
print("🔄 Loading collection report from Excel...")
coll_path = 'inputs/Reporte - Cobranzas.xlsx'
df = pd.read_excel(coll_path)

# Rename columns for consistency
print("📑 Renaming columns...")
df.rename(columns={'Emisión': 'D_Emission', 'Tipo Cobranza': 'Type_Collection', 'Crédito': 'ID_Op',
                   'Cta.': 'Nro_Inst', 'CA': 'Capital', 'IN': 'Interest', 'IV': 'IVA', 'TOTAL': 'Total'}, inplace=True)

# Drop unused columns
print("🧹 Dropping unused columns...")
df.drop(columns=['GS', 'PU'], inplace=True)

# Filter out unwanted rows
print("🔍 Filtering rows...")
df = df.loc[df['Línea'] != 'DECRETO 14/12']
df = df.loc[(df['Type_Collection'] != 'ANTICIPO')]

# Convert CUIL to integer
print("🔢 Converting CUIL to integer...")
df['CUIL'] = df['CUIL'].astype(int)

# Create monthly period column
print("🗓️ Creating period column...")
df['Per. Ems.'] = df['D_Emission'].dt.to_period('M')

# Normalize 'Línea' values
print("🧾 Normalizing 'Línea' column...")
amuf = list(df['Línea'].unique())
amuf.remove('PENALTY')
df['Línea'] = df['Línea'].replace(amuf, 'Asociación Mutual Union Federal')

# Map external credit ID
print("🔗 Mapping external credit IDs...")
df['ID_Ext'] = df['ID_Op'].apply(lambda x: inv.loc[inv['Id. Op.'] == x].index[0] if x in inv['Id. Op.'].values else None)

# Process penalties
print("⚠️ Processing penalties...")
penalty = -df.loc[df['Línea'] == 'PENALTY'].groupby(['D_Emission', 'CUIL'])[['Total']].sum().copy()

# Create new penalty records
print("📄 Creating new penalty credit entries...")
credits = pd.read_sql('credits', engine, index_col='ID')
new_penalties = credits.loc[credits['TEM_W_IVA'] == 0.0].copy()
installments = pd.read_sql('installments', engine, index_col='ID')
new_installments = installments.iloc[0:0].copy()
penalty = penalty.reset_index()
customers = pd.read_sql('customers', engine, index_col='ID')
penalty['ID_Customers'] = penalty['CUIL'].apply(lambda x: customers.loc[customers['CUIL'] == x].index[0] if x in customers['CUIL'].values else 'Error')
penalty.set_index(['D_Emission', 'ID_Customers'], inplace=True, drop=True)

# Generate new penalties DataFrame
print("➕ Populating new penalties...")
for i, (d, id) in enumerate(penalty.index):
    new_penalties.loc[i] = {
        'ID_External': None,
        'ID_Client': id,
        'Date_Settlement': d,
        'ID_BP': 1,
        'Cap_Requested': 0.0,
        'Cap_Grant': 0.0,
        'N_Inst': 1,
        'First_Inst_Purch': None,
        'TEM_W_IVA': 0.0,
        'V_Inst': penalty.loc[(d,id), 'Total'],
        'First_Inst_Sold': None,
        'D_F_Due': d,
        'ID_Purch': None,
        'ID_Sale': None}

# Save penalties to database
print("💾 Saving penalties to 'credits' table...")
new_penalties.to_sql('credits', engine, if_exists='append', index=False)

# Reload updated credits
print("♻️ Reloading updated credits from DB...")
credits = pd.read_sql('credits', engine, index_col='ID')
new_installments = installments.iloc[0:0].copy()
new_penalties = credits.loc[credits['TEM_W_IVA'] == 0.0]

# Generate new installment records for penalties
print("📊 Creating new installments...")
for id in new_penalties.index:
    new_installments.loc[id] = {
        'ID_Op': id,
        'Nro_Inst': 1,
        'D_Due': new_penalties.loc[id, 'Date_Settlement'],
        'Capital': 0.0,
        'Interest': new_penalties.loc[id, 'V_Inst'] / 1.21,
        'IVA': new_penalties.loc[id, 'V_Inst'] / 1.21 * 0.21,
        'Total': new_penalties.loc[id, 'V_Inst'],
        'ID_Owner': 1
       }

# Save installments
print("💾 Saving new installments...")
new_installments.to_sql('installments', engine, if_exists='append', index=False)

# Reload installments
print("♻️ Reloading updated installments...")
installments = pd.read_sql('installments', engine, index_col='ID')
installments['ID_Ext'] = installments['ID_Op'].apply(lambda x: credits.loc[x, 'ID_External'])

# Build penalty collections
print("📥 Preparing penalty collection records...")
inst_penalties = installments.loc[installments['ID_Op'].isin(new_penalties.index.values)].copy().reset_index()
inst_penalties.rename(columns={'ID': 'ID_Inst', 'D_Due': 'D_Emission'}, inplace=True)
inst_penalties['Type_Collection'] = 'PENALTY'
inst_penalties = inst_penalties[['ID_Inst', 'D_Emission', 'Type_Collection', 'Capital', 'Interest', 'IVA', 'Total']]

# Group and sum non-penalty collections
print("📊 Summarizing standard collections...")
coll = -df.loc[df['Línea'] != 'PENALTY'].groupby(['D_Emission', 'Línea', 'Type_Collection', 'ID_Ext', 'Nro_Inst'])[['Capital', 'Interest', 'IVA', 'Total']].sum().copy()
coll = coll.reset_index()

# Normalize Type_Collection values
print("🔁 Normalizing collection type values...")
coll['Type_Collection'] = coll['Type_Collection'].replace({'COBRANZA': 'CAN. ANT.', 'COBRANZA X CANCEL ANT': 'BON. CAN. ANT.'})

# Map to installment IDs
print("🔗 Mapping collections to installment IDs...")
coll['ID_Inst'] = coll.apply(lambda row: installments.loc[(installments['ID_Ext'] == row['ID_Ext']) & (installments['Nro_Inst'] == row['Nro_Inst'])].index[0], axis=1)

# Final formatting
print("📋 Finalizing collection DataFrame...")
coll = coll[['ID_Inst', 'D_Emission', 'Type_Collection', 'Capital', 'Interest', 'IVA', 'Total']]
coll = pd.concat([coll, inst_penalties], ignore_index=True)

# Save all collection data
print("💾 Saving full collection data...")
coll.to_sql('collection', engine, if_exists='append', index=False)
print("✅ Collection process completed.")
