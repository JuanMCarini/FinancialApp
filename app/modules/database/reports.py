import pandas as pd

# Import your module
from app.modules.database.connection import engine
from app.modules.database.credit_manager import credits_balance

pp = pd.read_sql('portfolio_purchases', engine, index_col='ID')
customers = pd.read_sql('customers', engine, index_col='ID')
credits = pd.read_sql('credits', engine, index_col='ID')
installments = pd.read_sql('installments', engine, index_col='ID')
collections = pd.read_sql('collection', engine, index_col='ID')
collections['ID_Op'] = collections['ID_Inst'].apply(lambda x: installments.loc[x, 'ID_Op'])
collections['D_Emission'] = collections['D_Emission'].dt.to_period('D')
companies = pd.read_sql('companies', engine, index_col='ID')
bp = pd.read_sql('business_plan', engine, index_col='ID')
companies = pd.read_sql('companies', engine, index_col='ID')


def portfolio_inventory(date: pd.Period = pd.Period.now('D'), save: bool = False, es: bool = False):
    """
    Genera un inventario detallado de la cartera de créditos para una fecha dada.

    Parámetros:
    - date (pd.Period): Fecha objetivo para el inventario de la cartera. Por defecto, es el día actual.
    - save (bool): Si es True, guarda el resultado en un archivo Excel.
    - es (bool): Si es True, renombra las columnas del inglés al español.

    Retorna:
    - pd.DataFrame: Un DataFrame que contiene el inventario de la cartera con información financiera detallada.
    """
    # Merge de datos desde créditos, clientes y empresas
    df = credits.merge(customers, how='inner', left_on='ID_Client', right_on='ID')
    df = df.merge(bp.drop(columns=['Detail', 'Date']), how='inner', left_on='ID_BP', right_on='ID')
    df = df.merge(companies['Social_Reason'], how='inner', left_on='ID_Company', right_on='ID')
    df.index = credits.index

    # Selección y orden de columnas relevantes
    df = df[['ID_External', 'ID_Company', 'Social_Reason',
             'ID_Client', 'CUIL', 'DNI', 'Last_Name', 'Name', 'Gender', 'Date_Birth', 'Marital_Status', 'Age_at_Discharge',
             'Country', 'ID_Province', 'Locality', 'Street', 'Nro', 'CP', 'Feature', 'Telephone', 'Seniority', 'Salary', 
             'CBU', 'Collection_Entity', 'Employer', 'Dependence', 'CUIT_Employer', 'ID_Empl_Prov', 'Empl_Loc', 'Empl_Adress', 
             'Last_Update', 'Date_Settlement', 'ID_BP', 'Cap_Requested', 'Cap_Grant', 'TEM_W_IVA', 'N_Inst', 'D_F_Due', 
             'ID_Purch', 'First_Inst_Purch', 'V_Inst', 'ID_Sale', 'First_Inst_Sold']]

    # Cálculo de días en mora según balance
    balance = credits_balance(pd.Period.to_timestamp(date))
    balance['D_Due'] = balance['D_Due'].dt.to_period('D')
    balance['Days_in_Default'] = balance.apply(lambda row: (date - row['D_Due']).n if ((date - row['D_Due']).n > 0) and (row['Total'] > 0.009) else 0, axis=1)
    df['Days_in_Default'] = balance.groupby('ID_Op')['Days_in_Default'].max().values

    # Determinación de la última fecha de cobro por operación
    df.loc[df.index.isin(collections['ID_Op'].values), 'Last_Collection'] = collections.groupby('ID_Op')['D_Emission'].max()
    df.loc[~df.index.isin(collections['ID_Op'].values), 'Last_Collection'] = None

    # Cálculo de montos cobrados, en mora, vencidos y adeudados
    for concept in ['Capital', 'Interest', 'IVA', 'Total']:
        df.loc[df.index.isin(collections['ID_Op']), f'{concept}_Collected'] = collections.groupby('ID_Op')[concept].sum()
        df.loc[~df.index.isin(collections['ID_Op']), f'{concept}_Collected'] = 0.0
        df[f'{concept}_in_Default'] = df.apply(
            lambda row: 0 if (row['Days_in_Default'] == 0) else balance.loc[(balance['ID_Op'] == row.name) & (balance['D_Due'] <= date), concept].sum(), axis=1)
        df[f'{concept}_to_Due'] = df.apply(
            lambda row: balance.loc[(balance['ID_Op'] == row.name) & (balance['D_Due'] > date), concept].sum(), axis=1)
        df[f'{concept}_Owed'] = balance.groupby('ID_Op')[concept].sum()

    # Cálculo de días desde el último cobro
    df['Days_since_last_Collection'] = df['Last_Collection'].apply(lambda x: 0 if pd.isna(x) else (date - x).n)

    # Selección y orden final de columnas
    df = df[['ID_External', 'ID_Company', 'Social_Reason', 'ID_Client', 'CUIL', 'DNI', 'Last_Name', 'Name', 'Gender',
             'Date_Birth', 'Marital_Status', 'Age_at_Discharge', 'Country', 'ID_Province', 'Locality', 'Street', 'Nro',
             'CP', 'Feature', 'Telephone', 'Seniority', 'Salary', 'CBU', 'Collection_Entity', 'Employer', 'Dependence',
             'CUIT_Employer', 'ID_Empl_Prov', 'Empl_Loc', 'Empl_Adress', 'Last_Update', 'Date_Settlement', 'ID_BP',
             'Cap_Requested', 'Cap_Grant', 'TEM_W_IVA', 'N_Inst', 'D_F_Due', 'ID_Purch', 'First_Inst_Purch', 'V_Inst',
             'ID_Sale', 'First_Inst_Sold', 'Last_Collection', 'Capital_Collected', 'Interest_Collected', 'IVA_Collected',
             'Total_Collected', 'Days_in_Default', 'Capital_in_Default', 'Interest_in_Default', 'IVA_in_Default',
             'Total_in_Default', 'Days_since_last_Collection', 'Capital_to_Due', 'Interest_to_Due', 'IVA_to_Due',
             'Total_to_Due', 'Capital_Owed', 'Interest_Owed', 'IVA_Owed', 'Total_Owed']]

    # Renombrar columnas al español si es True
    if es:
        df = df.rename(columns={
            'ID_External': 'ID_Externo',
            'ID_Company': 'ID_Empresa',
            'Social_Reason': 'Razón_Social',
            'ID_Client': 'ID_Cliente',
            'CUIL': 'CUIL',
            'DNI': 'DNI',
            'Last_Name': 'Apellido',
            'Name': 'Nombre',
            'Gender': 'Género',
            'Date_Birth': 'Fecha_Nacimiento',
            'Marital_Status': 'Estado_Civil',
            'Age_at_Discharge': 'Edad_al_Alta',
            'Country': 'País',
            'ID_Province': 'ID_Provincia',
            'Locality': 'Localidad',
            'Street': 'Calle',
            'Nro': 'Número',
            'CP': 'Código_Postal',
            'Feature': 'Característica',
            'Telephone': 'Teléfono',
            'Seniority': 'Antigüedad',
            'Salary': 'Salario',
            'CBU': 'CBU',
            'Collection_Entity': 'Entidad_Cobradora',
            'Employer': 'Empleador',
            'Dependence': 'Dependencia',
            'CUIT_Employer': 'CUIT_Empleador',
            'ID_Empl_Prov': 'ID_Provincia_Empleador',
            'Empl_Loc': 'Localidad_Empleador',
            'Empl_Adress': 'Dirección_Empleador',
            'Last_Update': 'Última_Actualización',
            'Date_Settlement': 'Fecha_Liquidación',
            'ID_BP': 'ID_Punto_Bancario',
            'Cap_Requested': 'Capital_Solicitado',
            'Cap_Grant': 'Capital_Otorgado',
            'TEM_W_IVA': 'Tasa_TEM_IVA',
            'N_Inst': 'Nro_Cuotas',
            'D_F_Due': 'Fecha_Ultimo_Vencimiento',
            'ID_Purch': 'ID_Compra',
            'First_Inst_Purch': 'Primera_Cuota_Compra',
            'V_Inst': 'Valor_Cuota',
            'ID_Sale': 'ID_Venta',
            'First_Inst_Sold': 'Primera_Cuota_Vendida',
            'Last_Collection': 'Última_Cobranza',
            'Capital_Collected': 'Capital_Cobrado',
            'Interest_Collected': 'Intereses_Cobrados',
            'IVA_Collected': 'IVA_Cobrado',
            'Total_Collected': 'Total_Cobrado',
            'Days_in_Default': 'Días_en_Mora',
            'Capital_in_Default': 'Capital_en_Mora',
            'Interest_in_Default': 'Intereses_en_Mora',
            'IVA_in_Default': 'IVA_en_Mora',
            'Total_in_Default': 'Total_en_Mora',
            'Days_since_last_Collection': 'Días_desde_Última_Cobranza',
            'Capital_to_Due': 'Capital_por_Vencer',
            'Interest_to_Due': 'Intereses_por_Vencer',
            'IVA_to_Due': 'IVA_por_Vencer',
            'Total_to_Due': 'Total_por_Vencer',
            'Capital_Owed': 'Capital_Adeudado',
            'Interest_Owed': 'Intereses_Adeudados',
            'IVA_Owed': 'IVA_Adeudado',
            'Total_Owed': 'Total_Adeudado',
        })

    # Guardar en Excel si save es True
    if save:
        df.reset_index().to_excel(f'outputs/Portfolio Inventory - {date}.xlsx', index=False)

    return df