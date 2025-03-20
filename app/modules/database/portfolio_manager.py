import os, sys
import pandas as pd
import numpy_financial as npf
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import Session
from PyQt6.QtWidgets import QApplication, QFileDialog


# Import your module
from app.modules.database.connection import engine
from app.modules.database.customers import id_province, categorical_gender, add_customer, MaritalStatus
from app.modules.database.credit_manager import new_credit, credits_balance


app = QApplication(sys.argv)


def validate_supplier_and_business_plan(id_supplier: int, id_bp: int):
    """
    Validates if a given supplier ID exists in the 'companies' table and 
    checks if a given business plan ID is associated with the supplier.
    
    Parameters:
        id_supplier (int): The ID of the supplier to validate.
        id_bp (int): The ID of the business plan to validate.
    
    Raises:
        ValueError: If the supplier ID does not exist or if the business plan ID 
                    is not associated with the given supplier.
    """

    # ✅ Step 1: Validate supplier existence using an efficient SQL query
    supplier_count = pd.read_sql(f"SELECT COUNT(*) FROM companies WHERE ID = {id_supplier}", engine).iloc[0, 0]
    if supplier_count == 0:
        raise ValueError(f"❌ Supplier ID {id_supplier} does not exist in the 'companies' table.")

    # ✅ Step 2: Fetch only relevant business plans for this supplier
    supplier_bp = pd.read_sql(f"SELECT ID FROM business_plan WHERE ID_Company = {id_supplier}", engine)

    # ✅ Step 3: Validate if the business plan ID is associated with this supplier
    if id_bp not in supplier_bp["ID"].values:
        raise ValueError(
            f"❌ Business plan ID {id_bp} is not associated with supplier ID {id_supplier}.\n"
            f"✔ Available business plans for this supplier: {supplier_bp['ID'].tolist()}"
        )
    
    print(f"✅ Supplier {id_supplier} and business plan {id_bp} are valid and correctly associated.")


def load_file(path: str) -> pd.DataFrame:
    """
    Loads a file into a pandas DataFrame, supporting CSV and Excel files.

    Parameters:
        path (str): The file path to load.

    Returns:
        pd.DataFrame: The loaded data.

    Raises:
        FileNotFoundError: If the file does not exist at the specified path.
        ValueError: If the file type is unsupported.
    """

    # ✅ Step 1: Ensure the file exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ The file '{path}' does not exist.")

    # ✅ Step 2: Extract the file extension
    _, extension = os.path.splitext(path)
    extension = extension.lower()

    # ✅ Step 3: Load file based on its type
    match extension:
        case ".csv":
            df = pd.read_csv(path, sep=None, engine="python")  # Auto-detect delimiter
        case ".xls" | ".xlsx":
            df = pd.read_excel(path)
        case _:
            raise ValueError(f"❌ Unsupported file format '{extension}'. Please use CSV or Excel.")

    # ✅ Step 4: Ensure 'ID' column exists before setting it as an index
    if "ID" in df.columns:
        df.set_index("ID", inplace=True)
    else:
        print("⚠️ Warning: The 'ID' column was not found. The index remains unchanged.")

    return df


def _process_credit_row(row, first_inst):
    """
    Processes a single credit row from the dataset.

    Parameters:
        row (pd.Series): Row data.
        first_inst (pd.Series): First installment per credit.

    Returns:
        dict: Processed credit information.
    """
    return {
        'CUIL': row['CUIL del Cliente'],
        'DNI': row['DNI'],
        'Last_Name': row['Last_Name'].title(),
        'Name': row['Name'].title(),
        'Date_Birth': row['Fecha y Lugar de Nacimiento'],
        'Gender': 'O',
        'Marital_Status': MaritalStatus.SINGLE,
        'Age_at_Discharge': None,
        'Country': None,
        'Province': row['Provincia'],
        'Locality': row['Locality'],
        'Street': row['Street'],
        'Nro': None,
        'CP': row['Codigo Postal'],
        'Feature': None,
        'Email': row['Mail'],
        'Telephone': row['Telefono'],
        'Seniority': None,
        'Salary': row['Actividad Laboral (Ingreso mensual)'],
        'CBU': None,
        'Collection_Entity': row['Decreto 1412'],
        'Employer': None,
        'Dependence': None,
        'CUIT_Employer': None,
        'Empl_Prov': None,
        'Empl_Loc': None,
        'Empl_Adress': None,
        'Date_Settlement': row['Fecha de Liquidacion '],
        'Cap_Requested': row['Capital'],
        'Cap_Grant': row['Capital'],
        'N_Inst': row['n.º de Cuota'],
        'First_Inst_Purch': first_inst.get(row.name, None),
        'TEM_W_IVA': None,
        'V_Inst': row['Monto de cuota'],
        'D_F_Due': row['Fecha de Vencimiento']
    }
    
def _process_supplier_2(path: str) -> pd.DataFrame:
    """
    Reads and processes data for supplier ID 2.

    Parameters:
        path (str): Path to the Excel file.

    Returns:
        pd.DataFrame: Processed supplier data.
    """
    columns = [
        'CUIL', 'DNI', 'Last_Name', 'Name', 'Date_Birth', 'Gender', 'Marital_Status', 'Age_at_Discharge',
        'Country', 'Province', 'Locality', 'Street', 'Nro', 'CP', 'Feature', 'Email', 'Telephone',
        'Seniority', 'Salary', 'CBU', 'Collection_Entity', 'Employer', 'Dependence', 'CUIT_Employer',
        'Empl_Prov', 'Empl_Loc', 'Empl_Adress', 'Date_Settlement', 'Cap_Requested', 'Cap_Grant',
        'N_Inst', 'First_Inst_Purch', 'TEM_W_IVA', 'V_Inst', 'D_F_Due'
    ]
    
    df = pd.DataFrame(columns=columns)
    df.index.name = 'ID'

    try:
        first_inst = pd.read_excel(path, sheet_name='Detalle').groupby('id_credito')['nro_cuota'].min()
        credits = pd.read_excel(path, sheet_name='Creditos', index_col='Numero credito original')

        # Standardize CUIL format
        if credits['CUIL del Cliente'].dtype != 'int64':
            credits['CUIL del Cliente'] = credits['CUIL del Cliente'].str.replace('-', '').astype(int)

        # Extract DNI
        credits['DNI'] = credits['CUIL del Cliente'] % 10**9 // 10

        # Split full name and address
        credits[['Last_Name', 'Name']] = credits['Apellido y nombre del cliente'].str.split(", ", expand=True)
        credits[['Street', 'Locality']] = credits['Domicilio'].str.split(' - ', expand=True)
            
        # Process each record
        df = credits.apply(lambda row: _process_credit_row(row, first_inst), axis=1, result_type='expand')

    except Exception as e:
        print(f"❌ Error reading supplier data: {e}")
    
    return df

def read_data(path: str, model: bool = False, id_supplier=None, iqua: bool = False, cfl: bool = False):
    """
    Reads and processes portfolio data based on the specified mode.

    Parameters:
        path (str): Path to the data file (CSV or Excel).
        model (bool): Whether to process data as a 'model' file.
        id_supplier (int, optional): Supplier ID for certain processing cases.
        iqua (bool): Flag for a specific type of data processing.
        cfl (bool): Another optional flag for data processing.

    Returns:
        pd.DataFrame: Processed portfolio data.
    """

    # ✅ Step 1: Validate the mode selection (only one flag can be True)
    active_flags = sum([model, iqua, cfl])
    if active_flags > 1:
        raise ValueError("Cannot set more than one flag (model, iqua, and cfl) to True at the same time.")
    elif active_flags == 0 and not id_supplier:
        raise ValueError("id_supplier must be provided if none of the flags (model, iqua, cfl) are set.")

    # ✅ Step 2: Process data based on the selected mode
    if model:
        df = load_file(path)

        # Normalize text columns
        df[['Last_Name', 'Name']] = df[['Last_Name', 'Name']].apply(lambda col: col.str.title())

        # Standardize Marital Status values
        marital_status_map = {
            'SINGLE': MaritalStatus.SINGLE,
            'COHABITATION': MaritalStatus.COHABITATION,
            'MARRIED': MaritalStatus.MARRIED,
            'WIDOW': MaritalStatus.WIDOW,
            'DIVORCE': MaritalStatus.DIVORCE
        }
        df['Marital_Status'] = df['Marital_Status'].replace(marital_status_map)

    elif iqua or cfl:
        pass  # Placeholder for future implementations

    else:  # Default processing based on id_supplier
        if id_supplier == 2:
            df = _process_supplier_2(path)
        else:
            raise ValueError(f"Unsupported supplier ID: {id_supplier}")

    return df


def update_customers(df: pd.DataFrame, date, save: bool = True):
    """
    Updates the 'customers' table in the database by identifying new customers and updating existing ones.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing customer data to be processed.
        date (str or datetime): The date to assign to the 'Last_Update' column.
        save (bool, optional): If True, saves changes to the database; otherwise, modifies data locally.

    Returns:
        pd.DataFrame: Updated customers DataFrame.
    """

    # ✅ Step 1: Load existing customers table
    customers = pd.read_sql('customers', engine, index_col='ID')

    # ✅ Step 2: Process required fields
    df['ID_Province'] = df['Province'].apply(id_province)
    df['ID_Empl_Prov'] = df['Empl_Prov'].apply(id_province)
    df['Last_Update'] = date
    df['Gender'] = df['Gender'].apply(categorical_gender)
    df['Country'] = df['Country'].fillna('Argentina')

    # ✅ Step 3: Keep only columns that exist in the customers table
    valid_columns = [col for col in customers.columns if col in df.columns]
    new_customers = df[valid_columns]

    # ✅ Step 4: Identify new and existing customers
    existing_customers = new_customers[new_customers['CUIL'].isin(customers['CUIL'])]
    new_customers = new_customers[~new_customers['CUIL'].isin(customers['CUIL'])]

    # ✅ Step 5: Save data if `save=True`
    if save:
        # Save new customers to the database
        if not new_customers.empty:
            new_customers.to_sql('customers', engine, index=False, if_exists='append')

        # Update existing customers
        if not existing_customers.empty:
            for _, row in existing_customers.iterrows():
                add_customer(customer=row.to_frame().T)

        # Reload updated customers table
        customers = pd.read_sql('customers', engine, index_col='ID')
        new_customers = customers[customers['CUIL'].isin(df['CUIL'])]

    else:
        # ✅ Step 6: Update indices for local changes
        existing_customers.index = existing_customers['CUIL'].apply(lambda cuil: customers.index[customers['CUIL'] == cuil].tolist()[0])
        new_customers.index = range(customers.index.max() + 1, customers.index.max() + 1 + len(new_customers))
        
        # Merge existing and new customers
        new_customers = pd.concat([existing_customers, new_customers], ignore_index=True)

        # Adjust index if the table is empty
        if customers.empty:
            new_customers.index += 1

    return new_customers


def add_portfolio_purchase(date, id_supplier, tna, buyback, resource, iva, save=True):
    """
    Adds a new record to the 'portfolio_purchases' table in the database.

    Returns:
        int: The new portfolio purchase ID.
    """
    id_purch = pd.read_sql("SELECT MAX(ID) FROM portfolio_purchases", engine).iloc[0, 0]
    id_purch = 1 if pd.isna(id_purch) else int(id_purch) + 1

    new_purchase = pd.DataFrame([{
        'ID': id_purch,
        'Date': date,
        'ID_Company': id_supplier,
        'TNA': tna,
        'Buyback': int(buyback),
        'Resource': int(resource),
        'IVA': int(iva)
    }])
    new_purchase.index = [id_purch]

    if save:
        try:
            new_purchase.to_sql('portfolio_purchases', engine, index=False, if_exists='append')
            print(f"✅ Portfolio purchase ID {id_purch} successfully added for supplier {id_supplier}.")
        except Exception as e:
            print(f"❌ Error adding portfolio purchase: {e}")

    return id_purch, new_purchase  # ✅ Ensure this new purchase is returned


def process_portfolio(df, new_customers, id_bp, id_purch, iva, date, save=True):
    """
    Processes a portfolio by generating new credits, installments, and collections.

    Parameters:
        df (pd.DataFrame): DataFrame containing credit and installment data.
        new_customers (pd.DataFrame): DataFrame containing updated customer information.
        id_bp (int): Business plan ID to be used for credit creation.
        id_purch (int): Purchase ID to be used for credit creation.
        date (str): Date for the collection emission.
        save (bool): If True, saves changes to the database.

    Returns:
        tuple:
            - new_credits (pd.DataFrame): Newly generated credits.
            - installments (pd.DataFrame): Newly generated installments.
            - collections (pd.DataFrame): Collections data based on installment conditions.
    """

    # ✅ Step 1: Calculate TEM_W_IVA where necessary
    filter = df['TEM_W_IVA'].isna() | (df['TEM_W_IVA'] == 0)
    df.loc[filter, 'TEM_W_IVA'] = df.loc[filter].apply(
        lambda row: npf.rate(row['N_Inst'], row['V_Inst'], -row['Cap_Grant'], 0.0, guess=0.1), axis=1
    )

    # ✅ Step 2: Get last credit ID to ensure unique IDs for new credits
    last_credit = pd.read_sql("SELECT MAX(ID) FROM credits", engine).iloc[0, 0]
    last_credit = 0 if pd.isna(last_credit) else int(last_credit)

    # ✅ Step 3: Generate new credits and installments
    new_credits = []
    installments = []

    for _, row in df.iterrows():
        last_credit += 1
        nc, insts = new_credit(
            id_customer=new_customers.loc[new_customers['CUIL'] == row['CUIL']].index[0],
            Date_Settlement=row['Date_Settlement'],
            ID_BP=id_bp,
            Cap_Requested=row['Cap_Requested'],
            Cap_Grant=row['Cap_Grant'],
            N_Inst=row['N_Inst'],
            TEM_W_IVA=row['TEM_W_IVA'],
            V_Inst=row['V_Inst'],
            D_F_Due=row['D_F_Due'],
            ID_Purch=id_purch,
            First_Inst_Purch=row['First_Inst_Purch'],
            ID_Sale=None,
            First_Inst_Sold=0,
            id_external=row.name,
            massive=last_credit
        )
        new_credits.append(nc)
        installments.append(insts)

    # ✅ Step 4: Concatenate all new credits and installments
    new_credits = pd.concat(new_credits, ignore_index=False)
    new_credits['First_Inst_Purch'] = new_credits['First_Inst_Purch'].astype(int)

    installments = pd.concat(installments, ignore_index=False, join='outer')

    # ✅ Step 5: Get last installment ID from the database
    last_inst = pd.read_sql("SELECT MAX(ID) FROM installments", engine).iloc[0, 0]
    last_inst = 0 if pd.isna(last_inst) else int(last_inst)

    # ✅ Step 6: Assign new unique IDs to installments
    installments.reset_index(drop=True, inplace=True)
    installments.index = range(last_inst + 1, last_inst + 1 + len(installments))

    # ✅ Step 7: Handle IVA (set to 0 if not applicable)
    if not iva:
        installments['IVA'] = 0.0
        installments['Total'] = installments['Capital'] + installments['Interest']

    # ✅ Step 8: Ensure correct data types
    installments['ID_Op'] = installments['ID_Op'].astype(int)
    installments['Nro_Inst'] = installments['Nro_Inst'].astype(int)

    # ✅ Step 9: Process collections
    collections = pd.DataFrame(columns=['ID_Inst', 'D_Emission', 'Type_Collection', 'Capital', 'Interest', 'IVA', 'Total'])
    collections.index.name = 'ID'
    
    j = 0 
    for i in installments.index:
        f_inst_purch = new_credits.loc[installments.loc[i, 'ID_Op'], 'First_Inst_Purch']
        if installments.loc[i, 'Nro_Inst'] < f_inst_purch:
            j += 1
            collections.loc[j] = {
                'ID_Inst': i,
                'D_Emission': date,
                'Type_Collection': 'NO COMPRADA',
                'Capital': installments.loc[i, 'Capital'],
                'Interest': installments.loc[i, 'Interest'],
                'IVA': installments.loc[i, 'IVA'],
                'Total': installments.loc[i, 'Total']
            }

    # ✅ Step 10: Save data if `save=True`
    if save:
        try:
            new_credits.to_sql('credits', engine, index=False, if_exists='append')
            installments.to_sql('installments', engine, index=False, if_exists='append')
            collections.to_sql('collection', engine, index=False, if_exists='append')

            print("✅ Portfolio processing completed successfully.")
        except Exception as e:
            print(f"❌ Error saving portfolio data: {e}")

    return new_credits, installments, collections


def portfolio_buyer(
        path: str,
        id_supplier: int,
        id_bp: int,
        tna: float,
        resource: bool,
        iva: bool,
        buyback: bool = False,
        date: pd.Timestamp = pd.Timestamp.now(),
        model: bool = True,
        iqua: bool = False,
        cfl: bool = False,
        save: bool = False):
    """
    Processes the purchase of a credit portfolio, updates customers, 
    and generates related data for analysis or storage.

    Parameters:
        path (str): Path to the input data file (CSV or Excel).
        id_supplier (int): Supplier ID providing the portfolio.
        id_bp (int): Business plan ID associated with the purchase.
        tna (float): Annual nominal interest rate (TNA).
        resource (bool): Indicates whether resources are involved in the purchase.
        iva (bool): Indicates whether VAT applies to the purchase.
        buyback (bool): Optional flag for buyback conditions (default is False).
        date (pd.Timestamp): Transaction date (default: current date).
        model (bool): Determines if the model is used (default is True).
        iqua (bool): Specific flag for data processing (default is False).
        cfl (bool): Additional optional flag for data processing (default is False).
        save (bool): If True, saves data to the database.

    Returns:
        tuple: Processed data, including:
            - df (DataFrame): Processed portfolio data.
            - new_customers (DataFrame): Updated customer data.
            - pp (DataFrame): Portfolio purchase record.
            - new_credits (DataFrame): New credit records.
            - installments (DataFrame): Installment records.
            - collections (DataFrame): Collection data.
    """
    try:
        # ✅ Step 1: Validate supplier and business plan
        validate_supplier_and_business_plan(id_supplier, id_bp)

        # ✅ Step 2: Read input data & process it based on provided flags
        df = read_data(path=path, model=model, id_supplier=id_supplier, iqua=iqua, cfl=cfl)

        # ✅ Step 3: Update customer data with the latest records
        new_customers = update_customers(df=df, date=date, save=save)

        # ✅ Step 4: Register the portfolio purchase & retrieve its ID
        id_purch, new_purch = add_portfolio_purchase(date=date, id_supplier=id_supplier, tna=tna, 
                                    buyback=buyback, resource=resource, iva=iva, save=save)
        
        # ✅ Step 5: Process the portfolio & generate credit-related records
        new_credits, installments, collections = process_portfolio(
            df=df, new_customers=new_customers, id_bp=id_bp, id_purch=id_purch, 
            iva=iva, date=date, save=save
        )

        # ✅ Fetch company name from the database
        company_name_query = f"SELECT Social_Reason FROM companies WHERE ID = {id_supplier}"
        company_name = pd.read_sql(company_name_query, engine).iloc[0, 0]

        print(f"✅ Portfolio purchase successfully processed for company {company_name} on {date}.")
        return df, new_customers, new_purch, new_credits, installments, collections

    except Exception as e:
        print(f"❌ Error processing portfolio purchase: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def translate_seller(full_inst, credits, customers, sheet_names):
    """
    Translates column names and specific values in the provided DataFrames to Spanish.

    Parameters:
    - full_inst (pd.DataFrame): DataFrame containing installment data.
    - credits (pd.DataFrame): DataFrame containing credit data.
    - customers (pd.DataFrame): DataFrame containing customer data.
    - sheet_names (list): List of sheet names to be used or updated.

    Returns:
    None (modifies DataFrames in place).
    """

    # Define sheet names for the output
    sheet_names = ['Clientes', 'Creditos', 'Cuotas']

    # Translate column names in the 'full_inst' DataFrame
    full_inst.rename(columns={
        'Nro_Inst': 'Nro_Cta',
        'D_Due': 'F_Vto',
        'Interest': 'Interés',
        'D_Emission': 'F_Emisión',
        'Amount_Financed': 'Monto_Financiado',
        'Current_Values': 'Valor_Actual',
        'Accumulated_CV': 'VA_Acumulado'
    }, inplace=True)

    # Translate column names in the 'credits' DataFrame
    credits.rename(columns={
        'ID_Client': 'ID_Cliente',
        'Date_Settlement': 'Fecha_Emisión',
        'Cap_Requested': 'Capital_Solicitado',
        'Cap_Grant': 'Capital_Otorgado',
        'N_Inst': 'Plazo',
        'First_Inst_Purch': 'Prim_Cta_Comprada',
        'TEM_W_IVA': 'TEM_C_IVA',
        'V_Inst': 'Valor_Cta',
        'First_Inst_Sold': 'Prim_Cta_Vendida',
        'D_F_Due': 'Fecha_Prim_Vto',
        'ID_Purch': 'ID_Compra',
        'ID_Sale': 'ID_Venta',
        'Resid_Cap': 'Capital_Residual',
        'Resid_Int': 'Interés_Residual',
        'Resid_IVA': 'IVA_Residual',
        'Resid_Total': 'Total_Residual',
        'Current_Value': 'Valor_Actual'
    }, inplace=True)

    # Translate column names in the 'customers' DataFrame
    translations = {
        'Last_Name': 'Apellido',
        'Name': 'Nombre',
        'Gender': 'Género',
        'Date_Birth': 'Fecha de Nacimiento',
        'Marital_Status': 'Estado Civil',
        'Age_at_Discharge': 'Edad al Egreso',
        'Country': 'País',
        'ID_Province': 'ID Provincia',
        'Locality': 'Localidad',
        'Street': 'Calle',
        'Nro': 'Número',
        'CP': 'Código Postal',
        'Feature': 'Característica',
        'Telephone': 'Teléfono',
        'Seniority': 'Antigüedad',
        'Salary': 'Salario',
        'CBU': 'CBU',
        'Collection_Entity': 'Entidad de Cobro',
        'Employer': 'Empleador',
        'Dependence': 'Dependencia',
        'CUIT_Employer': 'CUIT del Empleador',
        'ID_Empl_Prov': 'ID Provincia del Empleador',
        'Empl_Loc': 'Localidad del Empleador',
        'Empl_Adress': 'Dirección del Empleador',
        'Last_Update': 'Última Actualización'
    }
    customers.rename(columns=translations, inplace=True)

    # Translate specific values in the 'Estado Civil' column of the 'customers' DataFrame
    marital_status_translations = {
        'SINGLE': 'Soltero/a',
        'MARRIED': 'Casado/a',
        'WIDOW': 'Viudo/a',
        'DIVORCE': 'Divorciado/a',
        'COHABITATION': 'Concubinato'
    }
    customers['Estado Civil'] = customers['Estado Civil'].map(marital_status_translations)


def sell_to_sql(ps, full_inst, id_company, id_sale):
    """
    Sells a portfolio of credits and updates related tables in a SQL database.

    Parameters:
    - ps (pd.DataFrame): DataFrame containing portfolio sales data.
    - full_inst (pd.DataFrame): DataFrame containing installment data.
    - id_company (int): ID of the company owning the portfolio.
    - id_sale (int): ID of the sale transaction.

    Returns:
    - id_sale (int): The ID of the sale transaction after updating the database.
    """

    # Append the portfolio sales data to the 'portfolio_sales' table in the database
    ps.to_sql('portfolio_sales', engine, index=False, if_exists='append')

    # Retrieve the maximum ID from the 'portfolio_sales' table to update the sale ID
    id_sale = pd.read_sql('portfolio_sales', engine, index_col='ID').index.max()
    ps.index = [id_sale]  # Update the index of the portfolio sales DataFrame

    # Reflect the database schema and load the 'credits' and 'installments' tables
    metadata = MetaData()
    metadata.reflect(bind=engine)
    crts = Table('credits', metadata, autoload_with=engine)
    insts = Table('installments', metadata, autoload_with=engine)

    # Use a database session to update the 'credits' and 'installments' tables
    with Session(engine) as session:
        # Group installment data by operation ID and find the minimum installment number
        funded_credits = full_inst.groupby('ID_Op')['Nro_Inst'].min()

        # Update the 'credits' table with the sale ID and first installment sold
        for i in funded_credits.index:
            stmt = crts.update().where(crts.c.ID == i).values(
                ID_Sale=id_sale, First_Inst_Sold=funded_credits[i]
            )
            session.execute(stmt)

        # Update the 'installments' table with the company ID as the new owner
        for i in full_inst.index:
            stmt = insts.update().where(insts.c.ID == i).values(ID_Owner=id_company)
            session.execute(stmt)

        # Commit the changes to the database
        session.commit()

    # Return the updated sale ID
    return id_sale


def export_sell(customers, sheet_names, full_inst, credits, date, id_sale):
    """
    Exports customer, credit, and installment data to an Excel file in a selected folder.

    Parameters:
    - customers (pd.DataFrame): DataFrame containing customer data.
    - sheet_names (list): List of sheet names for the Excel file.
    - full_inst (pd.DataFrame): DataFrame containing installment data.
    - credits (pd.DataFrame): DataFrame containing credit data.
    - date (str): Date of the sale transaction, used in the file name.
    - id_sale (int): ID of the sale transaction, used in the file name.

    Returns:
    None (exports data to an Excel file in the selected folder).
    """

    # Open a folder selection dialog for the user to choose the export location
    folder_selected = QFileDialog.getExistingDirectory(None, "Select a Folder")
    
    # Raise an error if no folder is selected
    if folder_selected == '':
        raise ValueError('No folder selected.')

    # Create an Excel file in the selected folder with a name based on the sale ID and date
    with pd.ExcelWriter(f'{folder_selected}/Venta de Cartera Nro. {id_sale} - {date}.xlsx') as writer:
        # Export customer data to the first sheet
        customers.to_excel(writer, sheet_name=sheet_names[0], index=True)

        # Export credit data to the second sheet
        credits.to_excel(writer, sheet_name=sheet_names[1], index=True)
        
        # Export installment data to the third sheet
        full_inst.to_excel(writer, sheet_name=sheet_names[2], index=True)


def portfolio_seller(
        date: pd.Period,
        tna: float,
        va: float,
        id_company: int,
        sort_by_tem: bool = False,
        sort_by_emission: bool = False,
        sort_tem_emission: bool = False,
        asc: bool = True,
        default: bool = False,
        resource: bool = False,
        iva: bool = False,
        save: bool = False,
        es: bool = False,
        export: bool = False):
    """
    Perform portfolio analysis for salle, filter installments, calculate financial values, and optionally save results.

    Parameters:
        date (pd.Period): Reference date for filtering and calculations.
        tna (float): Nominal annual interest rate.
        va (float): Available value for financing.
        id_company (int): Company ID to assign ownership.
        sort_by_tem (bool): Sort by the TEM (True/False).
        sort_by_emission (bool): Sort by emission date (True/False).
        sort_tem_emission (bool): Sort by TEM and emission date (True/False).
        asc (bool): Sorting order (ascending if True, descending if False).
        default (bool): Include defaulted installments (True/False).
        resource (bool): Indicates if resource flag should be set.
        iva (bool): If True, exclude IVA from calculations.
        save (bool): If True, save the results to the database.
        es (bool): If True, translate field names to Spanish.
        export (bool): If True, export results to an Excel file.

    Returns:
        tuple: Filtered installments (`full_inst`), credits (`credits`), customers (`customers`), and portfolio sales (`ps`).
    """

    # Step 1: Filter Installments based on balance, default status, and due date
    installments = pd.read_sql('installments', engine, index_col='ID')
    installments['D_Due'] = installments['D_Due'].dt.to_period('D')
    balance = credits_balance()
    balance['D_Due'] = balance['D_Due'].dt.to_period('D')

    if default:
        full_inst = installments.loc[(balance['Total'] == installments['Total'])].copy()
    else:
        full_inst = installments.loc[
            (balance['Total'] == installments['Total']) & (installments['D_Due'] >= date)
        ].copy()
    
    # Step 2: Adjust for IVA if applicable
    if iva:
        full_inst['IVA'] = 0.0
        full_inst['Total'] = full_inst['Capital'] + full_inst['Interest']
    
    # Step 3: Filter by owner ID and create time difference column
    full_inst = installments.loc[
        (full_inst['ID_Owner'] == 1) &
        (balance['Total'] == installments['Total']) &
        (installments['D_Due'] >= date)
    ].copy()

    # Step 4: Retrieve credit details and assign TEM and emission date    
    full_inst[f'{date}'] = full_inst['D_Due'].apply(lambda x: (x - date).n)
    credits = pd.read_sql('credits', engine, index_col='ID')
    full_inst['TEM'] = full_inst['ID_Op'].apply(lambda x: credits.loc[x, 'TEM_W_IVA'])
    full_inst['D_Emission'] = full_inst['ID_Op'].apply(lambda x: credits.loc[x, 'Date_Settlement'])
    
    # Step 5: Sort installments based on provided criteria
    if sort_tem_emission and sort_by_tem and sort_by_emission:
        sort = ['TEM', 'D_Emission', 'ID_Op', 'Nro_Inst']
    elif not sort_by_emission and sort_by_emission:
        sort = ['TEM', 'ID_Op', 'Nro_Inst']
    elif sort_by_emission:
        sort = ['D_Emission', 'ID_Op', 'Nro_Inst']
    else:
        sort = ['ID_Op', 'Nro_Inst']
    
    full_inst.sort_values(by=sort, ascending=asc, inplace=True)

    # Step 6: Compute financial values (Present Value, Cumulative Value)
    full_inst['Amount_Financed'] = full_inst['Capital'] + full_inst['Interest']
    full_inst['Current_Values'] = full_inst.apply(
        lambda row: row['Amount_Financed'] / (1 + (tna / 365))**row[f'{date}'], axis=1
    )
    full_inst['Accumulated_CV'] = full_inst['Current_Values'].cumsum()
    
    # Step 7: Select viable installments based on available funds (va)
    not_inst = full_inst.loc[full_inst['Accumulated_CV'] > va]
    full_inst = full_inst.loc[full_inst['Accumulated_CV'] <= va]
    last_op = full_inst.iloc[-1]['ID_Op']
    
    if not not_inst.loc[not_inst['ID_Op'] == last_op].empty:
        full_inst.drop(
            index=full_inst.loc[full_inst['ID_Op'] == full_inst.iloc[-1]['ID_Op']].index.values, inplace=True
        )
    
    fall_inst = full_inst.groupby(['D_Emission', 'D_Due'])[['Capital', 'Amount_Financed', 'Current_Values']].sum()

    # Step 8: Generate cash flow and calculate IRR
    flow = pd.DataFrame(columns=['Amount'])
    flow.index.name = 'Period'
    
    start_date = fall_inst.index.get_level_values(0).min()
    end_date = fall_inst.index.get_level_values(1).unique().max()
    all_dates = pd.period_range(start=start_date, end=end_date, freq='D')
    
    for p in all_dates:
        flow.loc[p, 'Amount'] = 0.0
    
    for e, d in fall_inst.index:
        flow.loc[e, 'Amount'] -= fall_inst.loc[(e, d), 'Capital']
        flow.loc[d, 'Amount'] += fall_inst.loc[(e, d), 'Amount_Financed']
    
    print(f"TIR: {npf.irr(flow['Amount'])*30:,.2%}")

    # Step 9: Retrieve credits and customers information
    credits = credits.loc[credits.index.isin(full_inst['ID_Op'].unique())]
    customers = pd.read_sql('customers', engine, index_col='ID')
    customers = customers.loc[customers.index.isin(credits['ID_Client'].unique())]
    
    # Step 10: Create portfolio sales DataFrame
    ps = pd.DataFrame(columns=['Date', 'ID_Company', 'TNA', 'Resource', 'IVA'])
    ps.index.name = 'ID'
    ps.loc[0] = {
        'Date': date,
        'ID_Company': id_company,
        'TNA': tna,
        'Resource': 1 if resource else 0,
        'IVA': 1 if iva else 0
    }
    
    # Step 11: Save results to database if required
    if save:
        id_sale = sell_to_sql(ps, full_inst, id_company)
    else:
        id_sale = 'XXXX'    
        
    # Step 12: Calculate residual values and update credits DataFrame
    credits['Resid_Cap']     = full_inst.groupby('ID_Op')['Capital'].sum()
    credits['Resid_Int']     = full_inst.groupby('ID_Op')['Interest'].sum()
    credits['Resid_IVA']     = full_inst.groupby('ID_Op')['IVA'].sum()
    credits['Resid_Total']   = full_inst.groupby('ID_Op')['Total'].sum()
    credits['Current_Value'] = full_inst.groupby('ID_Op')['Current_Values'].sum().round(2)
    for c in ['First_Inst_Purch', 'First_Inst_Sold', 'ID_Purch']:
        credits[c] = credits[c].astype(int)
    credits['First_Inst_Sold'] = full_inst.groupby('ID_Op')['Nro_Inst'].min()    

    # Step 13: Define sheet names for export
    sheet_names = ['Customers', 'Credits', 'Installments']

    # Step 14: Translate field names to Spanish if required
    if es: translate_seller(full_inst, credits, customers, sheet_names)
    
    # Step 15: Export results to Excel if required        
    if export: export_sell(customers, sheet_names, full_inst, credits, date, id_sale)

    return full_inst, credits, customers, ps
