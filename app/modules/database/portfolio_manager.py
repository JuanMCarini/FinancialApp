import os
import pandas as pd
import numpy_financial as npf
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import Session


# Import your module
from app.modules.database.connection import engine
from app.modules.database.customers import id_province, categorical_gender, add_customer, MaritalStatus
from app.modules.database.credit_manager import new_credit, credits_balance


def validate_supplier_and_business_plan(id_supplier, id_bp):
    """
    Validates if a given supplier ID exists in the 'companies' table and 
    checks if a given business plan ID is associated with the supplier.
    
    Parameters:
        engine: SQLAlchemy Engine
            A database connection engine.
        id_supplier: int or str
            The ID of the supplier to validate.
        id_bp: int or str
            The ID of the business plan to validate.
    
    Raises:
        ValueError: If the supplier ID does not exist or if the business plan ID is not associated 
                    with the given supplier.
    """
    # Load the 'companies' table with the 'ID' column as the index
    companies = pd.read_sql('companies', engine, index_col='ID')

    # Validate if the supplier ID exists
    if id_supplier not in companies.index:
        raise ValueError(f"{id_supplier} is not a valid company value.")
    
    # Load the 'business_plan' table with the 'ID' column as the index
    bp = pd.read_sql('business_plan', engine, index_col='ID')

    # Filter business plans for those associated with the supplier company
    supplier_bp = bp.loc[bp['ID_Company'] == id_supplier]

    # Validate if the business plan ID exists in the filtered plans
    if id_bp not in supplier_bp.index:
        raise ValueError(
            f"{id_bp} is not a business plan associated with {companies.loc[id_supplier]}.\n"
            f"The actual business plans are:\n{supplier_bp}"
        )


def load_file(path):
    """
    Loads a file into a pandas DataFrame, supporting CSV and Excel files.

    Parameters:
        path: str
            The file path to load.

    Returns:
        DataFrame: The loaded data.

    Raises:
        FileNotFoundError: If the file does not exist at the specified path.
        ValueError: If the file type is unsupported.
    """
    # Ensure the file path exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"The file '{path}' does not exist.")

    # Get the file extension to determine how to read the file
    _, extension = os.path.splitext(path)
    extension = extension.lower()

    # Read the file based on the extension (CSV or Excel)
    if extension == '.csv':
        # Read CSV file assuming semicolon as separator (adjust as needed)
        df = pd.read_csv(path, sep=";")
    elif extension == '.xlsx':
        # Read Excel file
        df = pd.read_excel(path)

    df.set_index('ID', inplace=True)

    return df


def read_data(path, model: bool = False, id_supplier = None, iqua: bool = False, cfl: bool= False):
    # Check for conflicting flags
    if (model and iqua) or (model and cfl) or (iqua and cfl):
        raise ValueError("Cannot set more than one flag (model, iqua, and cfl) to True at the same time.")
    
    # Check for missing id_supplier when none of model, iqua, or cfl is True
    elif not (model or iqua or cfl) and not id_supplier:
        raise ValueError("id_supplier must be provided if none of the flags (model, iqua, cfl) are set.")

    # Load data based on the flags
    if model:
        df = load_file(path)
        for c in ['Last_Name', 'Name']:
            df[c] = df[c].str.title()

        marital_status_map = {
            'SINGLE': MaritalStatus.SINGLE,
            'COHABITATION': MaritalStatus.COHABITATION,
            'MARRIED': MaritalStatus.MARRIED,
            'WIDOW': MaritalStatus.WIDOW,
            'DIVORCE': MaritalStatus.DIVORCE
            }
        # Now replace the values in the 'Marital_Status' column
        df['Marital_Status'] = df['Marital_Status'].replace(marital_status_map)

    elif iqua:
        pass  # Implement loading logic for iqua if needed
    elif cfl:
        pass  # Implement loading logic for cfl if needed
    else:
        if id_supplier == 2:
            # Read data from the provided Excel file into a DataFrame
            df = pd.DataFrame(columns=[
                'CUIL', 'DNI', 'Last_Name', 'Name', 'Date_Birth', 'Gender', 'Marital_Status', 'Age_at_Discharge',
                'Country', 'Province', 'Locality', 'Street', 'Nro', 'CP', 'Feature', 'Email',
                'Telephone', 'Seniority', 'Salary', 'CBU', 'Collection_Entity',
                'Employer', 'Dependence', 'CUIT_Employer', 'Empl_Prov', 'Empl_Loc',
                'Empl_Adress', 'Date_Settlement', 'Cap_Requested', 'Cap_Grant',
                'N_Inst', 'First_Inst_Purch', 'TEM_W_IVA', 'V_Inst', 'D_F_Due'
            ])
            df.index.name = 'ID'

            first_inst = pd.read_excel(path, sheet_name='Detalle')
            first_inst = first_inst.groupby('id_credito')['nro_cuota'].min()

            credits = pd.read_excel(path, sheet_name='Creditos', index_col='Numero credito original')
            if credits['CUIL del Cliente'].dtype != 'int64':
                credits['CUIL del Cliente'] = credits['CUIL del Cliente'].str.replace('-', '').astype(int)
            credits['DNI'] = credits['CUIL del Cliente'] % 10 ** 9 // 10
            credits['Apellido y nombre del cliente'] = credits['Apellido y nombre del cliente'].str.split(", ")
            credits['Domicilio'] = credits['Domicilio'].str.split(' - ')
            for i in credits.index:
                df.loc[i] = {
                    'CUIL': credits.loc[i, 'CUIL del Cliente'],
                    'DNI': credits.loc[i, 'DNI'],
                    'Last_Name': credits.loc[i, 'Apellido y nombre del cliente'][0].title(),
                    'Name': credits.loc[i, 'Apellido y nombre del cliente'][1].title(),
                    'Date_Birth': credits.loc[i, 'Fecha y Lugar de Nacimiento'],
                    'Gender': 'O',
                    'Marital_Status': MaritalStatus.SINGLE,
                    'Age_at_Discharge': None,
                    'Province': credits.loc[i, 'Provincia'],
                    'Locality': credits.loc[i, 'Domicilio'][1],
                    'Street': credits.loc[i, 'Domicilio'][0],
                    'Nro': None,
                    'CP': credits.loc[i, 'Codigo Postal'],
                    'Feature': None,
                    'Email': credits.loc[i, 'Mail'],
                    'Telephone': credits.loc[i, 'Telefono'],
                    'Seniority': None,
                    'Salary': credits.loc[i, 'Actividad Laboral (Ingreso mensual)'],
                    'CBU': None,
                    'Collection_Entity': credits.loc[i, 'Decreto 1412'],
                    'Employer': None,
                    'Dependence': None,
                    'CUIT_Employer': None,
                    'Empl_Prov': None,
                    'Empl_Loc': None,
                    'Empl_Adress': None,
                    'Date_Settlement': credits.loc[i, 'Fecha de Liquidacion '],
                    'Cap_Requested': credits.loc[i, 'Capital'],
                    'Cap_Grant': credits.loc[i, 'Capital'],
                    'N_Inst': credits.loc[i, 'n.º de Cuota'],
                    'First_Inst_Purch': first_inst[i],
                    'TEM_W_IVA': None,
                    'V_Inst': credits.loc[i, 'Monto de cuota'],
                    'D_F_Due': credits.loc[i, 'Fecha de Vencimiento']
                }
        
    return df


def update_customers(df, date, save):
    """
    Updates the 'customers' table in the database by identifying new customers and updating existing ones.

    Parameters:
        df: DataFrame
            The input DataFrame containing customer data to be processed.
        date: str or datetime
            The date to assign to the 'Last_Update' column.
        save: bool, optional (default=True)
            If True, saves changes to the database; otherwise, modifies data locally.

    Returns:
        DataFrame: Updated customers DataFrame.
    """
    # Load existing customers table
    customers = pd.read_sql('customers', engine, index_col='ID')

    # Process province IDs
    df['ID_Province'] = [id_province(p) for p in df['Province']]
    df['ID_Empl_Prov'] = [id_province(p) for p in df['Empl_Prov']]
    df['Last_Update'] = date
    df['Gender'] = [categorical_gender(g) for g in df['Gender']]
    df['Country'] = df['Country'].fillna('Argentina', axis=0)

    # Filter columns to match existing customers table
    new_customers = df[[col for col in customers.columns if col in df.columns]]

    # Separate new and existing customers
    act_customers = new_customers.loc[new_customers['CUIL'].isin(customers['CUIL'])]
    new_customers = new_customers.loc[~new_customers['CUIL'].isin(customers['CUIL'])]

    if save:
        # Save new customers to database
        if not new_customers.empty:
            new_customers.to_sql('customers', engine, index=False, if_exists='append')

        # Update existing customers
        if not act_customers.empty:
            act_customer = act_customers.copy()
            for i in act_customers.index:
                if not act_customer.empty:
                    add_customer(customer=act_customer)
                    act_customer = act_customer.drop(index=i)

        # Reload updated customers table
        customers = pd.read_sql('customers', engine, index_col='ID')
        new_customers = customers.loc[customers['CUIL'].isin(df['CUIL'])]
    else:
        # Update indices for local changes
        act_customers.index = [
            customers.loc[customers['CUIL'] == act_customers.loc[i, 'CUIL']].index[0]
            for i in act_customers.index
        ]
        new_customers.index = [
            customers.index.max() + i + 1 for i in range(len(new_customers))
        ]
        new_customers = pd.concat([act_customers, new_customers], ignore_index=True)

        # Adjust indices if customers table is empty
        if customers.empty:
            new_customers.index += 1

    return new_customers


def add_portfolio_purchase(date, id_supplier, tna, buyback, resource, iva, save):
    """
    Adds a new record to the 'portfolio_purchases' table in the database.

    Parameters:
        engine: SQLAlchemy Engine
            A database connection engine.
        date: str or datetime
            The date of the portfolio purchase.
        id_supplier: int
            The ID of the supplier company associated with the purchase.
        tna: float
            The TNA (nominal annual rate) value.
        buyback: bool
            Indicates if the purchase has a buyback guarantee.
        resource: bool
            Indicates if the purchase involves a resource.
        iva: bool
            Indicates if the purchase includes VAT.
        save: bool, optional (default=True)
            If True, saves changes to the database; otherwise, modifies data locally.

    Returns:
        DataFrame: The updated 'portfolio_purchases' DataFrame with the new entry added.
    """
    # Load existing 'portfolio_purchases' table
    pp = pd.read_sql('portfolio_purchases', engine, index_col='ID')

    # Determine the new purchase ID
    id_purch = int(pp.index.max() + 1) if not pp.empty else 1

    # Create a new record
    new_purchase = {
        'Date': date,
        'ID_Company': id_supplier,
        'TNA': tna,
        'Buyback': 1 if buyback else 0,
        'Resource': 1 if resource else 0,
        'IVA': 1 if iva else 0
    }

    # Append the new record to the DataFrame
    pp = pp.iloc[0:0]  # Clear the DataFrame while retaining the structure
    pp.loc[id_purch] = new_purchase
    
    if save:
        pp.to_sql('portfolio_purchases', engine, index=False, if_exists='append')

    return pp


def process_portfolio(df, new_customers, id_bp, id_purch, iva, date, save):
    """
    Processes a portfolio by generating new credits, installments, and collections.

    This function:
    - Calculates `TEM_W_IVA` for missing or zero values in the input DataFrame `df`.
    - Generates new credits and corresponding installments based on data in `df` and `new_customers`.
    - Updates the `new_credits` and `installments` DataFrames with calculated values.
    - Creates collection entries for installments that are marked as 'NO COMPRADA'.
    - Ensures unique IDs for new credits and installments by querying the database.

    Args:
    df (pandas.DataFrame): The DataFrame containing credit and installment data to be processed.
    new_customers (pandas.DataFrame): The DataFrame containing information about new customers.
    id_bp (int): The business plan ID to be used in credit creation.
    id_purch (int): The purchase ID to be used in credit creation.
    date (str): The date for the collection emission (used in the collections DataFrame).
    save: bool, optional (default=True): If True, saves changes to the database; otherwise, modifies data locally.

    Returns:
    tuple: A tuple containing three pandas DataFrames:
        - new_credits (pandas.DataFrame): The DataFrame containing the newly generated credits.
        - installments (pandas.DataFrame): The DataFrame containing the newly generated installments.
        - collections (pandas.DataFrame): The DataFrame containing the collections based on installment conditions.
    """

    # Initialize lists to collect new credits and installments
    new_credits = []
    installments = []
    
    # Calculate TEM_W_IVA where necessary
    for i in df.index:
        if pd.isna(df.loc[i, 'TEM_W_IVA']) or df.loc[i, 'TEM_W_IVA'] == 0.0:
            df.loc[i, 'TEM_W_IVA'] = npf.rate(df.loc[i, 'N_Inst'], df.loc[i, 'V_Inst'], -df.loc[i, 'Cap_Grant'], 0.0, guess = 0.1)
    
    # Get last credit ID to ensure unique IDs for new credits
    last_credit = pd.read_sql('credits', engine, index_col='ID')
    last_credit = 0 if last_credit.empty else last_credit.index.max()
    
    # Loop through the DataFrame to generate new credits and installments
    for i in df.index:
        last_credit += 1
        nc, insts = new_credit(
            id_customer=new_customers.loc[new_customers['CUIL'] == df.loc[i, 'CUIL']].index.values[0],
            Date_Settlement=df.loc[i, 'Date_Settlement'],
            ID_BP=id_bp,
            Cap_Requested=df.loc[i, 'Cap_Requested'],
            Cap_Grant=df.loc[i, 'Cap_Grant'],
            N_Inst=df.loc[i, 'N_Inst'],
            TEM_W_IVA=df.loc[i, 'TEM_W_IVA'],
            V_Inst=df.loc[i, 'V_Inst'],
            D_F_Due=df.loc[i, 'D_F_Due'],
            ID_Purch=id_purch,
            First_Inst_Purch=df.loc[i, 'First_Inst_Purch'],
            ID_Sale=None,
            First_Inst_Sold=0,
            id_external=i,
            massive=last_credit
        )
        # Append the generated new credit and installments to respective lists
        new_credits.append(nc)
        installments.append(insts)
    
    # Concatenate all new credits and installments
    new_credits = pd.concat(new_credits, ignore_index=False)
    new_credits['First_Inst_Purch'] = new_credits['First_Inst_Purch'].astype(int)
    installments = pd.concat(installments, ignore_index=False, join='outer')
    
    # Get the last installment ID from the database
    last_inst = pd.read_sql('installments', engine, index_col='ID')
    last_inst = last_inst.index.max() if not last_inst.empty else 0
    
    # Reset index for installments and assign new unique indices
    installments.reset_index(drop=True, inplace=True)
    new_index = [last_inst + i for i in range(1, len(installments)+1)]
    installments.index = new_index
    
    if not iva:
        installments['IVA'] = 0
        installments['Total'] = installments['Capital'] + installments['Interest']
        
    # Ensure ID_Op and Nro_Inst are integers
    installments['ID_Op'] = installments['ID_Op'].astype(int)
    installments['Nro_Inst'] = installments['Nro_Inst'].astype(int)
    # Initialize an empty collection DataFrame
    collections = pd.read_sql('collection', engine, index_col='ID').iloc[0:0]
    
    # Process collections based on installment conditions
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

    if save:
        new_credits.to_sql('credits', engine, index=False, if_exists='append')
        installments.to_sql('installments', engine, index=False, if_exists='append')
        collections.to_sql('collection', engine, index=False, if_exists='append')

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
    Function to process the purchase of a portfolio of credits, update customers, 
    and generate necessary data for further analysis or storage.

    Parameters:
    - path (str): Path to the input data file (CSV or Excel).
    - id_supplier (int): ID of the supplier providing the portfolio.
    - id_bp (int): ID of the business plan associated with the purchase.
    - tna (float): The annual nominal interest rate (TNA).
    - resource (bool): Indicates whether resources are involved in the purchase.
    - iva (bool): Indicates whether VAT applies to the purchase.
    - buyback (bool): Optional flag for buyback conditions (default is False).
    - date (pd.Timestamp): Date of the transaction (default is the current date).
    - model (bool): Optional flag to determine if the model is used (default is True).
    - iqua (bool): Flag for a specific type of data processing (default is False).
    - cfl (bool): Another optional flag for data processing (default is False).
    - save (bool): Flag to indicate if the data should be saved to the database.

    Returns:
    - df (DataFrame): Processed data after the portfolio is purchased.
    - new_customers (DataFrame): Updated customer information.
    - pp (DataFrame): Portfolio purchase data.
    - new_credits (DataFrame): Processed new credits.
    - installments (DataFrame): Processed installment information.
    - collections (DataFrame): Processed collection information.
    """
    
    # Step 1: Validate the supplier and business plan to ensure they are correct.
    validate_supplier_and_business_plan(id_supplier, id_bp)

    # Step 2: Read the data from the given path and process it based on various flags.
    df = read_data(path, model, id_supplier, iqua, cfl)

    # Step 3: Update customer information based on the data and current date.
    new_customers = update_customers(df, date, save)

    # Step 4: Add the portfolio purchase to the system and retrieve its ID.
    pp = add_portfolio_purchase(date, id_supplier, tna, buyback, resource, iva, save)
    id_purch = pp.index.values[0]  # Get the first purchase ID

    # Step 5: Process the portfolio to generate new credits, installments, and collections.
    new_credits, installments, collections = process_portfolio(df, new_customers, id_bp, id_purch, iva, date, save)

    # Return the processed data.
    return df, new_customers, pp, new_credits, installments, collections


def portfolio_seller(date: pd.Period,
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
                     save: bool = False):
    """
    Perform portfolio analysis and optionally save the results to a database.

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

    Returns:
        tuple: Filtered installments (`full_inst`), grouped installments (`fall_inst`), and cash flow (`flow`).
    """
    # Step 1: Filter Installments
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
    
    if iva:
        full_inst['IVA'] = 0.0
        full_inst['Total'] = full_inst['Capital'] + full_inst['Interest']
    
    full_inst = installments.loc[
        (full_inst['ID_Owner'] == 1) &
        (balance['Total'] == installments['Total']) &
        (installments['D_Due'] >= date)
    ].copy()
    
    full_inst[f'{date}'] = full_inst['D_Due'].apply(lambda x: (x - date).n)
    credits = pd.read_sql('credits', engine, index_col='ID')
    full_inst['TEM'] = full_inst['ID_Op'].apply(lambda x: credits.loc[x, 'TEM_W_IVA'])
    full_inst['D_Emission'] = full_inst['ID_Op'].apply(lambda x: credits.loc[x, 'Date_Settlement'])
    
    # Step 2: Sorting
    if sort_tem_emission and sort_by_tem and sort_by_emission:
        sort = ['TEM', 'D_Emission', 'ID_Op', 'Nro_Inst']
    elif not sort_by_emission and sort_by_emission:
        sort = ['TEM', 'ID_Op', 'Nro_Inst']
    elif sort_by_emission:
        sort = ['D_Emission', 'ID_Op', 'Nro_Inst']
    else:
        sort = ['ID_Op', 'Nro_Inst']
    
    full_inst.sort_values(by=sort, ascending=asc, inplace=True)

    # Step 3: Calculate Financial Values
    full_inst['Amount_Financed'] = full_inst['Capital'] + full_inst['Interest']
    full_inst['Current_Values'] = full_inst.apply(
        lambda row: row['Amount_Financed'] / (1 + (tna / 365))**row[f'{date}'], axis=1
    )
    full_inst['Accumulated_CV'] = full_inst['Current_Values'].cumsum()
    
    not_inst = full_inst.loc[full_inst['Accumulated_CV'] > va]
    full_inst = full_inst.loc[full_inst['Accumulated_CV'] <= va]
    last_op = full_inst.iloc[-1]['ID_Op']
    
    if not not_inst.loc[not_inst['ID_Op'] == last_op].empty:
        full_inst.drop(
            index=full_inst.loc[full_inst['ID_Op'] == full_inst.iloc[-1]['ID_Op']].index.values, inplace=True
        )
    
    fall_inst = full_inst.groupby(['D_Emission', 'D_Due'])[['Capital', 'Amount_Financed', 'Current_Values']].sum()

    # Step 4: Generate Cash Flow
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

    credits = credits.loc[credits.index.isin(full_inst['ID_Op'].unique())]
    customers = pd.read_sql('customers', engine, index_col='ID')
    customers = customers.loc[customers.index.isin(credits['ID_Client'].unique())]
    
    # Step 5: Save Results (Optional)
    if save:
        ps = pd.DataFrame(columns=['Date', 'ID_Company', 'TNA', 'Resource', 'IVA'])
        ps.index.name = 'ID'
        ps.loc[0] = {
            'Date': date,
            'ID_Company': id_company,
            'TNA': tna,
            'Resource': 1 if resource else 0,
            'IVA': 1 if iva else 0
        }
        ps.to_sql('portfolio_sales', engine, index=False, if_exists='append')
        id_sale = pd.read_sql('portfolio_sales', engine, index_col='ID').index.max()
        ps.index = [id_sale]

        metadata = MetaData()
        metadata.reflect(bind=engine)
        crts = Table('credits', metadata, autoload_with=engine)
        insts = Table('installments', metadata, autoload_with=engine)
        
        with Session(engine) as session:
            funded_credits = full_inst.groupby('ID_Op')['Nro_Inst'].min()
            for i in funded_credits.index:
                stmt = crts.update().where(crts.c.ID == i).values(
                    ID_Sale=id_sale, First_Inst_Sold=funded_credits[i]
                )
                session.execute(stmt)
            
            for i in full_inst.index:
                stmt = insts.update().where(insts.c.ID == i).values(ID_Owner=id_company)
                session.execute(stmt)
            session.commit()

        return full_inst, credits, customers, ps
        
    return full_inst, credits, customers
