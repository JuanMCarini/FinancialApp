import pandas as pd

# Import your module
from app.modules.database.connection import engine
from app.modules.database.credit_manager import credits_balance

pp = pd.read_sql('portfolio_purchases', engine, index_col='ID')

customers = pd.read_sql('customers', engine, index_col='ID')

companies = pd.read_sql('companies', engine, index_col='ID')

bp = pd.read_sql('business_plan', engine, index_col='ID')

credits = pd.read_sql('credits', engine, index_col='ID')
credits['Date_Settlement'] = credits['Date_Settlement'].dt.to_period('D')

installments = pd.read_sql('installments', engine, index_col='ID')
installments['D_Due'] = installments['D_Due'].dt.to_period('D')

collections = pd.read_sql('collection', engine, index_col='ID')
collections['ID_Op'] = collections['ID_Inst'].apply(lambda x: installments.loc[x, 'ID_Op'])
collections['D_Emission'] = collections['D_Emission'].dt.to_period('D')


def portfolio_inventory(date: pd.Period = pd.Period.now('D'), save: bool = False, es: bool = False):
    """
    Generates a detailed credit portfolio inventory for a given date.

    Parameters:
    - date (pd.Period): Target date for the portfolio inventory. Defaults to the current day.
    - save (bool): If True, saves the result to an Excel file.
    - es (bool): If True, renames the columns from English to Spanish.

    Returns:
    - pd.DataFrame: A DataFrame containing the portfolio inventory with detailed financial information.
    """
    # Merge data from credits, customers, and companies
    df = credits.merge(customers, how='inner', left_on='ID_Client', right_on='ID')
    df = df.merge(bp.drop(columns=['Detail', 'Date']), how='inner', left_on='ID_BP', right_on='ID')
    df = df.merge(companies['Social_Reason'], how='inner', left_on='ID_Company', right_on='ID')
    df.index = credits.index

    # Select and order relevant columns
    df = df[['ID_External', 'ID_Company', 'Social_Reason',
             'ID_Client', 'CUIL', 'DNI', 'Last_Name', 'Name', 'Gender', 'Date_Birth', 'Marital_Status', 'Age_at_Discharge',
             'Country', 'ID_Province', 'Locality', 'Street', 'Nro', 'CP', 'Feature', 'Telephone', 'Seniority', 'Salary', 
             'CBU', 'Collection_Entity', 'Employer', 'Dependence', 'CUIT_Employer', 'ID_Empl_Prov', 'Empl_Loc', 'Empl_Adress', 
             'Last_Update', 'Date_Settlement', 'ID_BP', 'Cap_Requested', 'Cap_Grant', 'TEM_W_IVA', 'N_Inst', 'D_F_Due', 
             'ID_Purch', 'First_Inst_Purch', 'V_Inst', 'ID_Sale', 'First_Inst_Sold']]

    # Calculate days in default based on the balance
    balance = credits_balance(pd.Period.to_timestamp(date))
    balance['D_Due'] = balance['D_Due'].dt.to_period('D')
    balance['Days_in_Default'] = balance.apply(lambda row: (date - row['D_Due']).n if ((date - row['D_Due']).n > 0) and (row['Total'] > 0.009) else 0, axis=1)
    df['Days_in_Default'] = balance.groupby('ID_Op')['Days_in_Default'].max().values

    # Determine the last collection date for each operation
    df.loc[df.index.isin(collections['ID_Op'].values), 'Last_Collection'] = collections.groupby('ID_Op')['D_Emission'].max()
    df.loc[~df.index.isin(collections['ID_Op'].values), 'Last_Collection'] = None

    # Calculate collected, overdue, due, and owed amounts
    for concept in ['Capital', 'Interest', 'IVA', 'Total']:
        df.loc[df.index.isin(collections['ID_Op']), f'{concept}_Collected'] = collections.groupby('ID_Op')[concept].sum()
        df.loc[~df.index.isin(collections['ID_Op']), f'{concept}_Collected'] = 0.0
        df[f'{concept}_in_Default'] = df.apply(
            lambda row: 0 if (row['Days_in_Default'] == 0) else balance.loc[(balance['ID_Op'] == row.name) & (balance['D_Due'] <= date), concept].sum(), axis=1)
        df[f'{concept}_to_Due'] = df.apply(
            lambda row: balance.loc[(balance['ID_Op'] == row.name) & (balance['D_Due'] > date), concept].sum(), axis=1)
        df[f'{concept}_Owed'] = balance.groupby('ID_Op')[concept].sum()

    # Calculate days since the last collection
    df['Days_since_last_Collection'] = df['Last_Collection'].apply(lambda x: 0 if pd.isna(x) else (date - x).n)

    # Final column selection and order
    df = df[['ID_External', 'ID_Company', 'Social_Reason', 'ID_Client', 'CUIL', 'DNI', 'Last_Name', 'Name', 'Gender',
             'Date_Birth', 'Marital_Status', 'Age_at_Discharge', 'Country', 'ID_Province', 'Locality', 'Street', 'Nro',
             'CP', 'Feature', 'Telephone', 'Seniority', 'Salary', 'CBU', 'Collection_Entity', 'Employer', 'Dependence',
             'CUIT_Employer', 'ID_Empl_Prov', 'Empl_Loc', 'Empl_Adress', 'Last_Update', 'Date_Settlement', 'ID_BP',
             'Cap_Requested', 'Cap_Grant', 'TEM_W_IVA', 'N_Inst', 'D_F_Due', 'ID_Purch', 'First_Inst_Purch', 'V_Inst',
             'ID_Sale', 'First_Inst_Sold', 'Last_Collection', 'Capital_Collected', 'Interest_Collected', 'IVA_Collected',
             'Total_Collected', 'Days_in_Default', 'Capital_in_Default', 'Interest_in_Default', 'IVA_in_Default',
             'Total_in_Default', 'Days_since_last_Collection', 'Capital_to_Due', 'Interest_to_Due', 'IVA_to_Due',
             'Total_to_Due', 'Capital_Owed', 'Interest_Owed', 'IVA_Owed', 'Total_Owed']]

    # Rename columns to Spanish if es is True
    if es:
        df = df.rename(columns={
            'ID_External': 'ID_Externo',
            'ID_Company': 'ID_Empresa',
            'Social_Reason': 'Razón_Social',
            # Additional translations...
        })

    # Save to Excel if save is True
    if save:
        df.reset_index().to_excel(f'outputs/Portfolio Inventory - {date}.xlsx', index=False)

    return df


def fall_inst(emission_from: pd.Period = pd.Period("1900/01/01"),
              emission_until: pd.Period = pd.Period.now('D'),
              save: bool = False,
              es: bool = False):
    """
    Generate a summary of outstanding installments by period and social reason.

    This function filters credits and balances within a specified emission date range,
    calculates outstanding amounts grouped by due date and social reason, and optionally
    saves the results to an Excel file.

    Parameters:
    - emission_from (pd.Period): Start date for filtering emissions (default: "1900/01/01").
    - emission_until (pd.Period): End date for filtering emissions (default: today).
    - save (bool): Whether to save the resulting DataFrame to an Excel file (default: False).
    - es (bool): If True, column names will be translated to Spanish (default: False).

    Returns:
    - pd.DataFrame: A DataFrame containing the grouped summary of outstanding installments.
    """

    # Retrieve the balance of credits up to the specified end date
    balance = credits_balance(emission_until)

    # Load credits data and filter based on the specified emission range
    credits = pd.read_sql('credits', engine, index_col='ID')
    credits['Date_Settlement'] = credits['Date_Settlement'].dt.to_period('D')
    credits_filter = credits.loc[credits['Date_Settlement'] >= emission_from].index.values
    balance = balance.loc[balance['ID_Op'].isin(credits_filter)]

    # Convert due dates to monthly periods for aggregation
    balance['D_Due'] = balance['D_Due'].dt.to_period('M')

    # Merge credits with associated business partner and company data
    credits = credits.merge(bp[['ID_Company']], how='left', left_on='ID_BP', right_index=True)
    credits = credits.merge(companies[['Social_Reason']], how='left', left_on='ID_Company', right_index=True)

    # Map 'Social_Reason' from credits to the balance DataFrame
    balance['Social_Reason'] = balance['ID_Op'].apply(lambda x: credits.loc[x, 'Social_Reason'])

    # Filter out zero-balance rows and group by due date and social reason
    balance = balance.loc[balance['Total'] != 0].groupby(['D_Due', 'Social_Reason'])[['Capital', 'Interest', 'IVA', 'Total']].sum().reset_index()

    # Rename columns for better readability
    balance.rename(columns={'D_Due': 'Period'}, inplace=True)

    # Translate column names to Spanish if specified
    if es:
        balance.rename(columns={
            'Period': 'Periodo',
            'Social_Reason': 'Razón Social',
            'Interest': 'Intereses'
        }, inplace=True)

    # Save the resulting DataFrame to an Excel file if specified
    if save:
        balance.to_excel(f'outputs/Installments Fall - {emission_from} - {emission_until}.xlsx', index=False)

    return balance


