import pandas as pd
from app.modules.database.connection import engine


def add_bussines_plan(
        id_company: int,
        detail: str,
        date: pd.Timestamp = pd.Timestamp.now(),
        com: float = 0.0,
        com_6: float = 0.0,
        com_9: float = 0.0,
        com_12: float = 0.0,
        com_15: float = 0.0,
        com_18: float = 0.0,
        com_24: float = 0.0,
        com_36: float = 0.0,
        com_48: float = 0.0,
        com_coll: float = 0.0,
        com_extra: float = 0.0,
        save: bool = True):
    
    '''
    Adds a business plan to the database and associates it with a registered
    company. Validates the company ID before creating the plan. Optionally saves
    the business plan to the database and provides feedback on the operation.
    
    Parameters:
     - id_company (int): ID of the company to associate the business plan with.
     - detail (str): Description or detail of the business plan.
     - com (float): Base commission rate. Defaults to 0.0.
     - com_6 (float): Commission rate for a 6-month plan. Defaults to 0.0.
     - com_9 (float): Commission rate for a 9-month plan. Defaults to 0.0.
     - com_12 (float): Commission rate for a 12-month plan. Defaults to 0.0.
     - com_15 (float): Commission rate for a 15-month plan. Defaults to 0.0.
     - com_18 (float): Commission rate for an 18-month plan. Defaults to 0.0.
     - com_24 (float): Commission rate for a 24-month plan. Defaults to 0.0.
     - com_36 (float): Commission rate for a 36-month plan. Defaults to 0.0.
     - com_48 (float): Commission rate for a 48-month plan. Defaults to 0.0.
     - save (bool): If True, the business plan is saved to the database.
                    Defaults to True.
    
    Raises:
     - KeyError: If the `id_company` does not correspond to a registered company.
    
    Behavior:
     - The function retrieves the company data to verify the existence of the
       specified company.
     - Creates a business plan record with the given details and commission rates.
     - Optionally saves the record to the `business_plan` table in the database.
     - Prints the ID of the new business plan and the associated company's
       social reason (name) if the plan is saved successfully.
    -----------------------------------------------------------------------------
    '''

    
    # Retrieve the list of companies to validate the provided company ID
    companies = pd.read_sql('companies', engine, index_col='ID')
    if not id_company in companies.index.values:
        raise KeyError(f'El id {id_company} no corresponde a ninguna compañía registrada.')

    # Create a new business plan record
    bp = pd.read_sql('business_plan', engine, index_col='ID').iloc[0:0]  # Initialize an empty DataFrame
    # Ensure commission columns have the correct data type
    bp[['Comission', 'Comission_6', 'Comission_9', 'Comission_12', 'Comission_15', 'Comission_18', 'Comission_24', 'Comission_Collection', 'Comission_Extra']] = bp[['Comission', 'Comission_6', 'Comission_9',  'Comission_12', 'Comission_15', 'Comission_18',  'Comission_24', 'Comission_Collection', 'Comission_Extra']].astype('float')

    # Populate the business plan with provided data
    bp.loc[0] = {
        'ID_Company': id_company,
        'Detail': detail,
        'Date': date,
        'Comission': com,
        'Comission_6': com_6,
        'Comission_9': com_9,
        'Comission_12': com_12,
        'Comission_15': com_15,
        'Comission_18': com_18,
        'Comission_24': com_24,
        'Comission_36': com_36,
        'Comission_48': com_48,
        'Comission_Collection': com_coll,
        'Comission_Extra': com_extra
    }

    if save:
        # Save the business plan to the database
        bp.to_sql('business_plan', engine, index=False, if_exists='append')
        # Retrieve the updated business plans to fetch the new ID
        bp = pd.read_sql('business_plan', engine, index_col='ID')
        # Print confirmation with the ID of the new business plan
        print(f"Nuevo plan de negocio creado, su id es {max(bp.index)} y fue asociado a la empresa {companies.loc[id_company, 'Social_Reason']}")


def add_company(
        social_reason: str,
        cuit: int,
        bp: bool = False,
        save: bool = True) -> None:

    '''
    Adds a new company to the `companies` table in the database. Validates the
    CUIT (a unique identifier) and ensures no duplicate entries are added.
    Optionally associates a business plan with the new company.
    
    Parameters:
     - social_reason (str): The company's social reason (name).
     - cuit (int): The unique CUIT (Clave Única de Identificación Tributaria) of the company.
     - bp (bool): If True, a business plan will be created for the company.
                  Defaults to False.
     - save (bool): If True, the company will be saved to the database.
                    Defaults to True.
    
    Raises:
     - KeyError: If the CUIT is not valid or if the company is already registered.
    
    Behavior:
     - Validates the CUIT to ensure it has 11 digits.
     - Checks for existing companies with the same CUIT to prevent duplicates.
     - Creates and optionally saves the new company record to the database.
     - If `bp` is True, prompts the user for details to create a business plan
       for the new company.
    
    Assumptions:
     - The `companies` table contains columns: 'Social_Reason', 'CUIT', and 'Advance'.
     - The `Advance` column must be treated as a floating-point number.
     - Relies on the `add_bussines_plan` function for business plan creation.
    -----------------------------------------------------------------------------
    ''' 
    
    # Validate the CUIT length
    if len(str(cuit)) != 11:
        raise KeyError(f"{cuit} no es un CUIT valido.")  # Raise error for invalid CUIT

    # Retrieve the companies table to check for existing records
    companies = pd.read_sql('companies', engine, index_col='ID')
    companies['Advance'] = companies['Advance'].astype('float')  # Ensure 'Advance' column is float
    
    # Check for duplicate CUIT
    if cuit in companies['CUIT'].values:
        existing_company = companies.loc[companies['CUIT'] == cuit, 'Social_Reason'].values[0]
        raise KeyError(f"{existing_company} ya está registrada como un socio comercial.")
    else:
        # Prepare a new company record
        companies = companies.iloc[0:0]  # Initialize an empty DataFrame
        companies.loc[0] = {
            'Social_Reason': social_reason,
            'CUIT': cuit
        }
        companies['Advance'] = companies['Advance'].fillna(0)  # Fill missing advances with 0

        if save:
            # Save the new company to the database
            companies.to_sql('companies', engine, if_exists='append', index=False)
            companies = pd.read_sql('companies', engine, index_col='ID')
            new_company_id = max(companies.index)  # Get the new company's ID

            # Optionally create a business plan for the new company
            if bp:
                add_bussines_plan(
                    new_company_id,
                    input('Detalle del plan comercial:'),  # Prompt user for plan details
                    pd.Timestamp(input('Fecha:')),
                    float(input('Comisión por fondeo:')),
                    float(input('Comisión por cobranza (plazo 6):')),
                    float(input('Comisión por cobranza (plazo 9):')),
                    float(input('Comisión por cobranza (plazo 12):')),
                    float(input('Comisión por cobranza (plazo 15):')),
                    float(input('Comisión por cobranza (plazo 18):')),
                    float(input('Comisión por cobranza (plazo 24):')),
                    float(input('Comisión por cobranza (plazo 36):')),
                    float(input('Comisión por cobranza (plazo 48):')),
                    float(input('Comisión por cobranza:')),
                    float(input('Comisión sobre cada crédito:'))
                )
            else:
                add_bussines_plan(new_company_id, input('Detalle del plan comercial:'))  # Minimal business plan
