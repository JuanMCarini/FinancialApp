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
    """
    Adds a business plan to the database and associates it with a registered company.

    Validates the company ID before creating the plan. Optionally saves the business
    plan to the database and provides feedback on the operation.

    Parameters:
        id_company (int): ID of the company to associate the business plan with.
        detail (str): Description or detail of the business plan.
        date (pd.Timestamp): Date of creation. Defaults to the current timestamp.
        com (float): Base commission rate. Defaults to 0.0.
        com_6, com_9, com_12, com_15, com_18, com_24, com_36, com_48 (float): 
            Commission rates for different time frames. Defaults to 0.0.
        com_coll (float): Collection commission rate. Defaults to 0.0.
        com_extra (float): Extra commission rate. Defaults to 0.0.
        save (bool): If True, the business plan is saved to the database. Defaults to True.

    Raises:
        KeyError: If the `id_company` does not correspond to a registered company.
    """

    # Verify if the company exists in the database
    company_exists = pd.read_sql(f"SELECT COUNT(*) FROM companies WHERE ID = {id_company}", engine).iloc[0, 0]
    if company_exists == 0:
        raise KeyError(f"El ID {id_company} no corresponde a ninguna compañía registrada.")

    # Create business plan dictionary
    business_plan_data = {
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

    # Convert dictionary to DataFrame
    bp = pd.DataFrame([business_plan_data])

    if save:
        try:
            # Save the business plan to the database
            bp.to_sql('business_plan', engine, index=False, if_exists='append')
            
            # Retrieve the new business plan ID
            new_bp_id = pd.read_sql("SELECT MAX(ID) FROM business_plan", engine).iloc[0, 0]

            # Retrieve the company's name
            company_name = pd.read_sql(f"SELECT Social_Reason FROM companies WHERE ID = {id_company}", engine).iloc[0, 0]

            # Print confirmation message
            print(f"✅ New business plan created, its ID is {new_bp_id} and it was associated with the company {company_name}.")

        except Exception as e:
            print(f"❌ Error adding the business plan: {e}")


def add_company(
        social_reason: str,
        cuit: int,
        bp: bool = False,
        save: bool = True) -> None:
    """
    Adds a new company to the `companies` table in the database.
    
    Validates the CUIT (unique identifier) and ensures no duplicate entries.
    Optionally, associates a business plan with the new company.

    Parameters:
        social_reason (str): The company's social reason (name).
        cuit (int): The unique CUIT (Clave Única de Identificación Tributaria).
        bp (bool): If True, a business plan will be created. Defaults to False.
        save (bool): If True, saves the company to the database. Defaults to True.

    Raises:
        KeyError: If the CUIT is invalid or already registered.
    """

    # Validate CUIT
    cuit_str = str(cuit)
    if not cuit_str.isdigit() or len(cuit_str) != 11:
        raise KeyError(f"{cuit} no es un CUIT válido.")

    # Load existing companies
    companies = pd.read_sql('companies', engine, index_col='ID')
    companies['Advance'] = companies['Advance'].astype(float)

    # Check if CUIT is already registered
    existing = companies.loc[companies['CUIT'] == cuit, 'Social_Reason']
    if not existing.empty:
        raise KeyError(f"{existing.values[0]} ya está registrada como un socio comercial.")

    # Create new company DataFrame
    new_company = pd.DataFrame.from_records([{
        'Social_Reason': social_reason,
        'CUIT': cuit,
        'Advance': 0  # Default value
    }])

    # Save to database
    if save:
        try:
            new_company.to_sql('companies', engine, if_exists='append', index=False)
            print(f"✅ The company {social_reason} has been added.")
            
            # Retrieve new company ID
            new_company_id = pd.read_sql('companies', engine, index_col='ID').index.max()

            # Add business plan if requested
            detail = input('Detalle del plan comercial:')
            params = [new_company_id, detail]
            if bp:
                params.extend([
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
                    float(input('Comisión sobre cada crédito:'))])
            add_bussines_plan(*params)

        except Exception as e:
            print(f"❌ Error al agregar la empresa: {e}")
