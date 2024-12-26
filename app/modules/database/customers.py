import os
import pandas as pd

# Import your module
from app.modules.database.connection import engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import update

from app.modules.database.structur_databases import Customer


# Function to convert a full gender label to its corresponding abbreviation
def categorical_gender(gender: str) -> str:
    """
    Convert a full gender label to its corresponding abbreviation for use in categorical data.

    Parameters:
    - gender: str
        The full gender label (e.g., 'male', 'female', 'other') to be converted. 
        The input is case-insensitive.

    Returns:
    - str
        The corresponding abbreviated gender value ('M', 'F', 'O', 'NB', 'T', 'F', 'A').

    Raises:
    - ValueError: If the provided gender is not recognized or not part of the defined list.
    """
    # Define a mapping of full gender labels to their abbreviations (case insensitive)
    gender_map = {
        'male': 'M',
        'female': 'F',
        'other': 'O',
        'non-binary': 'NB',
        'transgender': 'T',
        'genderfluid': 'F',
        'agender': 'A'
    }
    
    # Normalize input gender to lowercase for case-insensitive comparison
    if type(gender) == object:
        gender = gender.strip().lower()
    else:
        return None

    # Check if the gender exists in the map and return the corresponding abbreviation
    if gender in gender_map:
        return gender_map[gender]
    
    # Raise an error if the gender is not found in the mapping
    else:
        raise ValueError(f"'{gender}' is not a valid gender. Please provide one of {', '.join(gender_map.keys())}.")


# Function to add a new customer to the system (assuming a DataFrame or database insert)
def add_customer(CUIL: int = None,
                 DNI: int = None,
                 Last_Name: str = None,
                 Name: str = None,
                 Gender: str = None,
                 Date_Birth: pd.Timestamp = None,
                 Age_at_Discharge: pd.Timestamp = None,
                 ID_Province: int = None,
                 Locality: str = None,
                 Street: str = None,
                 Nro: int = None,
                 CP: int = None,
                 Feature: str = None,
                 Telephone: str = None,
                 Seniority: float = None,
                 Salary: float = None,
                 CBU: str = None,
                 Collection_Entity: str = None,
                 Employer: str = None,
                 Dependence: str = None,
                 CUIT_Employer: int = None,
                 ID_Empl_Prov: int = None,
                 Empl_Loc: str = None,
                 Empl_Adress: str = None,
                 customer: pd.DataFrame = pd.DataFrame()):
    
    '''
    Adds a new customer to the system. This function creates a customer record
    and appends it to the existing database table ('customers'). If a customer
    with the same `CUIL` already exists, it updates the database with the new
    data for any differing fields. Assumes the use of a Pandas DataFrame or SQL
    database for customer storage.

    Parameters:
     - CUIL (int): Unique tax identification number for the customer.
     - DNI (int): National identity number for the customer.
     - Last_Name (str): Last name of the customer.
     - Name (str): First name of the customer.
     - Gender (str): Gender as a string (e.g., 'M', 'F'). Validated inside the function.
     - Date_Birth (pd.Timestamp): Date of birth of the customer.
     - Age_at_Discharge (pd.Timestamp): Age of the customer at account discharge.
     - ID_Province (int): ID corresponding to the customer's province.
     - Locality (str): Customer's locality.
     - Street (str): Customer's street address.
     - Nro (int): Number of the house/building.
     - CP (int): Postal code.
     - Feature (str): Optional feature/attribute about the customer.
     - Telephone (str): Contact telephone number.
     - Seniority (float): Customer's seniority in years.
     - Salary (float): Customer's salary.
     - CBU (str): Bank account number (unique identifier).
     - Collection_Entity (str): Entity responsible for payment collection.
     - Employer (str): Name of the employer.
     - Dependence (str): Employment dependence type.
     - CUIT_Employer (int): Unique tax identification number of the employer.
     - ID_Empl_Prov (int): ID corresponding to the employer's province.
     - Empl_Loc (str): Employer's locality.
     - Empl_Adress (str): Employer's address.
     - new_customer (pd.DataFrame): A DataFrame to hold the new customer data.
                                     Defaults to an empty DataFrame.

    Returns:
     - new_customer (pd.DataFrame): DataFrame containing the new or updated customer data.

    Assumptions:
     - `categorical_gender` is a predefined function to validate/convert gender strings.
     - SQLAlchemy ORM and an active database engine `engine` are used for database access.
    -----------------------------------------------------------------------------
    '''

    if customer.empty:
        # Create a DataFrame template for new customers
        new_customer = pd.DataFrame(columns=['CUIL', 'DNI', 'Last_Name', 'Name', 'Gender', 'Date_Birth', 'Age_at_Discharge', 
                                              'ID_Province', 'Locality', 'Street', 'Nro', 'CP', 'Feature', 'Telephone', 
                                              'Seniority', 'Salary', 'CBU', 'Collection_Entity', 'Employer', 'Dependence', 
                                              'CUIT_Employer', 'ID_Empl_Prov', 'Empl_Loc', 'Empl_Adress'])
        
        # Populate the new customer record
        customer_data = {
            'CUIL': CUIL,
            'DNI': DNI,
            'Last_Name': Last_Name,
            'Name': Name,
            'Gender': categorical_gender(Gender),  # Convert/validate gender
            'Date_Birth': Date_Birth,
            'Age_at_Discharge': Age_at_Discharge,
            'ID_Province': ID_Province,
            'Locality': Locality,
            'Street': Street,
            'Nro': Nro,
            'CP': CP,
            'Feature': Feature,
            'Telephone': Telephone,
            'Seniority': Seniority,
            'Salary': Salary,
            'CBU': CBU,
            'Collection_Entity': Collection_Entity,
            'Employer': Employer,
            'Dependence': Dependence,
            'CUIT_Employer': CUIT_Employer,
            'ID_Empl_Prov': ID_Empl_Prov,
            'Empl_Loc': Empl_Loc,
            'Empl_Adress': Empl_Adress
        }

        # Insert the customer data into the DataFrame
        new_customer.loc[0, ['CUIL', 'DNI', 'Last_Name', 'Name', 'Gender', 'Date_Birth', 'Age_at_Discharge', 
                             'ID_Province', 'Locality', 'Street', 'Nro', 'CP', 'Feature', 'Telephone', 'Seniority', 
                             'Salary', 'CBU', 'Collection_Entity', 'Employer', 'Dependence', 'CUIT_Employer', 
                             'ID_Empl_Prov', 'Empl_Loc', 'Empl_Adress']] = customer_data
        
    else:
        new_customer = customer.iloc[0].copy()
        CUIL = new_customer['CUIL']
    # Query the existing customers table
    df = pd.read_sql('customers', engine, index_col='ID')
    
    if (not CUIL in df['CUIL'].values):
        # Add the new customer to the database
        pd.DataFrame(new_customer).to_sql('customers', engine, if_exists='append', index=False)
    else:
        # Update existing customer record if differences are found
        old_row = df.loc[df['CUIL'] == new_customer['CUIL']].index.values[0]
        Session = sessionmaker(bind=engine)
        session = Session()
        
        for c in df.columns.drop('Last_Update'):  # Skip metadata column
            if (df.loc[old_row, c] != new_customer[c]) or (new_customer[c] != None):
                if not pd.isna(new_customer[c]):
                    session.query(Customer).filter(Customer.ID == old_row).update(
                        {getattr(Customer, c): new_customer[c]}
                    )
                session.commit()  # Commit each change
        
        session.close()
        
    return new_customer


# This function takes a string representing a province in Argentina (either its full name or alias) and returns the corresponding ID of that province from a database table.
def id_province(prov: str) -> int:
    """
    This function takes a string representing a province in Argentina (either its full name or alias) 
    and returns the corresponding ID of that province from a database table.

    Parameters:
    prov (str): The name or alias of the Argentine province.

    Returns:
    int: The ID of the province in the database.

    Raises:
    KeyError: If the given province name or alias does not correspond to any valid Argentine province.
    """

    # Read the database table containing provinces into a pandas DataFrame
    df = pd.read_sql("provinces", engine, index_col="ID")

    # Dictionary of province names as keys, with their possible aliases as values (list of strings)
    alias = {
        'Buenos Aires': ["Buenos Aires", "Bs. As.", "Ba"],
        'Chubut': ['Chubut', 'Chub'],
        'Ciudad Autónoma de Buenos Aires': ['Ciudad Autónoma de Buenos Aires', 'CABA', 'Capital Federal', 'Ciudad de Buenos Aires', 'Ciudad Autónoma De Buenos Aires', 'Ciudad Autónoma De Bs. As.', 'Ciudad Autónoma De Bs. As.'],
        'Catamarca': ['Catamarca', 'Cat'],
        'Chaco': ['Chaco', 'Cha'],
        'Córdoba': ['Córdoba', 'Cordoba', 'Cba'],
        'Corrientes': ['Corrientes', 'Corr'],
        'Entre Ríos': ['Entre Ríos', 'Entre Rios', 'ER'],
        'Formosa': ['Formosa', 'For'],
        'Jujuy': ['Jujuy', 'Juj'],
        'La Pampa': ['La Pampa', 'LP'],
        'La Rioja': ['La Rioja', 'LR'],
        'Mendoza': ['Mendoza', 'Mdz'],
        'Misiones': ['Misiones', 'Mis'],
        'Neuquén': ['Neuquén', 'Neuquen', 'Neu'],
        'Río Negro': ['Río Negro', 'Rio Negro', 'RN'],
        'Salta': ['Salta', 'Sal'],
        'San Juan': ['San Juan', 'SJ'],
        'San Luis': ['San Luis', 'SL'],
        'Santa Cruz': ['Santa Cruz', 'SC'],
        'Santa Fe': ['Santa Fe', 'SF'],
        'Santiago del Estero': ['Santiago del Estero', 'Santiago', 'SE'],
        'Tierra del Fuego': ['Tierra del Fuego', 'TDF'],
        'Tucumán': ['Tucumán', 'Tucuman', 'Tuc']
    }

    # Clean the input province string (remove extra spaces and capitalize the first letter of each word)
    try:
        prov = prov.strip()
        prov = prov.title()

        # If the province name is not directly in the alias dictionary, try to match it with an alias
        if prov not in alias.keys():
            for p, a in alias.items():
                if prov in a:
                    prov = p  # Set the province to the correct name found in the alias
                    break

        # If the province is still not found, raise an exception
        if prov not in alias.keys():
            raise KeyError(f"{prov} no es una provincia de Argentina.")

        # If found, retrieve the province ID from the DataFrame and return it as an integer
        id = df.loc[df['Name'] == prov].index.values[0]

    except AttributeError:
        return None

    return int(id)


