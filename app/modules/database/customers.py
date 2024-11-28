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
    gender = gender.strip().lower()

    # Check if the gender exists in the map and return the corresponding abbreviation
    if gender in gender_map:
        return gender_map[gender]
    
    # Raise an error if the gender is not found in the mapping
    else:
        raise ValueError(f"'{gender}' is not a valid gender. Please provide one of {', '.join(gender_map.keys())}.")


# Function to add a new customer to the system (assuming a DataFrame or database insert)
def add_customer(CUIL: int,
                 DNI: int,
                 Last_Name: str,
                 Name: str,
                 Gender: str,
                 Date_Birth: pd.Timestamp,
                 Age_at_Discharge: pd.Timestamp,
                 ID_Province: int,
                 Locality: str,
                 Street: str,
                 Nro: int,
                 CP: int,
                 Feature: str,
                 Telephone: str,
                 Seniority: float,
                 Salary: float,
                 CBU: str,
                 Collection_Entity: str,
                 Employer: str,
                 Dependence: str,
                 CUIT_Employer: int,
                 ID_Empl_Prov: int,
                 Empl_Loc: str,
                 Empl_Adress: str,
                 new_customer: pd.DataFrame = pd.DataFrame()):
    
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

    # Query the existing customers table
    df = pd.read_sql('customers', engine, index_col='ID')
    
    if not CUIL in df['CUIL'].values:
        # Add the new customer to the database
        new_customer.to_sql('customers', engine, if_exists='append', index=False)
    else:
        # Update existing customer record if differences are found
        old_row = df.loc[df['CUIL'] == new_customer.loc[0, 'CUIL']].index.values[0]
        Session = sessionmaker(bind=engine)
        session = Session()
        
        for c in df.columns.drop('Last_Update'):  # Skip metadata column
            if df.loc[old_row, c] != new_customer.loc[0, c]:
                session.query(Customer).filter(Customer.ID == old_row).update(
                    {getattr(Customer, c): new_customer.loc[0, c]}
                )
                session.commit()  # Commit each change
        
        session.close()
        
    return new_customer
