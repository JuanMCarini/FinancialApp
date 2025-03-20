import os
import pandas as pd

# Import your module
from app.modules.database.connection import engine

from enum import Enum
from sqlalchemy import update
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError
from app.modules.database.credit_manager import credits_balance
from app.modules.database.structur_databases import Company, Collection
from sqlalchemy.exc import IntegrityError as alIE, SQLAlchemyError
from pymysql.err import IntegrityError as myIE

class TypeDataCollection(Enum):
    CUIL = 'CUIL'
    DNI = 'DNI'
    ID_Op = 'ID_Op'
    ID_Ext = 'ID_External'

    @staticmethod
    def validate(value: str, type: 'TypeDataCollection'):
        """
        Valida que el valor tenga la longitud correcta según el tipo de dato.
        
        Args:
            value (str): El valor que se va a validar.
            type (TypeDataCollection): El tipo de dato (CUIL o DNI).
        
        Returns:
            bool: True si el valor es válido, False de lo contrario.
        
        Raises:
            ValueError: Si el valor no es válido.
        """

        if type in [TypeDataCollection.ID_Op, TypeDataCollection.ID_Ext]:
            df_crs = pd.read_sql('credits', engine, index_col='ID')
        elif type in [TypeDataCollection.CUIL, TypeDataCollection.DNI]:
            df_cst = pd.read_sql('customers', engine, index_col='ID')

        if type == TypeDataCollection.CUIL:
            if len(str(value)) != 11:
                raise ValueError(f"CUIL debe tener exactamente 11 dígitos.")
            elif value not in df_cst['CUIL'].values:
                raise ValueError(f"El cliente con CUIL {value} no esta en la base de datos.")
            else:
                return True
        elif type == TypeDataCollection.DNI:
            if len(str(value).rjust(8, "0")) != 8:
                raise ValueError(f"DNI debe tener exactamente 8 dígitos.")
            elif value not in df_cst['DNI'].values:
                raise ValueError(f"El cliente con DNI {value} no esta en la base de datos.")
            else:
                return True   
        elif type == TypeDataCollection.ID_Op:
            if value in df_crs.index:
                return True
            else:
                raise ValueError(f"{value} no es un crédito de la base de datos")
        elif type == TypeDataCollection.ID_Ext:
            if value in df_crs['ID_External'].values:
                return True
            else:
                raise ValueError(f"{value} no es un crédito de la base de datos")


class IdentifierError(Exception):
    def __init__(self, type_data, data, message=f"There are no credits for the customer."):
        self.type = type_data
        self.data = data

    def __str__(self):
        return f"There are no credits for the customer ({self.type.name}: {self.data})."


class ResourceError(Exception):
    def __init__(self, type_data, data, message=f"There are no credits without recourse for the customer."):
        self.type = type_data
        self.data = data

    def __str__(self):
        return f"There are no credits without recourse for the customer ({self.type.name}: {self.data})."


def _get_customer_id(ident_type, identifier):
    """
    Retrieve the customer ID based on the identifier type.

    Args:
        ident_type (TypeDataCollection): Type of identifier (DNI or CUIL).
        identifier (int): The identifier value to look up.

    Returns:
        int or None: The customer ID if found, otherwise None.

    Raises:
        ValueError: If the identifier type is invalid.
    """
    # ✅ Load customer data
    customers = pd.read_sql('customers', engine, index_col='ID')

    # ✅ Check identifier type and filter safely
    if ident_type == TypeDataCollection.DNI:
        match = customers.loc[customers['DNI'] == identifier]
    elif ident_type == TypeDataCollection.CUIL:
        match = customers.loc[customers['CUIL'] == identifier]
    else:
        raise ValueError(f"Invalid identifier type: {ident_type}")

    # ✅ Ensure at least one match is found before accessing index[0]
    return match.index.values[0] if not match.empty else None

def _get_credits_by_identifier(ident_type, identifier, id_supplier):
    """
    Fetch credits based on the provided identifier type and value.

    Args:
        ident_type (TypeDataCollection): Type of identifier (DNI, CUIL, ID_Op, ID_External).
        identifier (int): The identifier value to look up.
        id_supplier (int, optional): Supplier ID for filtering.

    Returns:
        tuple: (List of credit IDs, DataFrame of all credits).

    Raises:
        IdentifierError: If no credits are found for the given identifier.
    """
    
    # ✅ Load credit data
    credits = pd.read_sql('credits', engine, index_col='ID')
    
    # ✅ Handle customer-based identifiers (DNI, CUIL)
    if ident_type in {TypeDataCollection.DNI, TypeDataCollection.CUIL}:
        id_cst = _get_customer_id(ident_type, identifier)

        if id_supplier:
            # ✅ Load business plan and filter by supplier
            bp = pd.read_sql('business_plan', engine, index_col='ID')
            bps = bp.loc[bp['ID_Company'] == id_supplier].index.values
            id_credits = credits.loc[(credits['ID_Client'] == id_cst) & (credits['ID_BP'].isin(bps))].index.values
        else:
            id_credits = credits.loc[credits['ID_Client'] == id_cst].index.values
        
        if id_credits.size == 0:
            raise IdentifierError(ident_type, identifier)

    # ✅ Handle direct operation ID lookup
    elif ident_type == TypeDataCollection.ID_Op:
        id_credits = [identifier] if identifier in credits.index else []

    # ✅ Handle external ID lookup
    else:
        id_credits = credits.loc[credits['ID_External'] == identifier].index.values

    return list(id_credits), credits  # ✅ Ensure id_credits is always a list

def _prepare_balance(id_credits):
    """
    Prepare and process the balance data for the given credits.

    Args:
        id_credits (list): List of credit IDs to filter.

    Returns:
        pd.DataFrame: Processed balance DataFrame, sorted and with accumulated totals.
    """
    # ✅ Retrieve credit balances
    balance = credits_balance()

    # ✅ Filter balance data based on credit IDs
    if not id_credits:  # Return empty DataFrame if no credits are provided
        return balance.iloc[0:0].copy()

    balance = balance.loc[balance['ID_Op'].isin(id_credits)].copy()

    # ✅ Ensure balance is not empty after filtering
    if balance.empty:
        return balance  # Return empty DataFrame if no matching records

    # ✅ Sort balance data
    balance.sort_values(by=['D_Due', 'ID_Op', 'Nro_Inst'], inplace=True)

    # ✅ Reset index for proper iteration and calculations
    balance.reset_index(inplace=True, drop=False)

    # ✅ Efficient cumulative sum calculation (avoids slow list comprehension)
    balance['Accumulated'] = balance['Total'].cumsum()

    # ✅ Restore index for consistency
    balance.set_index('ID', inplace=True)

    return balance.copy()

def _process_regular_collections(balance, amount, date):
    """
    Process regular collection installments.

    Args:
        balance (pd.DataFrame): DataFrame containing installment balances.
        amount (float): The available amount for collection.
        date (pd.Timestamp): Date of collection.

    Returns:
        pd.DataFrame: Processed collection DataFrame.
    """
    # ✅ Initialize an empty DataFrame with predefined columns
    collection_columns = ['ID_Inst', 'D_Emission', 'Type_Collection', 'Capital', 'Interest', 'IVA', 'Total']
    collection = pd.DataFrame(columns=collection_columns)

    # ✅ Filter balance data to include only installments within available amount
    eligible_balance = balance.loc[balance['Accumulated'] <= amount].copy()

    # ✅ Return empty DataFrame if no eligible installments
    if eligible_balance.empty:
        return collection

    # ✅ Construct the collection DataFrame efficiently
    collection = eligible_balance[['Capital', 'Interest', 'IVA', 'Total']].copy()
    collection['ID_Inst'] = eligible_balance.index
    collection['D_Emission'] = date
    collection['Type_Collection'] = 'COMUN'

    # ✅ Reset index for proper structure
    collection.reset_index(drop=True, inplace=True)

    return collection

def _process_early_payment(complete_balance, amount, collection, date, early: bool = False):
    """Process an early payment for the given balance."""
    balance = complete_balance.loc[~complete_balance.index.isin(collection['ID_Inst'].values)].iloc[0].copy()
    if not early:
        interest = amount / 1.21 if balance['Interest'] + balance['IVA'] >= amount else balance['Interest']
        iva = amount / 1.21 * 0.21 if balance['Interest'] + balance['IVA'] >= amount else balance['IVA']
        capital = amount - (interest + iva) if balance['Interest'] + balance['IVA'] < amount else 0
        total = capital + interest + iva
    else:
        interest = 0.0
        iva = 0.0
        capital = amount if balance['Capital'] > amount else balance['Capital']
        total = capital

        if balance['Capital'] - capital < 0.1:
            n = collection.index.max() + 1 if not collection.empty else 0
            collection.loc[n] = {
                'ID_Inst': balance.name,
                'D_Emission': date,
                'Type_Collection': 'BON. CAN. ANT.',
                'Capital': 0.0,
                'Interest': balance['Interest'],
                'IVA': balance['IVA'],
                'Total': balance['Interest'] + balance['IVA']
            }
    
    n = collection.index.max() + 1 if not collection.empty else 0
    collection.loc[n] = {
        'ID_Inst': balance.name,
        'D_Emission': date,
        'Type_Collection': 'ANTICIPADA' if balance['Total'] - total > 0.1 else 'COMÚN',
        'Capital': capital if capital > 0 else 0.0,
        'Interest': interest if interest > 0 else 0.0,
        'IVA': iva if iva > 0.0 else 0.0,
        'Total': total if total > 0 else 0.0
    }
    amount -= total

    return collection

def _process_penalty(amount, credits, date, id_credits, collection, save, early: bool = False):
    """Process a penalty if there is a remaining amount."""
    cr_penalty = credits.iloc[0:0].copy()
    inst = pd.read_sql('installments', engine, index_col='ID')
    id_penalty = inst.index.max() + 1
    inst = inst.iloc[0:0].copy()

    if amount <= 0.009:
        amount = 0.0
        return collection, cr_penalty, inst

    penalty = amount / 1.21
    iva = penalty * 0.21

    id_cst = credits.loc[credits.index.isin(id_credits), 'ID_Client'].values[0]
    
    cr_penalty.loc[0] = {
        'ID_Client': id_cst,
        'Date_Settlement': date,
        'ID_BP': 1,
        'Cap_Requested': 0.0,
        'Cap_Grant': 0.0,
        'N_Inst': 1,
        'TEM_W_IVA': 0.0,
        'V_Inst': amount,
        'D_F_Due': date,
    }

    inst.loc[0] = {
        'ID_Op': pd.read_sql('credits', engine, index_col='ID').index.max() + 1,
        'Nro_Inst': 1,
        'D_Due': date,
        'Capital': 0.0,
        'Interest': penalty,
        'IVA': iva,
        'Total': amount,
        'ID_Owner': 1
    }

    n = collection.index.max() + 1 if not collection.empty else 0
    collection.loc[n] = {
        'ID_Inst': id_penalty,
        'D_Emission': date,
        'Type_Collection': 'PENALTY',
        'Capital': 0.0,
        'Interest': penalty,
        'IVA': iva,
        'Total': amount
    }

    if save:
        cr_penalty.to_sql('credits', engine, index=False, if_exists='append')
        inst.to_sql('installments', engine, index=False, if_exists='append')
        
    return collection, cr_penalty, inst

def _process_remaining_amount(amount, collection, balance, credits, date, id_credits, save, early: bool = False):
    """
    Handle any remaining amount, including early payments or penalties.

    Args:
        amount (float): Remaining amount after initial collections.
        collection (pd.DataFrame): DataFrame of processed collections.
        balance (pd.DataFrame): DataFrame containing credit balances.
        credits (pd.DataFrame): DataFrame of credit information.
        date (pd.Timestamp): Transaction date.
        id_credits (list): List of credit IDs.
        save (bool): Whether to save the results to the database.
        early (bool, optional): Flag indicating whether it's an early payment. Default is False.

    Returns:
        tuple: (Updated collection DataFrame, Penalty credits DataFrame, Installments DataFrame)
    """
    # ✅ Deduct collected total from amount
    if not early:
        amount -= collection['Total'].sum()

    # ✅ Process early payment if balance still exists after previous collections
    remaining_balance = balance.query("index not in @collection['ID_Inst'].values")
    
    if amount > 0.0 and not remaining_balance.empty:
        collection = _process_early_payment(balance, amount, collection, date, early)
        last_row = collection.iloc[-1] if not collection.empty else None
        if last_row is not None:
            amount -= last_row['Total']

    # ✅ Process penalties if amount remains
    if amount > 0.0:
        collection, cr_penalty, inst = _process_penalty(amount, credits, date, id_credits, collection, save, early)
    else:
        cr_penalty, inst = pd.DataFrame(), pd.DataFrame()

    return collection, cr_penalty, inst

def _solve_rounding(collection, balance, date, early: bool = False, id_credits=None):
    """
    Adjusts collection records to account for small rounding differences in financial calculations.

    Args:
        collection (pd.DataFrame): DataFrame containing collected payments.
        balance (pd.DataFrame): DataFrame containing installment balances.
        date (pd.Timestamp): Date of the rounding adjustment.
        early (bool, optional): Whether this is for early payments. Defaults to False.
        id_credits (list, optional): List of credit IDs, required if `early` is True.

    Returns:
        pd.DataFrame: Updated collection DataFrame with rounding adjustments applied.
    """

    # ✅ Recalculate balance if early payment processing is required
    if early and id_credits:
        balance = _prepare_balance(id_credits)

    # ✅ Ensure collection contains necessary columns before subtracting
    if not collection.empty:
        balance[['Capital', 'Interest', 'IVA', 'Total']] -= (
            collection.groupby('ID_Inst')[['Capital', 'Interest', 'IVA', 'Total']].sum()
        )

    # ✅ Identify rows where remaining balance is small but non-zero
    rounding = balance.query("Total != 0.0 and Total < 0.1").copy()

    # ✅ Ensure collection index is numeric to avoid NaN issues
    new_index = collection.index.max()
    new_index = 0 if pd.isna(new_index) else new_index + 1

    # ✅ Add rounding adjustments
    if not rounding.empty:
        adjustments = pd.DataFrame({
            'ID_Inst': rounding.index,
            'D_Emission': date,
            'Type_Collection': 'REDONDEO',
            'Capital': rounding['Capital'],
            'Interest': rounding['Interest'],
            'IVA': rounding['IVA'],
            'Total': rounding['Total']
        })

        adjustments.index = range(new_index, new_index + len(adjustments))
        collection = pd.concat([collection, adjustments])

    return collection

def filter_credits_with_resources(id_credits, ident_type, identifier):
    """
    Filters credits based on their 'Resource' status in the portfolio_purchases table.

    Args:
        id_credits (list): List of credit IDs to filter.
        ident_type (TypeDataCollection): Identifier type for error handling.
        identifier (int or str): Specific identifier for error handling.

    Returns:
        list: Filtered list of credit IDs where 'Resource' is not 1.

    Raises:
        ResourceError: If all credits are filtered out but the original list was non-empty.
    """
    # ✅ Ensure id_credits is not empty before proceeding
    if not id_credits:
        return []

    # ✅ Load relevant tables from the database
    credits = pd.read_sql('credits', engine, index_col='ID')
    pp = pd.read_sql('portfolio_purchases', engine, index_col='ID')

    # ✅ Ensure all requested credit IDs exist in the 'credits' table
    valid_id_credits = [cid for cid in id_credits if cid in credits.index]

    if not valid_id_credits:
        return []  # Return empty list if none of the given credit IDs exist

    # ✅ Map credits to their associated portfolio purchase IDs
    id_pp_mapping = credits.loc[valid_id_credits, 'ID_Purch']

    # ✅ Extract 'Resource' values, handling missing data safely
    resources = pp.loc[id_pp_mapping.dropna(), 'Resource'].fillna(0)

    # ✅ Filter out credits where 'Resource' is 1
    filtered_credits = [cid for cid, resource in zip(valid_id_credits, resources) if resource != 1]

    # ✅ Raise error if all credits were removed and we started with valid ones
    if not filtered_credits and valid_id_credits:
        raise ResourceError(ident_type, identifier)

    return filtered_credits

def charging(
    ident_type: TypeDataCollection,
    identifier: int,
    amount: float,
    id_supplier: int,
    date: pd.Timestamp = pd.Timestamp.now().strftime('%Y/%m/%d'),
    save: bool = False
):
    """
    Processes a collection operation based on a given identifier and type.

    This function manages the collection of payments for credits by validating the identifier,
    retrieving the appropriate credit data, and processing regular collections, early payments,
    or penalties. The function can save the results to the database or return the processed data.

    Args:
        ident_type (TypeDataCollection): Type of identifier (e.g., DNI, CUIL, ID_Op, ID_External).
        identifier (int): Identifier value for the collection.
        amount (float): Amount to be collected.
        id_supplier (int, optional): ID of the supplier for business plans.
        date (pd.Timestamp, optional): Collection date. Defaults to the current date.
        save (bool, optional): Whether to save the results to the database. Default is False.

    Returns:
        tuple: If `save` is False, returns a tuple:
            (collection DataFrame, penalty credits DataFrame, installments DataFrame).
    """

    # ✅ Validate the identifier type and value
    TypeDataCollection.validate(identifier, ident_type)

    # ✅ Return an empty DataFrame if the collection amount is zero or negative
    if amount <= 0.0:
        return pd.DataFrame(columns=['ID_Inst', 'D_Emission', 'Type_Collection', 'Capital', 'Interest', 'IVA', 'Total'])

    # ✅ Retrieve and filter credits based on identifier type
    id_credits, credits = _get_credits_by_identifier(ident_type, identifier, id_supplier)
    id_credits = filter_credits_with_resources(id_credits, ident_type, identifier)

    # ✅ Retrieve and prepare balance data
    balance = _prepare_balance(id_credits)

    # ✅ Process regular collections based on balance and amount
    collection = _process_regular_collections(balance, amount, date)

    # ✅ Adjust remaining amount and process early payments or penalties if necessary
    collection, cr_penalty, inst = _process_remaining_amount(amount, collection, balance, credits, date, id_credits, save=False)

    # ✅ Remove zero-total rows and round numerical columns to two decimal places
    if not collection.empty:
        collection = collection[collection['Total'] != 0].copy()
        collection[['Capital', 'Interest', 'IVA', 'Total']] = collection[['Capital', 'Interest', 'IVA', 'Total']].round(2)

    # ✅ Solve rounding discrepancies
    collection = _solve_rounding(collection, balance, date)

    # ✅ Save results if requested
    if save:
        try:
            if not (cr_penalty.empty or inst.empty):
                cr_penalty.to_sql('credits', engine, index=False, if_exists='append')
                inst.to_sql('installments', engine, index=False, if_exists='append')
            collection.to_sql('collection', engine, index=False, if_exists='append')
        except (alIE, myIE) as e:
            print(f"Database Integrity Error: {e}\nID: {identifier}\nPenalty: {cr_penalty}\nInstallment: {inst}\nCollections: {collection}")

    return collection, cr_penalty, inst


def _process_early_settlement(collection, early, date, collection_type, amount):
    """
    Processes early settlement collection installments.

    Args:
        collection (pd.DataFrame): Existing collection DataFrame.
        early (pd.DataFrame): DataFrame of installments eligible for early settlement.
        date (pd.Timestamp): Collection date.
        collection_type (str): Type of collection (e.g., 'CAN. ANT.', 'BON. CAN. ANT.').
        amount (float): Total amount available for collection.

    Returns:
        pd.DataFrame: Updated collection DataFrame with early settlement records.
    """

    # ✅ Return collection unchanged if no early installments exist
    if early.empty:
        return collection

    # ✅ Ensure collection index is numeric to prevent NaN issues
    new_index = collection.index.max()
    new_index = 0 if pd.isna(new_index) else new_index + 1

    # ✅ Create new DataFrame for early settlements
    early_settlements = early[['Capital', 'Interest', 'IVA', 'Total']].copy()
    early_settlements['ID_Inst'] = early.index
    early_settlements['D_Emission'] = date
    early_settlements['Type_Collection'] = collection_type

    # ✅ Assign new index for proper alignment
    early_settlements.index = range(new_index, new_index + len(early_settlements))

    # ✅ Concatenate the new early settlements to the collection
    if early_settlements.empty:
        early_settlements = pd.DataFrame(columns=collection.columns)  # ✅ Preserve expected column types
    collection = pd.concat([collection, early_settlements], ignore_index=True)

    return collection


def collection_w_early_cancel(
        ident_type: TypeDataCollection,
        identifier: int,
        amount: float,
        id_supplier: int,
        date: pd.Timestamp = pd.Timestamp.now(),
        save: bool = False):
    """
    Processes a collection operation with early cancellation handling.

    This function manages the collection process, differentiating between regular 
    collections and early cancellations. It adjusts balances for early settlements, 
    applies bonuses for early cancellation, and optionally saves the results to the database.

    Args:
        ident_type (TypeDataCollection): The type of identifier (e.g., DNI, CUIL, ID_Op, ID_External).
        identifier (int): The specific identifier value for the collection.
        amount (float): The total amount to be collected.
        id_supplier (int): ID of the supplier for business plans (optional).
        date (pd.Timestamp): The date of the collection (defaults to the current date).
        save (bool): Whether to save the collection records to the database (default is False).

    Returns:
        tuple: (collection DataFrame, penalty DataFrame, installment DataFrame)
    """

    # ✅ Validate identifier
    TypeDataCollection.validate(identifier, ident_type)

    # ✅ Return empty DataFrame if amount is not positive
    if amount <= 0.0:
        return pd.DataFrame(columns=['ID_Inst', 'D_Emission', 'Type_Collection', 'Capital', 'Interest', 'IVA', 'Total'])

    # ✅ Retrieve and filter credits
    id_credits, credits = _get_credits_by_identifier(ident_type, identifier, id_supplier)
    id_credits = filter_credits_with_resources(id_credits, ident_type, identifier)

    # ✅ Prepare balance
    balance = _prepare_balance(id_credits)

    # ✅ Separate due and future installments
    common = balance.query("D_Due <= @date").copy()  # Due installments
    early = balance.query("D_Due > @date").copy()    # Future installments
    esd = early.copy()  # For bonus calculations

    # ✅ Process regular collections
    collection = _process_regular_collections(common, amount, date)
    amount -= collection['Total'].sum()

    # ✅ Handle early settlements
    early[['Interest', 'IVA']] = 0.0
    early['Total'] = early['Capital']
    early.sort_values(by=['D_Due', 'ID_Op', 'Nro_Inst'], inplace=True)
    early['Accumulated'] = early['Total'].cumsum()
    early = early.query("Accumulated <= @amount")
    esd = esd.loc[esd.index.isin(early.index)]

    collection = _process_early_settlement(collection, early, date, 'CAN. ANT.', amount)
    amount -= collection.query("Type_Collection == 'CAN. ANT.'")['Total'].sum()

    # ✅ Apply bonuses for early cancellations
    esd[['Capital', 'Total']] = 0.0
    esd['Total'] = esd['Interest'] + esd['IVA']
    collection = _process_early_settlement(collection, esd, date, 'BON. CAN. ANT.', amount)

    # ✅ Adjust balance based on collections
    collected_totals = collection.groupby('ID_Inst')[['Capital', 'Interest', 'IVA', 'Total']].sum()
    balance.loc[balance.index.isin(collected_totals.index), ['Capital', 'Interest', 'IVA', 'Total']] -= collected_totals

    # ✅ Process remaining amount for penalties or adjustments
    collection, penalties, installments = _process_remaining_amount(amount, collection, balance, credits, date, id_credits, False, True)
    amount -= collection.query("Type_Collection == 'PENALTY'")['Total'].sum()

    # ✅ Final cleanup and rounding
    collection = collection.loc[collection['Total'] != 0].copy()
    collection[['Capital', 'Interest', 'IVA', 'Total']] = collection[['Capital', 'Interest', 'IVA', 'Total']].round(2)

    # ✅ Solve rounding issues
    collection = _solve_rounding(collection, balance, date, True, id_credits)

    # ✅ Save results if requested
    if save:
        try:
            if not penalties.empty:
                penalties.to_sql('credits', engine, index=False, if_exists='append')
            if not installments.empty:
                installments.to_sql('installments', engine, index=False, if_exists='append')
            collection.to_sql('collection', engine, index=False, if_exists='append')
        except (alIE, myIE) as e:
            print(f"⚠️ Database Error: {e}")

    return collection, penalties, installments



def reverse(
        ident_type: TypeDataCollection,
        identifier: int,
        amount: float,
        id_supplier: int,
        date: pd.Timestamp = pd.Timestamp.now(),
        save: bool = False):
    """
    Processes the reversal of collections for a given identifier.

    This function reverses collection operations for a specific identifier, considering 
    the balance of the associated credits and installments. It adjusts the collection 
    amounts accordingly and optionally saves the reversed transactions to the database.

    Args:
        ident_type (TypeDataCollection): The type of identifier (e.g., DNI, CUIL, ID_Op, ID_External).
        identifier (int): The specific identifier value for the reversal operation.
        amount (float): The total amount to reverse.
        id_supplier (int): ID of the supplier for business plans (optional).
        date (pd.Timestamp): The date of the reversal operation (defaults to the current date).
        save (bool): Whether to save the reversed collection records to the database (default is False).

    Returns:
        pd.DataFrame: A DataFrame containing the reversed collection records.
    """
    # Validate the identifier type and value
    TypeDataCollection.validate(identifier, ident_type)

    # Retrieve credits based on the identifier type
    id_credits, credits = _get_credits_by_identifier(ident_type, identifier, id_supplier)

    # Retrieve and prepare the balance for the credits
    balance = _prepare_balance(id_credits)

    # Retrieve the installment data for the credits and sort for reversal processing
    installments = pd.read_sql('installments', engine, index_col='ID')
    installments = installments.loc[
        installments.index.isin(balance.index)
    ].sort_values(by=['D_Due', 'ID_Op', 'Nro_Inst'], ascending=False)

    # Initialize an empty DataFrame to store the reversed collection records
    collection = pd.read_sql('collection', engine, index_col='ID').iloc[0:0]

    # Iterate through the installments to process reversals
    for n, i in enumerate(installments.index):
        # Case 1: No adjustment needed if the balance matches the installment
        if balance.loc[i, 'Total'] == installments.loc[i, 'Total']:
            pass

        # Case 2: Partial reversal when there is an outstanding balance
        elif (balance.loc[i, 'Total'] > 0.0) & (balance.loc[i, 'Total'] < installments.loc[i, 'Total']) & \
             (amount >= installments.loc[i, 'Total'] - balance.loc[i, 'Total'].sum()):
            collection.loc[n] = {
                'ID_Inst': i,
                'D_Emission': date,
                'Type_Collection': 'REVERSA',
                'Capital': -(installments.loc[i, 'Capital'] - balance.loc[i, 'Capital']),
                'Interest': -(installments.loc[i, 'Interest'] - balance.loc[i, 'Interest']),
                'IVA': -(installments.loc[i, 'IVA'] - balance.loc[i, 'IVA']),
                'Total': -(installments.loc[i, 'Total'] - balance.loc[i, 'Total'])
            }
            amount -= (installments.loc[i, 'Total'] - balance.loc[i, 'Total'])

        # Case 3: Full reversal of an unpaid installment
        elif (balance.loc[i, 'Total'] == 0.0) & (amount - installments.loc[i, 'Total'] >= -0.1):
            collection.loc[n] = {
                'ID_Inst': i,
                'D_Emission': date,
                'Type_Collection': 'REVERSA',
                'Capital': -installments.loc[i, 'Capital'],
                'Interest': -installments.loc[i, 'Interest'],
                'IVA': -installments.loc[i, 'IVA'],
                'Total': -installments.loc[i, 'Total']
            }
            amount -= installments.loc[i, 'Total']

        # Case 4: Partial installment reversal due to insufficient amount
        elif amount < installments.loc[i, 'Total'] - balance.loc[i, 'Total']:
            capital = installments.loc[i, 'Capital'] - balance.loc[i, 'Capital']
            capital = capital if amount >= capital else amount
            amount -= capital

            interest = (installments.loc[i, 'Interest'] - balance.loc[i, 'Interest']) / 1.21
            iva = interest * 0.21
            interest = interest if amount >= interest else amount / 1.21
            amount -= interest

            iva = iva if amount >= iva else interest * 0.21

            collection.loc[n] = {
                'ID_Inst': i,
                'D_Emission': date,
                'Type_Collection': 'REVERSA',
                'Capital': -capital,
                'Interest': -interest,
                'IVA': -iva,
                'Total': -(capital + interest + iva)
            }
            amount -= capital + interest + iva

        # Case 5: Stop processing if the amount is nearly zero
        elif abs(amount) < 0.1:
            pass

        # Case 6: Raise an error for unhandled scenarios
        else:
            raise ValueError(f'amount: $ {amount:,.2f}\n{balance.loc[i]}')

    # Save the reversed collection records if required
    if save:
        collection.to_sql('collection', engine, index=False, if_exists='append')

    return collection


def read_collection_file(
        path: str,
        type_data: TypeDataCollection) -> pd.DataFrame:
    """
    Reads a collection file (CSV or Excel) and processes the data based on the provided type.

    Args:
        path (str): The file path of the collection file to read.
        type_data (TypeDataCollection): The type of data to read (ID_Op, ID_Ext, DNI, CUIL).

    Returns:
        pd.DataFrame: A DataFrame grouped by the identifier with the summed 'Amount' values.

    Raises:
        FileNotFoundError: If the provided file path does not exist.
        ValueError: If the file type is unsupported or if `type_data` is invalid.
    """

    # ✅ Ensure the file exists before proceeding
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ The file '{path}' does not exist. Please check the path.")

    # ✅ Mapping of TypeDataCollection values to column headers
    index_mapping = {
        TypeDataCollection.ID_Op: 'ID_Op',
        TypeDataCollection.ID_Ext: 'ID_Ext',
        TypeDataCollection.DNI: 'DNI',
        TypeDataCollection.CUIL: 'CUIL'
    }

    # ✅ Retrieve the column name based on `type_data`
    index = index_mapping.get(type_data)
    if index is None:
        raise ValueError(f"❌ Unsupported `type_data`: {type_data}. Expected one of {list(index_mapping.keys())}.")

    # ✅ Determine file extension
    _, extension = os.path.splitext(path)
    extension = extension.lower()

    # ✅ Read the file based on its extension
    if extension == '.csv':
        df = pd.read_csv(path, sep=None, engine='python', header=None)  # Auto-detect delimiter
    elif extension == '.xlsx':
        df = pd.read_excel(path, header=None)
    else:
        raise ValueError(f"❌ Unsupported file type: '{extension}'. Only CSV and Excel files are allowed.")

    # ✅ Ensure DataFrame is not empty
    if df.empty:
        return pd.DataFrame(columns=[index, 'Amount'])

    # ✅ Assign column names dynamically
    df.columns = [index, 'Amount']

    # ✅ Ensure 'Amount' is numeric, coercing errors to NaN
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

    # ✅ Remove rows with NaN values in the 'Amount' column
    df = df.dropna(subset=['Amount'])

    # ✅ Convert identifier column to string to prevent grouping issues
    df[index] = df[index].astype(str)

    # ✅ Group by identifier and sum amounts
    df = df.groupby(index, as_index=True)['Amount'].sum().reset_index()

    return df


def massive_collection(
        path: str,
        id_supplier: int,
        type_data: TypeDataCollection,
        date: pd.Timestamp = pd.Timestamp.now(),
        save: bool = False):
    
    '''
    Description:
        Processes a file containing financial records, groups the data
        by a specific type of identifier, and generates a summarized 
        collection report by interacting with the `charging` function.

    Parameters:
        path (str): 
            The file path to the input data file (.csv or .xlsx).
        id_supplier (int): 
            The identifier of the supplier.
        type_data (TypeDataCollection): 
            The type of data used to group records. It could be one of:
            - TypeDataCollection.ID_Op
            - TypeDataCollection.ID_Ext
            - TypeDataCollection.DNI
            - TypeDataCollection.CUIL
        date (pd.Timestamp, optional): 
            The date for the collection process (default is current date).
        save (bool, optional): 
            Flag to determine whether to save results (default is False).

    Returns:
        tuple:
            - df (pd.DataFrame): The grouped and summarized input data.
            - collection (pd.DataFrame): The collection report.

    Raises:
        FileNotFoundError: If the file does not exist at the specified path.
        ValueError: If the file type is unsupported or type_data is invalid.
    '''

    df = read_collection_file(path, type_data)

    # Initialize empty lists to collect the DataFrame results
    collection_list = []
    penalty_list = []
    inst_list = []

    # Initialize error DataFrame
    error = pd.DataFrame(columns=['Amount'])
    error.index.name = df.index.name

    # Iterate over each group in the DataFrame
    for i in df.index:
        print(f"Processing ID: {i}")  # Debugging output to track progress

        # Call the charging function to process each group
        try:
            result = charging(type_data, i, df.loc[i, 'Amount'], id_supplier, date)
            new_penalty = result[1]
            new_inst    = result[2]
            collection  = result[0]
                    
            # Append valid results to the corresponding lists
            if collection is not None and not collection.empty:
                collection_list.append(collection)

            if new_penalty is not None and not new_penalty.empty:
                penalty_list.append(new_penalty)

            if new_inst is not None and not new_inst.empty:
                inst_list.append(new_inst)

        except IdentifierError or ResourceError:
            # In case of an error, log the Amount in the error DataFrame
            error.loc[i] = {'Amount': df.loc[i, 'Amount']}
            continue

    # Convert the lists into DataFrames after the loop
    collection = pd.concat(collection_list, ignore_index=True) if collection_list else pd.DataFrame(columns=['ID_Inst', 'D_Emission', 'Type_Collection', 'Capital', 'Interest', 'IVA', 'Total'])
    penalty = pd.concat(penalty_list, ignore_index=True) if penalty_list else pd.DataFrame()
    inst = pd.concat(inst_list, ignore_index=True) if inst_list else pd.DataFrame()
    if not inst.empty:
        new_penalty = collection.loc[collection['Type_Collection'] == 'PENALTY', 'ID_Inst'].copy()
        collection.loc[collection['Type_Collection'] == 'PENALTY', 'ID_Inst'] = [new_penalty.max() + i for i in range(len(new_penalty))]
        inst['ID_Op'] = [pd.read_sql('credits', engine, index_col='ID').index.max() + i + 1 for i in range(len(inst))]

    # Set index names after creation
    collection.index.name = 'ID'
    penalty.index.name = 'ID'
    inst.index.name = 'ID'

    if save:
        try:
            if (penalty.empty) and (inst.empty):
                collection.to_sql('collection', engine, index=False, if_exists='append')
            else:
                penalty.to_sql('credits', engine, index=False, if_exists='append')
                inst.to_sql('installments', engine, index=False, if_exists='append')
                collection.to_sql('collection', engine, index=False, if_exists='append')

        except IntegrityError:
            print(f"IntegrityError.\n Check the error Dataframe.")
            return df, collection, error, penalty, inst


    # Return the grouped input data and the final collection report
    return df, collection, error, penalty, inst


def massive_early_collection(
        path: str,
        id_supplier: int,
        type_data: TypeDataCollection,
        date: pd.Timestamp = pd.Timestamp.now(),
        save: bool = False):
    """
    Processes a file for massive early collections and updates related data structures.

    Args:
        path (str): Path to the input file containing collection data.
        id_supplier (int): ID of the supplier associated with the collections.
        type_data (TypeDataCollection): Type of data used to process the collections (e.g., ID_Op, ID_Ext, DNI, CUIL).
        date (pd.Timestamp, optional): The date of the collection. Defaults to the current date and time.
        save (bool, optional): If True, saves the resulting DataFrames to the database. Defaults to False.

    Returns:
        tuple: A tuple containing:
            - collections (pd.DataFrame): Processed collections data.
            - penalties (pd.DataFrame): Penalties data resulting from early cancellations.
            - installments (pd.DataFrame): Installments data created during the process.
            - error (pd.DataFrame): DataFrame with errors for unmatched or problematic records.

    Raises:
        FileNotFoundError: If the specified input file is not found.
        ValueError: If data processing encounters an invalid condition.
        IntegrityError: If a database integrity constraint is violated during save.
    """

    # ✅ Validate file existence
    if not os.path.exists(path):
        raise FileNotFoundError(f"File '{path}' not found. Please check the path and try again.")

    # ✅ Read and process input file
    df = read_collection_file(path, type_data)

    # ✅ Print formatted DataFrame for debugging
    print(df.map('${:,.2f}'.format))

    # ✅ Initialize empty lists for collected data
    collection_list, penalty_list, inst_list = [], [], []

    # ✅ Initialize error DataFrame
    error = pd.DataFrame(columns=['Amount'])
    error.index.name = df.index.name

    # ✅ Load `credits` once to avoid redundant queries in loop
    credits = pd.read_sql('credits', engine, index_col='ID')

    # ✅ Process early collections for each record
    for i in df.index:
        try:
            collections, penalties, installments = collection_w_early_cancel(
                type_data, i, df.at[i, 'Amount'], id_supplier, date
            )

            # Append results if they contain data
            if not collections.empty:
                collection_list.append(collections)
            if not penalties.empty:
                penalty_list.append(penalties)
            if not installments.empty:
                inst_list.append(installments)

        except IdentifierError:
            error.at[i, 'Amount'] = df.at[i, 'Amount']

    # ✅ Combine collected DataFrames
    collections = pd.concat(collection_list, ignore_index=True) if collection_list else pd.DataFrame()
    penalties = pd.concat(penalty_list, ignore_index=True) if penalty_list else pd.DataFrame()
    installments = pd.concat(inst_list, ignore_index=True) if inst_list else pd.DataFrame()

    # ✅ Ensure valid IDs for installments and penalties
    if not installments.empty:
        new_penalty_ids = collections.query("Type_Collection == 'PENALTY'")['ID_Inst'].copy()
        if not new_penalty_ids.empty:
            collections.loc[new_penalty_ids.index, 'ID_Inst'] = range(
                new_penalty_ids.max() + 1, new_penalty_ids.max() + 1 + len(new_penalty_ids)
            )

        # ✅ Assign new credit IDs to installments
        last_credit_id = credits.index.max() if not credits.empty else 0
        installments['ID_Op'] = range(last_credit_id + 1, last_credit_id + 1 + len(installments))

    # ✅ Set index names for consistency
    collections.index.name, penalties.index.name, installments.index.name = 'ID', 'ID', 'ID'

    # ✅ Round numerical columns for precision
    for col in ['Capital', 'Interest', 'IVA', 'Total']:
        if not collections.empty:
            collections[col] = collections[col].round(2)
        if not installments.empty:
            installments[col] = installments[col].round(2)

    # ✅ Remove zero-value rows from collections
    collections = collections.query("Capital != 0 or Interest != 0 or IVA != 0 or Total != 0")

    # ✅ Save data to the database if required
    if save:
        try:
            if penalties.empty and installments.empty:
                collections.to_sql('collection', engine, index=False, if_exists='append')
            else:
                penalties.to_sql('credits', engine, index=False, if_exists='append')
                installments.to_sql('installments', engine, index=False, if_exists='append')
                collections.to_sql('collection', engine, index=False, if_exists='append')

        except IntegrityError as e:
            print(f"⚠️ IntegrityError: {e}\nCheck the error DataFrame.")
            return df, collections, error, penalties, installments

    # ✅ Return processed DataFrames
    return collections, penalties, installments, error


def calculate_accumulated_balance(id_supplier: int, date: pd.Timestamp) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calculates the accumulated balance of credits with resource type for a supplier up to a given date.

    Parameters:
        id_supplier (int): The ID of the supplier company.
        date (pd.Timestamp): The cutoff date for filtering credit balances.

    Returns:
        tuple: 
            - pd.DataFrame: Filtered balance DataFrame containing credit balances.
            - pd.DataFrame: DataFrame with daily total balance and accumulated balance up to the given date.
    """

    # ✅ Retrieve supplier's advance amount safely
    companies = pd.read_sql('companies', engine, index_col='ID')

    if id_supplier not in companies.index:
        raise ValueError(f"Supplier ID {id_supplier} not found in database.")

    advance = companies.at[id_supplier, 'Advance']

    # ✅ Fetch credit balance and related tables
    balance = credits_balance()  # Assumes this function fetches balance data
    credits = pd.read_sql('credits', engine, index_col='ID')
    pp = pd.read_sql('portfolio_purchases', engine, index_col='ID')

    # ✅ Filter credits with resource type and due date before the cutoff date
    resource_credits = credits.query("ID_Purch in @pp.query('Resource == 1').index").index

    if resource_credits.empty:
        return balance.iloc[0:0].copy(), pd.DataFrame(columns=['Total', 'Accumulated'])

    # ✅ Apply filtering to balance DataFrame
    balance = balance.query("ID_Op in @resource_credits and D_Due <= @date").copy()

    if balance.empty:
        return balance, pd.DataFrame(columns=['Total', 'Accumulated'])

    # ✅ Group by due date and sum total amounts
    balance_acum = balance.groupby('D_Due', as_index=True)['Total'].sum().reset_index()

    # ✅ Ensure balance_acum contains only non-zero totals
    balance_acum = balance_acum.query("Total != 0.0").copy()

    # ✅ Calculate accumulated balance
    balance_acum['Accumulated'] = balance_acum['Total'].cumsum()

    return balance, balance_acum


def resource_collection(
    id_supplier: int,
    amount: float,
    date: pd.Timestamp = pd.Timestamp.now().strftime('%Y/%m/%d'),
    save: bool = False) -> pd.DataFrame:
    """
    Manages the resource collection process for a supplier, updating the advance balance and recording collections.

    Parameters:
        id_supplier (int): The ID of the supplier company.
        amount (float): The amount available for collection.
        date (pd.Timestamp): The date of the collection (default is today).
        save (bool): Whether to save the updates and collection to the database.

    Returns:
        pd.DataFrame: A DataFrame with the details of the collections processed.
    """

    # ✅ Load supplier data
    companies = pd.read_sql(f"SELECT * FROM companies WHERE ID = {id_supplier}", engine, index_col="ID")

    # ✅ Verify supplier existence
    if companies.empty:
        raise ValueError(f"❌ Supplier ID {id_supplier} does not exist in the database.")

    supplier_name = companies.loc[id_supplier, "Social_Reason"]

    # ✅ Confirm action with the user
    confirmation = input(f"The customer is {supplier_name}. Shall we continue? (Yes/No) ").strip().lower()
    if confirmation != "yes":
        print("❌ Process cancelled.")
        return pd.DataFrame()

    # ✅ Adjust the amount using the supplier's advance balance
    amount += companies.loc[id_supplier, "Advance"]

    # ✅ Fetch accumulated balances
    balance, balance_acum = calculate_accumulated_balance(id_supplier, date)

    # ✅ Fetch system settings and determine cutoff
    setts = pd.read_sql("SELECT ID, Value FROM settings WHERE ID = 2", engine, index_col="ID")
    tolerance_value = float(setts.loc[2, "Value"])

    # ✅ Filter accumulated balances based on conditions
    filter_condition = (balance_acum["Accumulated"] <= amount) | (abs(balance_acum["Accumulated"] - amount) <= tolerance_value)
    amount -= balance_acum.loc[filter_condition, "Total"].sum()
    balance = balance.loc[balance['D_Due'].isin(balance_acum.loc[filter_condition].index)]

    # ✅ Initialize collections DataFrame
    collections = pd.DataFrame(columns=["ID_Inst", "D_Emission", "Type_Collection", "Capital", "Interest", "IVA", "Total"])

    # ✅ Process each balance entry
    for i, (index, row) in enumerate(balance.iterrows()):
        collections.loc[i] = {
            "ID_Inst": index,
            "D_Emission": date,
            "Type_Collection": "RECURSO",
            "Capital": row["Capital"],
            "Interest": row["Interest"],
            "IVA": row["IVA"],
            "Total": row["Total"]
        }

    # ✅ Save to database if required
    if save:
        try:
            # ✅ Update supplier's advance balance
            stmt = update(Company).where(Company.ID == id_supplier).values(Advance=amount)
            with Session(engine) as session:
                session.execute(stmt)
                session.commit()

            # ✅ Save collections to the database
            collections.to_sql("collection", engine, index=False, if_exists="append")

            print(f"✅ Resource collection for supplier {supplier_name} successfully recorded.")
        except Exception as e:
            print(f"❌ Error saving resource collection: {e}")

    return collections


def delete_collection_by_id(collection_id: int) -> None:
    """
    Deletes a row from the 'Collection' table based on the provided ID.

    Args:
        collection_id (int): The ID of the collection to be deleted.

    Raises:
        SQLAlchemyError: If any database-related error occurs during the deletion process.

    Returns:
        None
    """

    Session = sessionmaker(bind=engine)

    try:
        with Session() as session:
            # Attempt to retrieve the collection entry
            collection_entry = session.query(Collection).filter_by(id=collection_id).first()

            if collection_entry:
                # Delete and commit
                session.delete(collection_entry)
                session.commit()
                print(f"✅ Collection record with ID {collection_id} successfully deleted.")
            else:
                print(f"⚠️ No collection record found with ID {collection_id}.")
    
    except SQLAlchemyError as e:
        print(f"❌ Database error while deleting collection ID {collection_id}: {e}")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
