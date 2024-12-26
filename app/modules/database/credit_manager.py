import os
import pandas as pd
import numpy as np

# Import your module
from app.modules.database.connection import engine

from sqlalchemy.orm import sessionmaker
from sqlalchemy import update
import numpy_financial as npf
from dateutil.relativedelta import relativedelta

from enum import Enum
from sqlalchemy.exc import IntegrityError


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
        return f"For {self.type.name}: {self.data}."


def create_installments(id_credit: int, new_cr: pd.Series, save: bool = False):
    """
    Creates and stores installment records for a given credit.

    Parameters:
    id_credit (int): The unique identifier of the credit for which installments need to be created.

    Returns:
    pd.DataFrame: A DataFrame containing the installments for the specified credit.
    """

    # Fetch existing installment data from the 'installments' table, initializing an empty DataFrame if none exist.
    df = pd.read_sql('installments', engine, index_col='ID').iloc[0:0]

    # Retrieve the credit details for the given credit ID from the 'credits' table.
    cr = new_cr.copy()
    cr['D_F_Due'] = pd.Period(cr['D_F_Due'], freq='D')
    
    # Determine the starting installment ID based on existing data.
    id_inst = df.index.max() if not df.empty else 0

    # Generate installment details for each installment in the credit term.
    v_inst   = -npf.pmt(cr['TEM_W_IVA'], cr['N_Inst'], cr['Cap_Grant'])
    for i in range(int(cr['N_Inst'])):
        id_inst += 1
        
        # Calculate the interest portion of the installment using the IPMT formula.
        interest = -npf.ipmt(cr['TEM_W_IVA'], i, cr['N_Inst'], cr['Cap_Grant'])
        d_due = cr['D_F_Due'].asfreq('M') + i
        # Populate installment data for the current installment.
        df.loc[id_inst, ['ID_Op', 'Nro_Inst', 'D_Due', 'Capital', 'Interest', 'IVA', 'Total', 'ID_Owner']] = {
            'ID_Op': id_credit,  # Credit operation ID
            'Nro_Inst': i+1,  # Installment number
            'D_Due': pd.Timestamp(year=d_due.year, month=d_due.month, day=cr['D_F_Due'].day),  # Due date
            'Capital': v_inst - interest,  # Principal portion
            'Interest': interest / 1.21,  # Interest portion (excluding VAT)
            'IVA': interest / 1.21 * 0.21,  # VAT on interest
            'Total': v_inst,  # Total installment value
            'ID_Owner': 1  # Owner ID (assumed fixed value)
        }

    if save:
        # Save the new installment records to the 'installments' table in the database.
        df.to_sql('installments', engine, if_exists='append', index=False)

    # Return only the installments related to the given credit ID.
    return df.loc[df['ID_Op'] == id_credit]


def new_credit(
        id_customer: int,
        Date_Settlement: pd.Timestamp,
        ID_BP: int,
        Cap_Requested: float,
        Cap_Grant: float,
        N_Inst: int,
        TEM_W_IVA: float,
        V_Inst: float = None,
        D_F_Due: pd.Timestamp = None,
        ID_Purch: int = None,
        First_Inst_Purch: int = 0,
        ID_Sale: int = None,
        First_Inst_Sold: int = 0,        
        id_external: int = None,
        save: bool = False,
        massive = False
        ) -> pd.DataFrame:
    """
    Creates a new credit record and its corresponding installment schedule.

    Parameters:
    id_customer (int): The customer ID associated with the credit.
    Date_Settlement (pd.Timestamp): The settlement date of the credit.
    ID_BP (int): The business partner ID.
    Cap_Requested (float): The requested capital amount.
    Cap_Grant (float): The granted capital amount.
    N_Inst (int): The number of installments.
    TEM_W_IVA (float): The effective monthly rate including VAT.
    V_Inst (float, optional): The installment value (if known). Defaults to None.
    D_F_Due (pd.Timestamp, optional): The date of the first installment due. Defaults to None.
    ID_Purch (int, optional): The ID of the purchaser (if applicable). Defaults to None.
    First_Inst_Purch (int, optional): The first installment number purchased. Defaults to 0.
    ID_Sale (int, optional): The ID of the seller (if applicable). Defaults to None.
    First_Inst_Sold (int, optional): The first installment number sold. Defaults to 0.
    id_external (int, optional): An external identifier for the credit. Defaults to None.

    Returns:
    pd.DataFrame: A DataFrame with the new credit details.
    pd.DataFrame: A DataFrame with the generated installment schedule.
    """
    # Calculate the expected installment value using the PMT formula.
    value_inst = npf.pmt(TEM_W_IVA, N_Inst, Cap_Grant)
    
    # If the installment value (V_Inst) is not provided, use the calculated value.
    if V_Inst is None:
        V_Inst = -value_inst
    # Validate that the provided installment value matches the calculated value.
    elif abs(value_inst + V_Inst) > 1:
        raise ValueError(
            f"The rate ({TEM_W_IVA:,.2%}) and the number of installments ({N_Inst}) don't match the provided installment value ($ {V_Inst:,.2f} vs $ {value_inst:,.2f})."
        )
    
    # Retrieve global settings from the 'settings' table.
    df_set = pd.read_sql('settings', engine, index_col='ID')
    due_day = int(df_set.loc[1, 'Value'])          # Default due day of the month.
    grace_periods = int(df_set.loc[2, 'Value'])    # Number of grace months.
    
    # Calculate the next due date based on the settlement date and the due day.
    next_due = pd.Timestamp(year=Date_Settlement.year, month=Date_Settlement.month, day=due_day)
    
    # Set the first due date, applying the grace period if not explicitly provided.
    if D_F_Due is None:
        D_F_Due = next_due + relativedelta(months=grace_periods)
    # Ensure the first due date is not before the settlement date.
    elif D_F_Due < next_due:
        raise ValueError("The first due date cannot be earlier than the settlement date.")

    # Prepare the credit data to be inserted into the database.
    data_cr = {
        'ID_External': int(id_external) if id_external is not None else None,
        'ID_Client': int(id_customer),
        'Date_Settlement': Date_Settlement,
        'ID_BP': int(ID_BP),
        'Cap_Requested': float(Cap_Requested),
        'Cap_Grant': float(Cap_Grant),
        'N_Inst': int(N_Inst),
        'First_Inst_Purch': int(First_Inst_Purch),
        'TEM_W_IVA': float(TEM_W_IVA),
        'V_Inst': float(V_Inst) if V_Inst is not None else None,
        'First_Inst_Sold': int(First_Inst_Sold) if First_Inst_Sold is not None else None,
        'D_F_Due': D_F_Due,
        'ID_Purch': int(ID_Purch) if ID_Purch is not None else None,
        'ID_Sale': int(ID_Sale) if ID_Sale is not None else None
    }

    # Load existing credits to determine the next credit ID.
    df = pd.read_sql('credits', engine, index_col='ID')
    new_cr = df.iloc[0:0].copy()
    if massive:
        id = massive
    else:
        id = df.index.max() + 1 if not df.empty else 1

    # Add the new credit record to the DataFrame.
    new_cr.loc[id, [
        'ID_External', 'ID_Client', 'Date_Settlement', 'ID_BP', 'Cap_Requested', 'Cap_Grant', 
        'N_Inst', 'First_Inst_Purch', 'TEM_W_IVA', 'V_Inst', 'First_Inst_Sold', 
        'D_F_Due', 'ID_Purch', 'ID_Sale'
    ]] = data_cr

    if save:
        # Save the new credit record to the database.
        new_cr.to_sql('credits', engine, if_exists='append', index=False)


    # Generate installments for the new credit and save them to the database.
    installments = create_installments(id, new_cr.loc[id], save)
    
    return new_cr, installments


def credits_balance() -> pd.DataFrame:
    """
    Calculates the balance of credits by adjusting the installment amounts based on the recorded collections.

    This function retrieves data from the `collection` and `installments` tables, adjusts the values in 
    installments to account for the amounts already collected, and returns the updated installment data.

    Returns:
        pd.DataFrame: A DataFrame representing the updated balances for all installments, 
                      with columns for 'Capital', 'Interest', 'IVA', and 'Total'.
    """

    # Load the collections table and convert financial columns to float
    df_clt = pd.read_sql('collection', engine, index_col='ID')
    df_clt[['Capital', 'Interest', 'IVA', 'Total']] = df_clt[['Capital', 'Interest', 'IVA', 'Total']].astype(float)
    
    # Load the installments table and convert financial columns to float
    df_its = pd.read_sql('installments', engine, index_col='ID')
    df_its[['Capital', 'Interest', 'IVA', 'Total']] = df_its[['Capital', 'Interest', 'IVA', 'Total']].astype(float)
    
    # Group collections by installment ID and calculate the total for each column
    df_coll = df_clt.groupby('ID_Inst')[['Capital', 'Interest', 'IVA', 'Total']].sum()
    
    # Adjust the installment amounts by subtracting the collected amounts
    for c in ['Capital', 'Interest', 'IVA', 'Total']:
        df_its.loc[df_its.index.isin(df_coll.index), c] -= df_coll[c]
        df_its[c] = df_its[c].round(2)

    # Return the updated installments DataFrame
    return df_its


def _get_customer_id(ident_type, identifier):
    """Retrieve the customer ID based on the identifier type."""
    customers = pd.read_sql('customers', engine, index_col='ID')
    if ident_type == TypeDataCollection.DNI:
        return customers.loc[customers['DNI'] == identifier].index.values[0]
    elif ident_type == TypeDataCollection.CUIL:
        return customers.loc[customers['CUIL'] == identifier].index.values[0]


def _get_credits_by_identifier(ident_type, identifier, id_supplier):
    """Fetch credits based on the provided identifier type and value."""
    credits = pd.read_sql('credits', engine, index_col='ID')

    if ident_type in [TypeDataCollection.DNI, TypeDataCollection.CUIL]:
        id_cst = _get_customer_id(ident_type, identifier)

        if id_supplier:
            bp = pd.read_sql('business_plan', engine, index_col='ID')
            bps = bp.loc[bp['ID_Company'] == id_supplier].index.values
            id_credits = credits.loc[(credits['ID_Client'] == id_cst) & (credits['ID_BP'].isin(bps))].index.values
        else:
            id_credits = credits.loc[credits['ID_Client'] == id_cst].index.values
        
        if len(id_credits) == 0:
            raise IdentifierError(ident_type, identifier)

    elif ident_type == TypeDataCollection.ID_Op:
        id_credits = [identifier]
        
    else:
        id_credits = credits.loc[credits['ID_External'] == identifier].index.values

    return id_credits, credits


def _prepare_balance(id_credits):
    """Prepare and process the balance data for the given credits."""
    balance = credits_balance()
    balance = balance.loc[balance['ID_Op'].isin(id_credits)]
    balance.sort_values(by=['D_Due', 'ID_Op', 'Nro_Inst'], inplace=True)
    balance = balance.reset_index()
    balance['Accumulated'] = [balance.loc[balance.index <= i, 'Total'].sum() for i in balance.index]
    balance.set_index('ID', inplace=True)
    return balance


def _process_regular_collections(balance, amount, date):
    """Process regular collection installments."""
    
    # Initialize an empty collection DataFrame
    collection = pd.read_sql('collection', engine, index_col='ID').iloc[0:0]

    for n, i in enumerate(balance.loc[balance['Accumulated'] <= amount].index):
        collection.loc[n] = {
            'ID_Inst': i,
            'D_Emission': date,
            'Type_Collection': 'COMÚN',
            'Capital': balance.loc[i, 'Capital'],
            'Interest': balance.loc[i, 'Interest'],
            'IVA': balance.loc[i, 'IVA'],
            'Total': balance.loc[i, 'Total']
        }
    
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

    if amount <= 0.0:
        return collection

    penalty = amount / 1.21
    iva = penalty * 0.21

    id_cst = credits.loc[credits.index.isin(id_credits), 'ID_Client'].values[0]
    
    cr_penalty = credits.iloc[0:0].copy()
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

    inst = pd.read_sql('installments', engine, index_col='ID')
    id_penalty = inst.index.max() + 1
    inst = inst.iloc[0:0].copy()
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

    if save and not early:
        cr_penalty.to_sql('credits', engine, index=False, if_exists='append')
        inst.to_sql('installments', engine, index=False, if_exists='append')
        return collection
    elif save and early:
        cr_penalty.to_sql('credits', engine, index=False, if_exists='append')
        inst.to_sql('installments', engine, index=False, if_exists='append')
        return collection, cr_penalty, inst
    else:
        return collection, cr_penalty, inst


def _process_remaining_amount(amount, collection, balance, credits, date, id_credits, save, early: bool = False):
    """Handle any remaining amount, including early payments or penalties."""
    if not early:
        amount -= collection['Total'].sum()
    if amount > 0.0 and not balance.loc[~balance.index.isin(collection['ID_Inst'].values)].empty:
        collection = _process_early_payment(balance, amount, collection, date, early)
        amount -= collection.iloc[-1]['Total']

    if amount > 0.0:
        if save and not early:
            _process_penalty(amount, credits, date, id_credits, collection, save, early)
        else:
            collection, cr_penalty, inst = _process_penalty(amount, credits, date, id_credits, collection, save, early)
            return collection, cr_penalty, inst
    else:
        return collection, pd.DataFrame(), pd.DataFrame()


def _solve_rounding(collection, balance, date, early: bool = False, id_credits = None):
    """Adjusts the collection records to account for small rounding differences in financial calculations."""
    # Adjust balance by subtracting the sums of collected amounts grouped by installment ID
    if early:
        balance = _prepare_balance(id_credits)
    balance[['Capital', 'Interest', 'IVA', 'Total']] -= collection.groupby('ID_Inst')[['Capital', 'Interest', 'IVA', 'Total']].sum()
    
    # Identify rows where remaining total balance is small (less than 0.1) but non-zero
    rounding = balance.loc[(balance['Total'] != 0.0) & (balance['Total'] < 0.1)]

    # Add rounding adjustment entries to the collection for each identified installment
    for i in rounding.index:
        collection.loc[collection.index.max() + 1] = {
            'ID_Inst': i,
            'D_Emission': date,
            'Type_Collection': 'REDONDEO',
            'Capital': rounding.loc[i, 'Capital'],
            'Interest': rounding.loc[i, 'Interest'],
            'IVA': rounding.loc[i, 'IVA'],
            'Total': rounding.loc[i, 'Total']
        }
    
    # Return the updated collection DataFrame
    return collection


def charging(
    ident_type: TypeDataCollection,
    identifier: int,
    amount: float,
    id_supplier: int,
    date: pd.Timestamp = pd.Timestamp.now().strftime('%Y/%m/%d'),
    save: bool = False):
    """
    Processes a collection operation based on a given identifier and type.

    This function manages the collection of payments for credits by validating the identifier,
    retrieving the appropriate credit data, and processing regular collections, early payments,
    or penalties. The function can save the results to the database or return the processed data.

    Args:
        ident_type (TypeDataCollection): Type of identifier (e.g., DNI, CUIL, ID_Op, ID_External).
        identifier (int): Identifier value for the collection.
        amount (float): Amount to be collected.
        id_supplier (int, optional): ID of the supplier for business plans. Default is None.
        date (pd.Timestamp, optional): Collection date. Defaults to the current date.
        save (bool, optional): Whether to save the results to the database. Default is False.

    Returns:
        pd.DataFrame: If `save` is False, returns the collection DataFrame.
                      If penalties are involved and `save` is False, returns a tuple:
                      (penalty credits DataFrame, installments DataFrame, collection DataFrame).
    """
    # Validate the identifier type and value
    TypeDataCollection.validate(identifier, ident_type)

    # Return an empty DataFrame if the collection amount is zero or negative
    if amount <= 0.0:
        return pd.read_sql('collection', engine, index_col='ID').iloc[0:0]

    # Retrieve credits based on identifier type
    id_credits, credits = _get_credits_by_identifier(ident_type, identifier, id_supplier)

    # Retrieve and prepare the balance for the credits
    balance = _prepare_balance(id_credits)

    # Process regular collections based on the balance and amount
    collection = _process_regular_collections(balance, amount, date)

    # Adjust remaining amount and process early payments or penalties if necessary
    if save:
        _process_remaining_amount(amount, collection, balance, credits, date, id_credits, save)
    else:
        collection, cr_penalty, inst = _process_remaining_amount(amount, collection, balance, credits, date, id_credits, save)

    # Remove zero-total rows and round numerical columns to two decimal places
    collection = collection.loc[collection['Total'] != 0]
    for col in ['Capital', 'Interest', 'IVA', 'Total']:
        collection[col] = collection[col].round(2)

    # Solve any rounding issues and finalize the collection
    collection = _solve_rounding(collection, balance, date)

    # Save or return the results
    if save:
        collection.to_sql('collection', engine, index=False, if_exists='append')
        cr_penalty.to_sql('credits', engine, index=False, if_exists='append')
        inst.to_sql('installments', engine, index=False, if_exists='append')

        return collection
    else:
        return collection, cr_penalty, inst


def _process_early_settlement(collection, early, date, collection_type, amount):
    """Processes early settlement collection installments."""
    # Iterate over each installment in the early settlements DataFrame
    for i in early.index:
        n = collection.index.max() + 1 if len(collection) > 0 else 0
        # Append the installment details to the collection DataFrame
        collection.loc[n] = {
            'ID_Inst': i,
            'D_Emission': date,
            'Type_Collection': collection_type,
            'Capital': early.loc[i, 'Capital'],
            'Interest': early.loc[i, 'Interest'],
            'IVA': early.loc[i, 'IVA'],
            'Total': early.loc[i, 'Total']
        }

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
        pd.DataFrame: A DataFrame containing the collection records. 
                      If penalties or bonuses are involved and `save` is False, 
                      returns the complete collection DataFrame.
    """
    # Validate the identifier type and value
    TypeDataCollection.validate(identifier, ident_type)

    # Return an empty collection if the amount is not positive
    if amount <= 0.0:
        return pd.read_sql('collection', engine, index_col='ID').iloc[0:0]

    # Retrieve credits based on the identifier type
    id_credits, credits = _get_credits_by_identifier(ident_type, identifier, id_supplier)

    # Retrieve and prepare the balance for the credits
    balance = _prepare_balance(id_credits)
    common = balance.loc[balance['D_Due'] <= date].copy()  # Due installments

    # Process regular collections for due installments
    collection = _process_regular_collections(common, amount, date)
    amount -= collection['Total'].sum()

    # Handle early settlements for future installments
    early = balance.loc[balance['D_Due'] > date].copy()
    esd = early.copy()  # Early settlement details for bonuses
    early[['Interest', 'IVA']] = 0.0
    early['Total'] = early['Capital']
    early.sort_values(by=['D_Due', 'ID_Op', 'Nro_Inst'])
    early['Accumulated'] = [early.loc[esd.index <= i, 'Total'].sum() for i in early.index]
    early = early.loc[early['Accumulated'] <= amount]
    esd = esd.loc[esd.index.isin(early.index)]
    # Process early settlements and add them to the collection
    collection = _process_early_settlement(collection, early, date, 'CAN. ANT.', amount)
    amount -= collection.loc[collection['Type_Collection'] == 'CAN. ANT.', 'Total'].sum()
    
    # Apply bonuses for early cancellations
    esd['Capital'] = 0.0
    esd['Total'] = esd['Interest'] + esd['IVA']
    collection = _process_early_settlement(collection, esd, date, 'BON. CAN. ANT.', amount)
    
    # Adjust balance by subtracting the sums of collected amounts grouped by installment ID
    balance.loc[balance.index.isin(collection['ID_Inst']), ['Capital', 'Interest', 'IVA', 'Total']] -= collection.groupby('ID_Inst')[['Capital', 'Interest', 'IVA', 'Total']].sum()

    # Adjust remaining amount and process any penalties or adjustments
    collection, penalties, installments = _process_remaining_amount(amount, collection, balance, credits, date, id_credits, save, True)
    amount -= collection.loc[collection['Type_Collection'] == 'PENALTY', 'Total'].sum()
    
    # Clean up the collection DataFrame and ensure proper rounding
    collection = collection.loc[collection['Total'] != 0]
    for c in ['Capital', 'Interest', 'IVA', 'Total']:
        collection[c] = collection[c].round(2)

    # Solve rounding discrepancies
    _solve_rounding(collection, balance, date, True, id_credits)

    # Save or return the collection
    if save:    
        collection.to_sql('collection', engine, index=False, if_exists='append')

    return collection, penalties, installments


def reverse(
        ident_type: TypeDataCollection,
        identifier: int,
        amount: float,
        id_supplier: int,
        date: pd.Timestamp = pd.Timestamp.now(),
        save: bool = False
):
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
        ValueError: If the file type is unsupported or if the type_data is invalid.
    """
    
    # Ensure the file path exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"The file '{path}' does not exist.")

    # Mapping of the TypeDataCollection values to corresponding header names
    index_mapping = {
        TypeDataCollection.ID_Op: 'ID_Op',       # Map to 'ID_Op' if TypeDataCollection.ID_Op
        TypeDataCollection.ID_Ext: 'ID_Ext',     # Map to 'ID_Ext' if TypeDataCollection.ID_Ext
        TypeDataCollection.DNI: 'DNI',           # Map to 'DNI' if TypeDataCollection.DNI
        TypeDataCollection.CUIL: 'CUIL'          # Map to 'CUIL' if TypeDataCollection.CUIL
    }

    # Retrieve the header name based on the type_data argument
    index = index_mapping.get(type_data)
    if index is None:
        raise ValueError(f"Unsupported type_data: {type_data}")
    
    # Get the file extension to determine how to read the file
    _, extension = os.path.splitext(path)
    extension = extension.lower()

    # Read the file based on the extension (CSV or Excel)
    if extension == '.csv':
        # Read CSV file assuming semicolon as separator (adjust as needed)
        df = pd.read_csv(path, sep=";", header=None)
    elif extension == '.xlsx':
        # Read Excel file
        df = pd.read_excel(path, header=None)
    else:
        # Raise an error if the file extension is not supported
        raise ValueError(f"Unsupported file type: '{extension}'.")
    
    # Assign appropriate column names: identifier (based on type_data) and 'Amount'
    df.columns = [index, 'Amount']

    # Ensure the 'Amount' column is numeric for proper calculations
    df['Amount'] = df['Amount'].astype(float)

    # Group the DataFrame by the identifier (index) and sum the 'Amount' values
    df = pd.DataFrame(df.groupby(index)['Amount'].sum())
    
    # Return the processed DataFrame
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

        except IdentifierError:
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

    # Read the input file and process it into a DataFrame
    df = read_collection_file(path, type_data)

    # Print formatted DataFrame for verification/debugging
    print(df.map('${:,.2f}'.format))

    # Initialize empty lists to collect the resulting DataFrames
    collection_list = []
    penalty_list = []
    inst_list = []

    # Initialize an error DataFrame to capture problematic records
    error = pd.DataFrame(columns=['Amount'])
    error.index.name = df.index.name

    # Iterate through the input DataFrame index
    for i in df.index:
        try:
            # Process early collection logic for each record
            collections, penalties, installments = collection_w_early_cancel(type_data, i, df.loc[i, 'Amount'], id_supplier, date)

            # Append the results to the corresponding lists
            collection_list.append(collections)
            penalty_list.append(penalties)
            inst_list.append(installments)
        except IdentifierError:
            # Capture errors in processing and store in the error DataFrame
            error.loc[i, 'Amount'] = df.loc[i, 'Amount']

    # Combine the collected DataFrames into final DataFrames
    penalties = pd.concat(penalty_list, ignore_index=True) if penalty_list else pd.DataFrame()
    installments = pd.concat(inst_list, ignore_index=True) if inst_list else pd.DataFrame()
    collections = pd.concat(collection_list, ignore_index=True) if collection_list else pd.DataFrame(columns=['ID_Inst', 'D_Emission', 'Type_Collection', 'Capital', 'Interest', 'IVA', 'Total'])

    # Handle special cases for installments and penalties
    if not installments.empty:
        new_penalty = collections.loc[collections['Type_Collection'] == 'PENALTY', 'ID_Inst'].copy()
        collections.loc[collections['Type_Collection'] == 'PENALTY', 'ID_Inst'] = [new_penalty.max() + i for i in range(len(new_penalty))]
        installments['ID_Op'] = [pd.read_sql('credits', engine, index_col='ID').index.max() + i + 1 for i in range(len(installments))]

    # Set index names for the resulting DataFrames
    collections.index.name = 'ID'
    penalties.index.name = 'ID'
    installments.index.name = 'ID'

    # Round numerical columns for better precision
    for c in ['Capital', 'Interest', 'IVA', 'Total']:
        collections[c] = collections[c].round(2)
        installments[c] = installments[c].round(2)

    collections = collections.loc[~((collections['Capital'] == 0.0) &\
                                    (collections['Interest'] == 0.0) &\
                                    (collections['IVA'] == 0.0) &\
                                    (collections['Total'] == 0.0))]

    # Save the resulting DataFrames to the database if specified
    if save:
        try:
            if penalties.empty and installments.empty:
                # Save collections only if no penalties or installments exist
                collections.to_sql('collection', engine, index=False, if_exists='append')
            else:
                # Save penalties, installments, and collections
                penalties.to_sql('credits', engine, index=False, if_exists='append')
                installments.to_sql('installments', engine, index=False, if_exists='append')
                collections.to_sql('collection', engine, index=False, if_exists='append')

        except IntegrityError:
            # Handle database integrity errors and return current state
            print(f"IntegrityError.\n Check the error DataFrame.")
            return df, collections, error, penalties, installments

    # Return the processed DataFrames and the error DataFrame
    return collections, penalties, installments, error

