import os
import pandas as pd

# Import your module
from app.modules.database.connection import engine

from sqlalchemy.orm import sessionmaker
from sqlalchemy import update
import numpy_financial as npf
from dateutil.relativedelta import relativedelta


def create_installments(id_credit: int):
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
    cr = pd.read_sql('credits', engine, index_col='ID').loc[id_credit]

    # Determine the starting installment ID based on existing data.
    id_inst = df.index.max() + 1 if not df.empty else 0

    # Generate installment details for each installment in the credit term.
    v_inst   = -npf.pmt(cr['TEM_W_IVA'], cr['N_Inst'], cr['Cap_Grant'])
    for i in range(1, cr['N_Inst'] + 1):
        id_inst += 1
        
        # Calculate the interest portion of the installment using the IPMT formula.
        interest = -npf.ipmt(cr['TEM_W_IVA'], i, cr['N_Inst'], cr['Cap_Grant'])

        # Populate installment data for the current installment.
        df.loc[id_inst, ['ID_Op', 'Nro_Inst', 'D_Due', 'Capital', 'Interest', 'IVA', 'Total', 'ID_Owner']] = {
            'ID_Op': id_credit,  # Credit operation ID
            'Nro_Inst': i,  # Installment number
            'D_Due': cr['D_F_Due'] + relativedelta(month=i - 1),  # Due date
            'Capital': v_inst - interest,  # Principal portion
            'Interest': interest / 1.21,  # Interest portion (excluding VAT)
            'IVA': interest / 1.21 * 0.21,  # VAT on interest
            'Total': v_inst,  # Total installment value
            'ID_Owner': 1  # Owner ID (assumed fixed value)
        }

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
            f"The rate ({TEM_W_IVA:,.2%}) and the number of installments ({N_Inst}) don't match "
            f"the provided installment value ($ {V_Inst:,.2f})."
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
    id = df.index.max() + 1 if not df.empty else 1
    new_cr = df.iloc[0:0]

    # Add the new credit record to the DataFrame.
    new_cr.loc[id, [
        'ID_External', 'ID_Client', 'Date_Settlement', 'ID_BP', 'Cap_Requested', 'Cap_Grant', 
        'N_Inst', 'First_Inst_Purch', 'TEM_W_IVA', 'V_Inst', 'First_Inst_Sold', 
        'D_F_Due', 'ID_Purch', 'ID_Sale'
    ]] = data_cr

    # Save the new credit record to the database.
    new_cr.to_sql('credits', engine, if_exists='append', index=False)

    # Generate installments for the new credit and save them to the database.
    installments = create_installments(id)
    
    return df, installments
