import pandas as pd

# Import your module
from app.modules.database.connection import engine

import numpy_financial as npf
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy_financial as npf
from dateutil.relativedelta import relativedelta

def create_installments(id_credit: int, new_cr: pd.Series, save: bool = False) -> pd.DataFrame:
    """
    Creates and stores installment records for a given credit.

    Parameters:
        id_credit (int): The unique identifier of the credit for which installments need to be created.
        new_cr (pd.Series): Credit details used to generate installments.
        save (bool, optional): If True, saves the installments to the database. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing the installments for the specified credit.
    """

    # ✅ Step 1: Retrieve the last installment ID from the database
    last_inst_id = pd.read_sql("SELECT MAX(ID) FROM installments", engine).iloc[0, 0]
    last_inst_id = 0 if pd.isna(last_inst_id) else int(last_inst_id)

    # ✅ Step 2: Prepare credit data
    cr = new_cr.copy()
    cr["D_F_Due"] = pd.Period(cr["D_F_Due"], freq="D")

    # ✅ Step 3: Generate installment details
    v_inst = -npf.pmt(cr["TEM_W_IVA"], cr["N_Inst"], cr["Cap_Grant"])
    installment_data = []

    for i in range(int(cr["N_Inst"])):
        last_inst_id += 1

        # Calculate the interest portion using the IPMT formula
        interest = -npf.ipmt(cr["TEM_W_IVA"], i + 1, cr["N_Inst"], cr["Cap_Grant"])
        d_due = cr["D_F_Due"].asfreq("M") + i

        # Append installment data
        installment_data.append({
            "ID": last_inst_id,
            "ID_Op": id_credit,  # Credit operation ID
            "Nro_Inst": i + 1,  # Installment number
            "D_Due": pd.Timestamp(year=d_due.year, month=d_due.month, day=cr["D_F_Due"].day),  # Due date
            "Capital": v_inst - interest,  # Principal portion
            "Interest": interest / 1.21,  # Interest portion (excluding VAT)
            "IVA": interest / 1.21 * 0.21,  # VAT on interest
            "Total": v_inst,  # Total installment value
            "ID_Owner": 1  # Owner ID (assumed fixed value)
        })

    # ✅ Step 4: Convert to DataFrame
    df = pd.DataFrame(installment_data)
    df.set_index('ID', inplace=True)

    # ✅ Step 5: Save to database if required
    if save:
        try:
            df.to_sql("installments", engine, index=False, if_exists="append")
        except Exception as e:
            print(f"❌ Error saving installments: {e}")

    # ✅ Step 6: Return only installments related to the given credit ID
    return df[df["ID_Op"] == id_credit]


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
        massive=False
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Creates a new credit record and its corresponding installment schedule.

    Returns:
        tuple:
            - pd.DataFrame: New credit details.
            - pd.DataFrame: Generated installment schedule.
    """

    # ✅ Step 1: Calculate the expected installment value using the PMT formula.
    value_inst = npf.pmt(TEM_W_IVA, N_Inst, Cap_Grant)

    # ✅ Step 2: Validate installment value
    if V_Inst is None:
        V_Inst = -value_inst
    elif abs(value_inst + V_Inst) > 1:
        raise ValueError(
            f"The rate ({TEM_W_IVA:,.2%}) and the number of installments ({N_Inst}) "
            f"don't match the provided installment value ($ {V_Inst:,.2f} vs $ {value_inst:,.2f})."
        )

    # ✅ Step 3: Retrieve global settings from the 'settings' table.
    df_set = pd.read_sql("SELECT ID, Value FROM settings WHERE ID IN (1, 2)", engine).set_index("ID")["Value"]
    due_day, grace_periods = int(df_set[1]), int(df_set[2])

    # ✅ Step 4: Compute the next due date based on settings
    next_due = pd.Timestamp(year=Date_Settlement.year, month=Date_Settlement.month, day=due_day)
    if D_F_Due is None:
        D_F_Due = next_due + relativedelta(months=grace_periods)
    elif D_F_Due < next_due:
        raise ValueError("The first due date cannot be earlier than the settlement date.")

    # ✅ Step 5: Determine the next credit ID
    id = pd.read_sql("SELECT MAX(ID) FROM credits", engine).iloc[0, 0]
    id = massive if massive else (1 if pd.isna(id) else int(id) + 1)

    # ✅ Step 6: Prepare the credit record
    new_cr = pd.DataFrame([{
        'ID': id,
        'ID_External': int(id_external) if id_external is not None else None,
        'ID_Client': int(id_customer),
        'Date_Settlement': Date_Settlement,
        'ID_BP': int(ID_BP),
        'Cap_Requested': float(Cap_Requested),
        'Cap_Grant': float(Cap_Grant),
        'N_Inst': int(N_Inst),
        'First_Inst_Purch': int(First_Inst_Purch),
        'TEM_W_IVA': float(TEM_W_IVA),
        'V_Inst': float(V_Inst),
        'First_Inst_Sold': int(First_Inst_Sold) if First_Inst_Sold is not None else None,
        'D_F_Due': D_F_Due,
        'ID_Purch': int(ID_Purch) if ID_Purch is not None else None,
        'ID_Sale': int(ID_Sale) if ID_Sale is not None else None
    }])
    new_cr.set_index('ID', inplace=True)
    
    # ✅ Step 7: Save the new credit record to the database
    if save:
        try:
            new_cr.to_sql('credits', engine, if_exists='append', index=False)
        except Exception as e:
            print(f"❌ Error saving credit record: {e}")

    # ✅ Step 8: Generate installments and save them if needed
    installments = create_installments(id, new_cr.loc[id], save)

    return new_cr, installments


def credits_balance(date: pd.Timestamp = pd.Timestamp.now()) -> pd.DataFrame:
    """
    Calculates the balance of credits by adjusting installment amounts based on recorded collections.

    This function retrieves data from the `collection` and `installments` tables, adjusts the values in 
    installments to account for the amounts already collected, and returns the updated installment data.

    Parameters:
        date (pd.Timestamp, optional): The reference date for balance calculations. Defaults to the current date.

    Returns:
        pd.DataFrame: Updated installment balances with columns for 'Capital', 'Interest', 'IVA', and 'Total'.
    """
    
    # ✅ Step 1: Ensure correct date format
    date = pd.Timestamp(date)

    # ✅ Step 2: Load collections and filter by date
    df_clt = pd.read_sql(f"SELECT * FROM collection WHERE 'D_Emission' <= {pd.Period(date, freq='D')}", engine, index_col="ID")
    
    # ✅ Step 3: Convert financial columns to float for calculations
    numeric_cols = ["Capital", "Interest", "IVA", "Total"]
    df_clt[numeric_cols] = df_clt[numeric_cols].astype(float)

    # ✅ Step 4: Load installments and filter only relevant credits
    df_its = pd.read_sql("SELECT * FROM installments", engine, index_col="ID")
    credits = pd.read_sql(f"SELECT ID FROM credits WHERE 'Date_Settlement' <= {pd.Period(date, freq='D')}", engine, index_col="ID")
    
    df_its = df_its[df_its["ID_Op"].isin(credits.index)]

    # ✅ Step 5: Convert installment financial columns to float
    df_its[numeric_cols] = df_its[numeric_cols].astype(float)

    # ✅ Step 6: Aggregate collections by installment ID
    df_coll = df_clt.groupby("ID_Inst")[numeric_cols].sum()

    # ✅ Step 7: Adjust installments by subtracting collected amounts
    for col in numeric_cols:
        df_its.loc[df_its.index.isin(df_coll.index), col] -= df_coll[col]
    
    # ✅ Step 8: Prevent negative values due to floating-point errors and round values
    df_its[numeric_cols] = df_its[numeric_cols].round(2)

    credits = pd.read_sql('credits', engine, index_col='ID')
    bp = pd.read_sql('business_plan', engine, index_col='ID')
    companies = pd.read_sql('companies', engine, index_col='ID')

    df_its['Anchorer'] = df_its['ID_Op'].apply(lambda x: credits.loc[x, 'ID_BP'])
    df_its['Anchorer'] = df_its['Anchorer'].apply(lambda x: bp.loc[x, 'ID_Company'])
    df_its['Anchorer'] = df_its['Anchorer'].apply(lambda x: companies.loc[x, 'Social_Reason'])
    df_its['Owner']    = df_its['ID_Owner'].apply(lambda x: companies.loc[x, 'Social_Reason'])
    df_its.drop(columns=['ID_Owner'], inplace=True)

    # ✅ Step 9: Return the updated installment balances
    return df_its

