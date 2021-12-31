import pandas as pd
import numpy as np
from datetime import timedelta
from pandasql import sqldf

"""
UHI Analysis: Terminals & Insurance
"""
# import data
first_df = pd.read_csv(r'C:\Users\Noe_N\OneDrive\Projects\2021\UHI-DSA\docs\first.csv')
dispatch_df = pd.read_csv(r'C:\Users\Noe_N\OneDrive\Projects\2021\UHI-DSA\docs\disp.csv')

# check datatypes & convert where applicable (dt)
first_df.info()
first_df.head()

# update createdate to dt 
first_df.createdate = pd.to_datetime(first_df.createdate)

# dispatch datatype & conversion
dispatch_df.info()
dispatch_df.head()

dispatch_df.paymenttime = pd.to_datetime(dispatch_df.paymenttime)

# determine min and max dates
first_df.createdate.min()
first_df.createdate.max()

start_date = dispatch_df.paymenttime.min()
end_date = dispatch_df.paymenttime.max()
end_date - start_date #104/5 days

"""
Both dataframes capture data starting 1/2021 and ending mid April 2021. Approx 104/105 days.
"""

"""
Objective:
    Determine whether the terminal (transactionentrymethod == 'Emv') efficient in upselling insurance across TRUCK models
    If so, are they effective enough to justify a $10M replacement?
"""
# Establish Analysis Scope within Dfs (Trucks)
first_df.modeltype.unique() #unique model types

# there is an unknown section, lets fill if necessary
model_unknown_arr = first_df[first_df.modeltype == 'unknown'].model.unique()
model_known_arr = first_df[first_df.modeltype != 'unknown'].model.unique()
# compare the two arrays, see if there is overlap between model_unknown and known
np.isin(model_unknown_arr, model_known_arr)
"""
All models listed as 'unknown' under the 'modeltype' column are present within the known cols.
Therefore, we can clean the data a bit to ensure all classifications are complete. We can do this by matching
all known modeltypes to their associated 'model'; if modeltype is unknown it will be updated. Lastly, the objective
calls for a truck scope. The data will be filtered accordingly
"""
# filter first df for truck modeltypes
model_known_first_query = """
    SELECT 
        *, 
        -- case statement for truck 'model' column values
        CASE 
            WHEN model IN (     
                SELECT 
                    DISTINCT
                        model
                FROM
                    first_df
                WHERE
                    modeltype = 'TRK')
            THEN 'TRK'
            ELSE modeltype
            END AS "modeltype2"
    FROM
        first_df
"""
# bruteforce drop
first_truck_df = sqldf(model_known_first_query)
first_truck_df = first_truck_df[first_truck_df.modeltype2 == 'TRK'][list(first_truck_df.columns.values)[:-1]] #apply filter, drop last col

# filter dispatch df for truck modeltype
model_known_disp_query = """
    SELECT 
        *,
        -- case statement for truck 'model' column values
        CASE 
            WHEN model IN (     
                SELECT 
                    DISTINCT
                        model
                FROM
                    dispatch_df
                WHERE
                    modeltype = 'TRK')
            THEN 'TRK'
            ELSE modeltype
            END AS "modeltype2"
    FROM 
        dispatch_df
    WHERE
        modeltype2 = 'TRK'
"""
# pandas drop
dispatch_truck_df = sqldf(model_known_disp_query)
dispatch_truck_df.drop('modeltype2', axis=1, inplace=True)

# Checkpoint: DFs have been scrubbed and filtered for only TRK models.
# --------------------------------------------------------------------

"""
Analysis
--------

    Data
    ----
    first: Reservations made on website; no info on insurance (could be online | walk-in)
    dispatch: Info when the EQP was dispatched; reservation start (online | walk-in)

    Assumption:
    -----------
        dispatch.transactionentrymethod == 'Emv' = payment(Terminal) & Insurance(Terminal)
        dispatch.transactionentrymethod == 'Emv' = payment(Agent) & Insurance(Agent)

    Types of Insurance:
    -------------------
        Trucks = $20 per rental

    Rental Outcomes
    ---------------
        1. Online
            i. Online + Insurance (Online) - Base Case Scenario
            ii. Online + Insurance (Terminal)
            iii. Online + Insurance (Agent)

        2. Walk-In
            i. Walk-In + Insurance (Terminal)
            ii. Walk-In + Insurance (Agent)

        What about the No insurance cases? 
"""


