import pymongo
import pandas as pd
from datetime import timedelta
from config import password
from funs import functions as f

raw_data = pd.read_feather('results/raw_data.pickle')

sampled_raw_data = raw_data.sample(60000)

del raw_data

# -- Imputing missing values -------------------------------------------------------------------------------------------

sampled_raw_data['NoOfPastBusinesses'] = sampled_raw_data.NoOfPastBusinesses.fillna(0).astype(int)


# -- Creating new variables from existing ones -------------------------------------------------------------------------

# - Duration of Existence

sampled_raw_data['RandomDate'] = f.random_date(sampled_raw_data.StartDate, sampled_raw_data.EndDate, set_seed=42)

delta = timedelta(days=365)

sampled_raw_data['RandomDatePlus12M'] = sampled_raw_data.RandomDate + delta

sampled_raw_data['DurationOfExistenceInMonths'] = f.month_diff(sampled_raw_data.RandomDate,
                                                               sampled_raw_data.StartingDateOfTheBusiness)
sampled_raw_data['DurationOfExistenceInMonths'] = sampled_raw_data.DurationOfExistenceInMonths.astype(int)

# - Month of Starting of the Business

sampled_raw_data['MonthOfStartingOfTheBusiness'] = sampled_raw_data.StartingDateOfTheBusiness.dt.month_name()

cols_to_drop = ['Status', 'StartDate', 'EndDate', 'Date']

#  -- Target varibale

sampled_raw_data['Target'] = sampled_raw_data.RandomDatePlus12M > sampled_raw_data.DateOfTerminationOrSuspension
sampled_raw_data['Target'] = sampled_raw_data.Target.astype(int)

sampled_raw_data = sampled_raw_data.drop(columns=cols_to_drop)
sampled_raw_data = sampled_raw_data.reset_index()

# -- save DataFrame to files -------------------------------------------------------------------------------------------

sampled_raw_data.to_feather('results/ceidg_data_sample.feather')
