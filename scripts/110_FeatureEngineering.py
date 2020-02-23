import pymongo
import random
import pandas as pd
from datetime import timedelta
from config import password
from funs import functions as f

raw_data = pd.read_feather('results/raw_data.feather')

sampled_raw_data = raw_data.sample(100000)

del raw_data

# -- Imputing missing values -------------------------------------------------------------------------------------------

sampled_raw_data['NoOfPastBusinesses'] = sampled_raw_data.NoOfPastBusinesses.fillna(0).astype(int)


# -- Creating new variables from existing ones -------------------------------------------------------------------------

# - Duration of Existence

sampled_raw_data['RandomDate'] = sampled_raw_data.apply(lambda x: f.random_date(x['StartDate'], x['EndDate']), axis=1)

delta = timedelta(days=365)
sampled_raw_data['RandomDatePlus12M'] = sampled_raw_data.RandomDate + delta

sampled_raw_data['DurationOfExistenceInMonths'] = f.month_diff(sampled_raw_data.RandomDate,
                                                               sampled_raw_data.StartingDateOfTheBusiness)
sampled_raw_data['DurationOfExistenceInMonths'] = sampled_raw_data.DurationOfExistenceInMonths.astype(int)

sampled_raw_data['RandomDate'] = sampled_raw_data.RandomDate.dt.strftime('%Y-%m-%d')

# - Month and Quarter of Starting of the Business

sampled_raw_data['MonthOfStartingOfTheBusiness'] = sampled_raw_data.StartingDateOfTheBusiness.dt.month_name()
sampled_raw_data['QuarterOfStartingOfTheBusiness'] = sampled_raw_data.StartingDateOfTheBusiness.dt.quarter.astype(object)

cols_to_drop = ['Status', 'StartDate', 'EndDate', 'Date', 'StartingDateOfTheBusiness', 'RandomDatePlus12M']

#  -- Target varibale

sampled_raw_data['Target'] = sampled_raw_data.RandomDatePlus12M > sampled_raw_data.DateOfTerminationOrSuspension
sampled_raw_data['Target'] = sampled_raw_data.Target.astype(int)

sampled_raw_data = sampled_raw_data.drop(columns=cols_to_drop)
sampled_raw_data = sampled_raw_data.reset_index()

# -- save DataFrame to files -------------------------------------------------------------------------------------------

sampled_raw_data.to_feather('results/ceidg_data_sample.feather')
