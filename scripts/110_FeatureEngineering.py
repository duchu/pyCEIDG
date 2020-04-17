
import pandas as pd
from datetime import timedelta
from funs import functions as f

preprocessed_data = pd.read_feather('results/raw_data.feather')


# -- Imputing missing values -------------------------------------------------------------------------------------------

preprocessed_data['NoOfPastBusinesses'] = preprocessed_data.NoOfPastBusinesses.fillna(0).astype(int)


# -- Creating new variables from existing ones -------------------------------------------------------------------------

# - Duration of Existence

# TODO set random seed

preprocessed_data['RandomDate'] = preprocessed_data.apply(lambda x: f.random_date(x['StartDate'], x['EndDate']), axis=1)

delta = timedelta(days=365)
preprocessed_data['RandomDatePlus12M'] = preprocessed_data.RandomDate + delta

preprocessed_data['DurationOfExistenceInMonths'] = f.month_diff(preprocessed_data.RandomDate,
                                                                preprocessed_data.StartingDateOfTheBusiness)
preprocessed_data['DurationOfExistenceInMonths'] = preprocessed_data.DurationOfExistenceInMonths.astype(int)

# - Month and Quarter based on  Starting of the Business and Date of Scoring

preprocessed_data['MonthOfStartingOfTheBusiness'] = preprocessed_data.StartingDateOfTheBusiness.dt.month_name()
preprocessed_data['QuarterOfStartingOfTheBusiness'] = preprocessed_data.StartingDateOfTheBusiness.dt.quarter.astype(
    object)

preprocessed_data['RandomDate'] = preprocessed_data.RandomDate.dt.strftime('%Y-%m-%d')

#  -- Target varibale

preprocessed_data['Target'] = preprocessed_data.RandomDatePlus12M > preprocessed_data.DateOfTerminationOrSuspension
preprocessed_data['Target'] = preprocessed_data.Target.astype(bool)

cols_to_drop = ['Status', 'StartDate', 'EndDate', 'StartingDateOfTheBusiness', 'RandomDatePlus12M', 'Status', 'NIP']
preprocessed_data = preprocessed_data.drop(columns=cols_to_drop)


# -- save DataFrame to file --------------------------------------------------------------------------------------------

preprocessed_data.to_csv('results/ceidg_data_classif.csv', index=False)

