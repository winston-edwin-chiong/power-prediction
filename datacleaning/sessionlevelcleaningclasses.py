from sklearn.base import BaseEstimator, TransformerMixin
from datetime import datetime
import pandas as pd 
import numpy as np 


class SortDropCast(BaseEstimator, TransformerMixin):
    """
    This pipeline step will sort values by field "connectTime",
    drop columns "user_email", "slrpPaymentId", 
    and cast columns "cumEnergy_Wh", "peakPower_W" as float values. 
    Time-based columns will be converted into pd_datetime like.
    """
    def fit(self, X, y=None):
        return self

    @staticmethod
    def transform(X) -> pd.DataFrame:
        X["cumEnergy_Wh"] = X["cumEnergy_Wh"].astype(float)
        X["peakPower_W"] = X["peakPower_W"].astype(float)
        X["userId"] = X["userId"].astype(str)

        X["connectTime"] = pd.to_datetime(X["connectTime"])
        X["startChargeTime"] = pd.to_datetime(X["startChargeTime"])
        X["Deadline"] = pd.to_datetime(X["Deadline"])
        X["lastUpdate"] = pd.to_datetime(X["lastUpdate"])

        X = X.loc[(X["peakPower_W"] != 0) & (X["cumEnergy_Wh"] != 0)].copy()

        X = X.sort_values(by="connectTime").drop(columns=["user_email", "slrpPaymentId"]).reset_index(drop=True)

        return X

class HelperFeatureCreation(BaseEstimator, TransformerMixin):
    """
    This pipeline step will drop any records that contain 0 for 
    "peakPower_W" or "cumEnergy_Wh". Two additional columns will be created:
    "reqChargeTime" and "finishChargeTime".
    """
    def fit(self, X, y=None):
        return self

    @staticmethod
    def transform(X) -> pd.DataFrame:
        X["reqChargeTime_h"] = X["cumEnergy_Wh"] / X["peakPower_W"]

        X["finishChargeTime"] = (X["startChargeTime"] + pd.to_timedelta(X['reqChargeTime_h'], unit='hours').round("s"))
        
        X = X.loc[X["reqChargeTime_h"] < 24].copy() # filter out bad rows (this occurs when there is a very low peak power and high energy delivered)

        X['temp_0'] = pd.Timedelta(days=0,seconds=0)
        X['Overstay'] = X["lastUpdate"] - X['Deadline']
        X["Overstay"] = X[["Overstay", "temp_0"]].max(axis=1)
        X['Overstay_h'] = X['Overstay'].dt.seconds / 3600
        X.drop(columns = ['temp_0'], inplace=True)

        return X 

class CreateNestedSessionTimeSeries(BaseEstimator, TransformerMixin):
    """
    This pipeline step will create a time series for each session. Two new columns will be created, 
    "time_vals" and "power_vals", respective lists for a time and power demand. "time_vals" are rounded to the 
    closest 5 min. 
    """ 
    
    def fit(self, X, y=None):
        return self 

    def transform(self, X) -> pd.DataFrame:
        self.ts_df = pd.DataFrame(columns=["time_vals", "power_vals"])
        X.apply(self.__create_ts, axis=1)
        X = pd.concat([X.reset_index(), self.ts_df], axis=1)
        X = X.explode(["time_vals", "power_vals"])
        X.rename(columns={"time_vals":"Time", "power_vals":"Power (W)"}, inplace=True)
        return X

    def __create_ts(self, session):

        date_range = pd.date_range(start=session["startChargeTime"], end=session["finishChargeTime"], freq="5min").to_list()
        power_vals = np.ones(len(date_range)) * session["peakPower_W"]
        
        now = session["startChargeTime"].strftime('%D')
        temp_df = pd.DataFrame({"power":power_vals}, index=date_range).resample("5min").sum().reindex(index = pd.period_range(now, periods=288, freq='5min').to_timestamp(), fill_value=0)
        
        date_range = temp_df.index.to_list()
        power_vals = temp_df["power"].to_list()

        temp_df = pd.DataFrame([[date_range, power_vals]], columns=self.ts_df.columns)
        
        self.ts_df = pd.concat([self.ts_df, temp_df], ignore_index=True)

class ResampleTimeSeries(BaseEstimator, TransformerMixin):
    """
    """

    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X = X[["Time", "Power (W)", "userId"]].set_index("Time", drop=True)
        OHE_users = pd.get_dummies(X["userId"]).astype(float)
        X = pd.concat([X, OHE_users], axis=1).drop(columns=["userId"])

        single_column = X["Power (W)"]
        temp = X.drop(columns=["Power (W)"]).multiply(single_column, axis='index')
        X = pd.concat([X["Power (W)"], temp], axis=1)

        X = X.resample("5MIN").sum()

        return X

class SaveToCsv(BaseEstimator, TransformerMixin):
    """
    """

    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X.to_csv("data/user_data.csv")
        return X