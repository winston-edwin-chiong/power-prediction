from sklearn.base import BaseEstimator, TransformerMixin
from datetime import datetime
import pandas as pd 
import numpy as np 


class SortDropCast(BaseEstimator, TransformerMixin):
    """
    This pipeline step will sort values by field "connectTime",
    drop columns "user_email", "slrpPaymentId", 
    and cast columns "cumEnergy_Wh", "peakPower_W" as float values. 
    "connectTime" will be converted into pd_datetime like.
    """
    def fit(self, X, y=None):
        return self

    @staticmethod
    def transform(X) -> pd.DataFrame:
        now = datetime.now().strftime('%D')
        X = X.sort_values(by="connectTime").drop(columns=["user_email", "slrpPaymentId"]).reset_index(drop=True)
        X["connectTime"] = pd.to_datetime(X["connectTime"])
        X["cumEnergy_Wh"] = X["cumEnergy_Wh"].astype(float)
        X["peakPower_W"] = X["peakPower_W"].astype(float)
        X["userId"] = X["userId"].astype(str)
        X = X[X["connectTime"] >= now]
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
        X = X.loc[(X["peakPower_W"] != 0) & (X["cumEnergy_Wh"] != 0)]
        X = X.assign(reqChargeTime_h=(X["cumEnergy_Wh"] / X["peakPower_W"]))
        X = X.assign(connectTime=(pd.to_datetime(X["connectTime"])))
        X = X.assign(
            finishChargeTime=(X["connectTime"] + pd.to_timedelta(X['reqChargeTime_h'], unit='hours').round("s"))
        )
        return X 


class CreateNestedSessionTimeSeries(BaseEstimator, TransformerMixin):
    """
    This pipeline step will create a time series for each session. Two new columns will be created, 
    "time_vals" and "power_vals", respective lists for a time and power demand. "time_vals" are rounded to the 
    closest 5 min. 
    """ 
    def __init__(self) -> None:
        super().__init__()
    
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

        date_range = pd.date_range(start=session["connectTime"], end=session["finishChargeTime"], freq="5min").to_list()
        power_vals = np.ones(len(date_range)) * session["peakPower_W"]
        
        now = session["connectTime"].strftime('%D')
        temp_df = pd.DataFrame({"power":power_vals}, index=date_range).resample("5min").sum().reindex(index = pd.period_range(now, periods=288, freq='5min').to_timestamp(), fill_value=0)
        
        date_range = temp_df.index.to_list()
        power_vals = temp_df["power"].to_list()

        temp_df = pd.DataFrame([[date_range, power_vals]], columns=self.ts_df.columns)
        
        self.ts_df = pd.concat([self.ts_df, temp_df], ignore_index=True)

class SaveCSV(BaseEstimator, TransformerMixin):
    """
    Saves session level dataframe to file system. 
    """

    def fit(self, X, y=None):
        return self 

    def transform(self, X) -> pd.DataFrame:
        X.to_csv("data/todays_sessions.csv")
        return X