from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd 


class SortDropCast(BaseEstimator, TransformerMixin):
    """
    This pipeline step will sort values by field "connectTime",
    drop columns "user_email", "slrpPaymentId", 
    and cast columns "cumEnergy_Wh", "peakPower_W" as float values. 
    """
    def fit(self, X, y=None):
        return self

    @staticmethod
    def transform(X) -> pd.DataFrame:
        X = X.sort_values(by="connectTime").drop(columns=["user_email", "slrpPaymentId"]).reset_index(drop=True)
        X["cumEnergy_Wh"] = X["cumEnergy_Wh"].astype(float)
        X["peakPower_W"] = X["peakPower_W"].astype(float)
        return X


class HelperFeatureCreation(BaseEstimator, TransformerMixin):
    """
    This pipeline step will drop any records that contain 0 for 
    "peakPower_W" or "cumEnergy_Wh". Four additional columns will be created:
    "reqChargeTime", "finishChargeTime", "Overstay", and "Overstay_h". 
    Any records with calculated charging durations greater than a day will be dropped. 
    Raw data at this staged will be saved.
    """
    def fit(self, X, y=None):
        return self

    @staticmethod
    def transform(X) -> pd.DataFrame:
        X = X.loc[(X["peakPower_W"] != 0) & (X["cumEnergy_Wh"] != 0)].copy(deep=True)

        X["reqChargeTime_h"] = X["cumEnergy_Wh"] / X["peakPower_W"]

        X["connectTime"] = pd.to_datetime(X["connectTime"])
        X["startChargeTime"] = pd.to_datetime(X["startChargeTime"])
        X["Deadline"] = pd.to_datetime(X["Deadline"])
        X["lastUpdate"] = (pd.to_datetime(X["lastUpdate"]))

        X["finishChargeTime"] = (X["startChargeTime"] + pd.to_timedelta(X['reqChargeTime_h'], unit='hours').round("s"))
        
        X = X.loc[X["reqChargeTime_h"] < 24] # filter out bad rows (this occurs when there is a very low peak power and high energy delivered)

        X['temp_0'] = pd.Timedelta(days=0,seconds=0)
        X['Overstay'] = X["lastUpdate"] - X['Deadline']
        X["Overstay"] = X[["Overstay", "temp_0"]].max(axis=1)
        X['Overstay_h'] = X['Overstay'].dt.seconds / 3600

        X.drop(columns = ['temp_0'], inplace=True)

        return X 


class CreateSessionTimeSeries(BaseEstimator, TransformerMixin):
    """
    This pipeline step will create a time series for each session. A dataframe
    with 5-min granularity will be returned, with one column, "power_demand_W".
    """ 

    def fit(self, X, y=None):
        return self 

    def transform(self, X) -> pd.DataFrame:
        self.rows = []
        X.apply(self.__create_ts, axis=1)
        X = pd.concat(self.rows, axis=0).sort_index()
        X = X.resample("5MIN").sum() # sum records that occur at the same time
        return X
    
    def __create_ts(self, session):
        """
        This helper function takes in a session, with a "startChargeTime", "finishChargeTime", and 
        a "peakPower_W" column. Function will return a time series at 5-min granularity. 
        """
        date_range = pd.date_range(start=session["startChargeTime"], end=session["finishChargeTime"], freq="5min")
        temp_df = pd.DataFrame(index=date_range)
        temp_df["avg_power_demand_W"] = session["peakPower_W"]  # rename
        self.rows.append(temp_df)  


class FeatureCreation(BaseEstimator, TransformerMixin):
    """
    This pipeline step will create an "energy_demand_kWh" and "peak_power_W" column. 
    The name of the dataframe's index will be set to "time", and "day" and "month" columns 
    will be created. 
    """
    
    def fit(self, X, y=None):
        return self 

    @ staticmethod
    def transform(X) -> pd.DataFrame:
        X["energy_demand_kWh"] = (X["avg_power_demand_W"]/1000)/12
        # for the highest granularity, peak power is equal to the power demand
        # (different for different granularities though)
        X["peak_power_W"] = X["avg_power_demand_W"] 
        X.index.name = "time"
        X["day"] = X.index.day_name()
        X["month"] = X.index.month_name()
        return X


class SaveToCsv(BaseEstimator, TransformerMixin):
    """
    This pipeline step takes each dataframe and creates new granularities
    (hourly, daily, and monthly). Each dataframe is saved to a "data/" file. 
    """
    def __init__(self) -> None:
        self.agg_key = {
            "avg_power_demand_W": "mean",
            "energy_demand_kWh": "sum",
            "peak_power_W": "max",
            "day": "first",
            "month": "first"
        }
        self.dataframe_names = [
            "fivemindemand", 
            "hourlydemand", 
            "dailydemand", 
            "monthlydemand"
        ]
        super().__init__()

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        # create new granularities
        hourlydemand = X.resample("1H").agg(self.agg_key)
        dailydemand = X.resample("24H").agg(self.agg_key)
        monthlydemand = X.resample("1M").agg(self.agg_key)

        new_dataframes = {
            "fivemindemand": X, 
            "hourlydemand": hourlydemand, 
            "dailydemand": dailydemand, 
            "monthlydemand": monthlydemand
        }

        # save to file system
        for idx, dataframe in enumerate(new_dataframes.values()):
            dataframe.to_csv(f"data/{self.dataframe_names[idx]}.csv")
        return new_dataframes
