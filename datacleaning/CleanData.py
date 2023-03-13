from sklearn.pipeline import Pipeline
import pandas as pd
import datacleaning.fullcleaningclasses as fcc
import datacleaning.sessionlevelcleaningclasses as sc

class CleanData:

    def __init__(self):
        pass

    @staticmethod
    def clean_save_raw_data(raw_data):

        # load data
        __raw_data = pd.read_csv("data/raw_data.csv")

        # full time series pipeline
        full_ts_pipeline = Pipeline(
            [
                ("sort_drop_cast", fcc.SortDropCast()),
                ("create_helpers", fcc.HelperFeatureCreation()),
                ("create_session_TS", fcc.CreateSessionTimeSeries()),
                ("create_features", fcc.FeatureCreation()),
                ("save_to_csv", fcc.SaveToCsv()),
            ]
        )
        cleaned_df = full_ts_pipeline.fit_transform(raw_data)

        # session level pipeline
        session_lvl_pipeline = Pipeline(
            [
                ("sort_drop_cast", sc.SortDropCast()),
                ("create_helpers", sc.HelperFeatureCreation()),
                ("nested_ts", sc.CreateNestedSessionTimeSeries()),
                ("resample", sc.ResampleTimeSeries()),
                ("save_csv", sc.SaveToCsv())
            ]
        )
        cleaned_df["session"] = session_lvl_pipeline.fit_transform(raw_data)
        cleaned_df["raw_data"] = raw_data

        return cleaned_df
