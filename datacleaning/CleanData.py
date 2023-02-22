from sklearn.pipeline import Pipeline
import pandas as pd
import datacleaning.fullcleaningclasses as pc
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
                ("sort_drop_cast", pc.SortDropCast()),
                ("create_helpers", pc.HelperFeatureCreation()),
                ("create_session_TS", pc.CreateSessionTimeSeries()),
                ("create_features", pc.FeatureCreation()),
                ("save_to_csv", pc.SaveToCsv()),
            ]
        )
        cleaned_df = full_ts_pipeline.fit_transform(raw_data)

        # session level pipeline
        session_lvl_pipeline = Pipeline(
            [
                ("sort_drop_cast", sc.SortDropCast()),
                ("create_helpers", sc.HelperFeatureCreation()),
                ("nested_ts", sc.CreateNestedSessionTimeSeries()),
                ("save_csv", sc.SaveCSV()),
            ]
        )
        cleaned_df["session"] = session_lvl_pipeline.fit_transform(raw_data)
        cleaned_df["raw_data"] = raw_data

        return cleaned_df
