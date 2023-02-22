from datacleaning.FetchData import FetchData
from datacleaning.CleanData import CleanData

print("Fetching data...")
raw_data = FetchData.scan_save_all_records()

print("Cleaning data...")
cleaned_df = CleanData.clean_save_raw_data(raw_data)

print("Done!")
