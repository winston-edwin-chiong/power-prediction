import redis 
import os
import pandas as pd 
from io import BytesIO
from dotenv import load_dotenv

#! RUN THIS SCRIPT TO UPDATE THE DATA IN THE DATA FOLDER

load_dotenv()
r = redis.Redis(connection_pool=redis.ConnectionPool.from_url(url=os.getenv("REDIS_URI")))

def get_df(redis_client: redis.Redis, name) -> pd.DataFrame:
    """
    Get a dataframe from Redis.
    """
    buffer = redis_client.get(name)
    result = pd.read_parquet(BytesIO(buffer))
    return result


print("Fetching data...")

dataframes = ["raw_data", "fivemindemand", "hourlydemand", "dailydemand", "monthlydemand"]

for df in dataframes:
    print("Fetching data for", df, "...")
    result = get_df(r, df)

    print("Saving data", df, "...")
    result.to_csv(f"data/{df}.csv")

print("Done!")
