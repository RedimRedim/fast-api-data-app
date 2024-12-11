import pandas as pd
import os


class DataFrameUtils:
    @staticmethod
    def save_to_csv(dataframe, path, filename):
        try:
            path = os.path.join(path, filename + ".csv")
            dataframe.to_csv(path, index=False)
            print(f"{dataframe} saved to {path}")
        except Exception as e:
            print(f"Error saving dataframe to CSV: {e}")
