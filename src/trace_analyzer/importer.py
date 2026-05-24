from pathlib import Path

from pandas import DataFrame
import pandas as pd


class Importer:
    @staticmethod
    def import_csv(input_path: str | Path) -> DataFrame:
        return pd.read_csv(input_path)
