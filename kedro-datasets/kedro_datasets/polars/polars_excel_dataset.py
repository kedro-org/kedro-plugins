from typing import Dict, Any
from pathlib import Path
import polars as pl
import pandas as pd
import openpyxl
from kedro.io import AbstractDataset
from kedro.io.core import DatasetError

class PolarsExcelDataset(AbstractDataset[Dict[str, pl.DataFrame], Dict[str, pl.DataFrame]]):
    """
    Kedro Dataset for reading and writing multiple Polars DataFrames to/from an Excel file.

    Example:
        >>> dataset = PolarsExcelDataset("data.xlsx")
        >>> data = dataset.load()  # Returns a dictionary of Polars DataFrames
        >>> dataset.save({"sheet1": df1, "sheet2": df2})  # Saves multiple DataFrames to Excel
    """

    def __init__(self, filepath: str):
        """
        Initialize PolarsExcelDataset.

        Args:
            filepath (str): Path where the dataset will be stored.
        """
        self._filepath = Path(filepath)

    def _load(self) -> Dict[str, pl.DataFrame]:
        """Load multiple sheets into a dictionary of Polars DataFrames."""
        if not self._filepath.exists():
            raise DatasetError(f"File not found: {self._filepath}")

        try:
            # Use pandas to read all sheets
            pandas_data = pd.read_excel(self._filepath, sheet_name=None)
            # Convert each sheet to a Polars DataFrame
            return {sheet_name: pl.DataFrame(df) for sheet_name, df in pandas_data.items()}
        except Exception as e:
            raise DatasetError(f"Failed to load dataset: {e}")

    def _save(self, data: Dict[str, pl.DataFrame]) -> None:
        """Save multiple Polars DataFrames as different sheets."""
        if not isinstance(data, dict) or not all(isinstance(df, pl.DataFrame) for df in data.values()):
            raise DatasetError("Data must be a dictionary of Polars DataFrames.")

        try:
            # Convert Polars DataFrame to Pandas DataFrame and then save with openpyxl
            with pd.ExcelWriter(self._filepath, engine='openpyxl') as writer:
                for sheet_name, df in data.items():
                    if not isinstance(sheet_name, str) or len(sheet_name) > 31:
                        raise DatasetError(f"Invalid sheet name: {sheet_name}. Sheet names must be strings and <= 31 characters.")
                    df.to_pandas().to_excel(writer, sheet_name=sheet_name, index=False)
        except Exception as e:
            raise DatasetError(f"Failed to save dataset: {e}")

    def _exists(self) -> bool:
        """Check if the dataset exists."""
        return self._filepath.exists()

    def _describe(self) -> Dict[str, Any]:
        """Return dataset metadata."""
        return {
            "filepath": str(self._filepath),
            "exists": self._exists(),
        }
# Create sample data
df1 = pl.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
df2 = pl.DataFrame({"colA": [4, 5], "colB": ["x", "y"]})
data = {"sheet1": df1, "sheet2": df2}

# Save and load data
dataset = PolarsExcelDataset("data.xlsx")
dataset.save(data)
loaded_data = dataset.load()

print(loaded_data)