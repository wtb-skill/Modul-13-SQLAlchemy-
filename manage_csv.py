import pandas as pd


class ManageCSV:
    def __init__(self, file):
        self.file = file
        self.data = self._create_dataframe()

    def _create_dataframe(self):
        try:
            data = pd.read_csv(self.file)
            return data
        except FileNotFoundError:
            print(f"File '{self.file}' not found. Please provide a valid file path.")
            return None

