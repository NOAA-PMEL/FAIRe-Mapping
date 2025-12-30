import pandas as pd
from faire_mapping.utils import load_google_sheet_as_df, load_csv_as_df

class BaseDfBuilder:
    """
    Loads data from a csv file or a google sheet into a pandas data frame.
    """
    def __init__(self,
                 header: int = 0, 
                 sep: str = ',',
                 csv_path: str = None,
                 google_sheet_id: str = None, 
                 json_creds_path: str = None,
                 sheet_name: str = None):
        """"
        Include either a csv_path or google_sheet_id. If google_sheet_id then must include json_creds_path and sheet_name
        """
        self.csv_path = csv_path
        self.google_sheet_id = google_sheet_id
        self.json_creds_path = json_creds_path
        self.sheet_name = sheet_name
        self.header = header
        self.sep = sep

        # Validation
        if not csv_path and not google_sheet_id:
            raise ValueError("Must provide either a csv path or a google sheet id!")
        
        if csv_path and google_sheet_id:
            raise ValueError("Provide only one csv_path or google_sheet_id!")
        
        if google_sheet_id and not json_creds_path:
            raise ValueError("Must provide the path to the json google API credentials!")
        
        if google_sheet_id and not sheet_name:
            raise ValueError("Must provide the name of the google sheet when providing a google_sheet_id!")
        
        self.df = self._load_data()
        
    def _load_data(self) -> pd.DataFrame:
        """
        Load data based on provided source.
        """
        if self.csv_path:
            return load_csv_as_df(file_path=self.csv_path, header=self.header, sep=self.sep)
        else:
            return load_google_sheet_as_df(google_sheet_id=self.google_sheet_id, sheet_name=self.sheet_name, header=self.header, google_sheet_json_cred=self.json_creds_path)