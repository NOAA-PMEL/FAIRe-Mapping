import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import yaml
# Will need to update to only add new data and not clear the whole sheet eventually.

class GoogleSheetConcatenator:

    def __init__(self, root_dir, metadata_type: str, credentials='/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json', destination_sheet_id='1askd-wDorl-YVh7jk6vBEVtzGMLKp9oXGC7k-SqSrtc'):
        """
        root dir: The data root dir with the *_faire.csv files to be concatentated
        credentials: path to json file with google auth credentials
        metadaata_type: either sampleMetadata or experimentRunMetadata exactly
        destination_sheet_id: default sheet for all the metadata
        """
        self.root_dir = root_dir
        self.creds = credentials
        self.destination_sheet_id = destination_sheet_id
        self.destination_sheet_name = metadata_type
        self.client = self.setup_google_client()
        self.combined_df = self.concat_csv_files()
        self.write_data_to_master_sheet()

    def concat_csv_files(root_dir: str):

        csv_files = list(root_dir.rglob("*_faire.csv"))
        df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

        return df

    def setup_google_client(self):
        # Set up credentials
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_file(
            self.creds, scopes=scopes)
        client = gspread.authorize(creds)
        return client

    def write_data_to_master_sheet(self):
        """Write the combined sheet data to the master destination sheet"""
        # load destination sheet and clear
        dest_spreadsheet = self.client.open_by_key(self.destination_sheet_id)
        combined_sheet = dest_spreadsheet.worksheet(
            self.destination_sheet_name)
        combined_sheet.clear()

        # Convert dataframe to list format (without index)
        data_to_write = [self.combined_df.columns.values.tolist()] + \
            self.combined_df.fillna('').values.tolist()

        # Write data
        combined_sheet.update(values=data_to_write, range_name='A1')

        # Freeze header and first column
        combined_sheet.freeze(rows=1, cols=1)
        print(f"✅ Done! {dest_spreadsheet.url}")
