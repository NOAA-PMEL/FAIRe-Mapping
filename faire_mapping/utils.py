import pandas as pd
import gspread #library that makes it easy for us to interact with the sheet
from google.oauth2.service_account import Credentials
from datetime import datetime

# TODO: outline keys that need to be present in the google_sheet_json_cred (see credentials.json to speicify how it should look)
def load_google_sheet_as_df(google_sheet_id: str, sheet_name: str, header: int, google_sheet_json_cred: str) -> pd.DataFrame:
        """
        Load a google sheet as a data frame. The google_sheet_json_cred is the path to the credentials.json file 
        with credentials for acessing google sheets programatically."""

        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        creds = Credentials.from_service_account_file(google_sheet_json_cred, scopes=scopes)
        client = gspread.authorize(creds)

        sheet = client.open_by_key(google_sheet_id)
        worksheet = sheet.worksheet(sheet_name)
        
        # Get all values
        values = worksheet.get_all_values()
        headers = values[header]
        data_rows = values[header+1:]
        df = pd.DataFrame(data_rows, columns=headers)
        
        return df

def load_csv_as_df(file_path: str, header=0, sep=',') -> pd. DataFrame:
        # Load csv files as a data frame
        return pd.read_csv(file_path, header=header, sep=sep)

def fix_cruise_code_in_samp_names(df: pd.DataFrame, unwanted_cruise_code: str, desired_cruise_code: str, sample_name_col: str = None) -> pd.DataFrame:
        """
        Fixes the cruise code in the sample names for a data frame by specifying the sample name column. 
        Needed to hard code .DY20-12 cruise codes in there, can remove when EcoFOCI data is submitted.
        """
        if desired_cruise_code == '.DY20-12': # since the extraction sheet used .DY20 and teh sample metadata use .DY2012 need to replace for both
                mask = (df[sample_name_col].str.endswith('.DY20')) | (df[sample_name_col].str.endswith(unwanted_cruise_code))
                # Apply the replacement only to the rows where the mask is True
                df.loc[mask, sample_name_col] = df.loc[mask, sample_name_col].str.replace(
                unwanted_cruise_code, 
                desired_cruise_code
                )
        elif not unwanted_cruise_code: # If not unwanted cruise code, just append desired cruise code onto saple name
             df[sample_name_col] = df[sample_name_col].apply(lambda x: x if str(x).endswith(desired_cruise_code) else str(x) + desired_cruise_code)
        else: # everything else just replaces with the desired cruise code
                df[sample_name_col] = df[sample_name_col].str.replace(unwanted_cruise_code, desired_cruise_code)

        return df 

def str_replace_for_samps(samp_name: pd.Series) -> str:
        """
        Fixes sample names - specific to OME with sample name mismatches
        if sample is part of the DY2012 cruise, will replace any str of DY20 with DY2012
        """
        samp_name = str(samp_name)
        if '_' in samp_name and 'pool' not in samp_name: # osu samp names have _ that needs to be . For example, E62_1B_DY20. Pooled samples keep the underscore
            samp_name = samp_name.replace('_', '.')
        if '.DY20' in samp_name:
           samp_name = samp_name.replace('.DY20', '.DY20-12')
        if '(P10 D2)' in samp_name:
            samp_name = samp_name.replace(' (P10 D2)', '') # for E1875.OC0723 (P10 D2) in Run2
        if '.IB.NO20' in samp_name: # for E265.1B.NO20 sample - in metadata was E265.1B.NO20
            return samp_name.replace('.IB.NO20', '.1B.NO20-01')
        if 'E.2139.' in samp_name:
            return samp_name.replace('E.2139.', 'E2139.') # For E239 QiavacTest, had a . between E and number in metadata
        if 'E687' in samp_name:
            return samp_name.replace('E687', 'E687.WCOA21')
        if '.NC' in samp_name: # If an E was put in front of an NC sample (this happends in some of the extractions e.g. the SKQ21 extractions), will remove the E
            samp_name = samp_name.replace('E.', '')
        if '*' in samp_name:
            samp_name = samp_name.replace('*','')
        if '.SKQ2021' in samp_name:
            samp_name = samp_name.replace('.SKQ2021', '.SKQ21-15S')
        if '.NO20' in samp_name:
            samp_name = samp_name.replace('.NO20', '.NO20-01')
        if 'Mid.NC.SKQ21' in samp_name:
            samp_name = samp_name.replace('Mid.NC.SKQ21', 'MID.NC.SKQ21-15S')
        if '.DY2206' in samp_name:
            samp_name = samp_name.replace('.DY2206', '.DY22-06')
        if '.DY2209' in samp_name:
            samp_name =  samp_name.replace('.DY2209', '.DY22-09')
        if '.DY2306' in samp_name:
            samp_name =  samp_name.replace('.DY2306', '.DY23-06')
        if 'E2030.NC' == samp_name:
            samp_name = 'E2030.NC.SKQ23-12S'

        return samp_name

def convert_mdy_date_to_iso8061(date_string: str) -> str:
        """
        Converts a date string from Month/Day/Year format to ISO 8601 (YYYY-MM-DD).

        This function handles both 'M/D/YYYY' and 'M/YYYY' formats. If only a 
        month and year are provided, the day is assumed to be the 1st of the month.
        The function strips whitespace and handles single-digit months/days by 
        padding them with leading zeros.

        Args:
                date_string (str): The date string to convert (e.g., "6/15/2021" 
                or "06/2021").

        Returns:
                str: The formatted date string in 'YYYY-MM-DD' format.
                None: Returns None (implicit) if an error occurs during conversion, 
                after printing the error message.

        Raises:
                ValueError: If the date_string does not contain exactly two or 
                three parts when split by '/'.
        """
        date_string = str(date_string).strip()

        # Handle both single and double digit formats
        try:
            parts = date_string.split('/')
            if len(parts) == 3:
                month, day, year = parts

                formatted_date = f"{int(month):02d}/{int(day):02}/{year}"

                # parse date string
                date_obj = datetime.strptime(formatted_date, "%m/%d/%Y")

                # convert to iso 8601 format
                return date_obj.strftime('%Y-%m-%d')

            elif len(parts) == 2:
                # hande month/year format by assuming day=1
                month, year = parts
                formatted_date = f"{int(month):02d}/01/{year}"

                # parse date string
                date_obj = datetime.strptime(formatted_date, "%m/%d/%Y")

                # convert to iso 8601 format
                return date_obj.strftime('%Y-%m-%d')
            else:
                raise ValueError(
                    f"Date doesn't have two or three parts: {date_string}")

        except Exception as e:
                # Print the error for debugging
                print(f"Error converting {date_string}: {str(e)}!")
                return date_string