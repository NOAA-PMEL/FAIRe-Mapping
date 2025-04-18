from pathlib import Path
import openpyxl
from openpyxl.utils import get_column_letter
import pandas as pd
# import difflib
import yaml
from .custom_exception import ControlledVocabDoesNotExistError
import gspread #library that makes it easy for us to interact with the sheet
from google.oauth2.service_account import Credentials
# import requests
# from bs4 import BeautifulSoup

# TODO: Fix sample name - either map from Sample column or add PCR back into Sample_Name column, and a technical replicates column
# TODO: double check neg_cont_type logic after Sean and Zack add these into the run metadata?
# TODO: add pos_cont_type logic after Sean and Zack and positive control to run metadata?
# TODO: check controls and make sure the metadata make sense for those (e.g. samp_collect_device - would this be a CTD Niskin?)
# field_negatives are sterile RO water pre-filled and sealed prior to sample - so a lot of this metadata does not make sense for it.
# TODO: Which other columns should we include that were not included: e.g. Cast_No, Rosette_position, Station, Cruise_ID_short, Cruise_ID_long, sites, technical_reps
# TODO: Split into on sheet per cruise - or keep as one file because in Luke's template he has added assay columns in the Project Metadata (to account for different assays across cruises)
# TODO: add logic for env_local_scale based on depth
# TODO: maybe try to figure out logic for mapping two columns to one (e.g. silicate stuff) in a better way. But will this ever happen again? Happening cause of combining cruise data
# TODO: Fix GeoLoc to pull in location in R scripts (I think I saw Bering/Chulchi Sea somehwere in that data) and add Arctic Ocean in main.
# TODO: add blanks to metadata sheet (not sure if this can be pulled in somewhere besides the run1_4_revamp_metadata)
# TODO: transorm Extraction Date to meet standard
# TODO Someday: Get faire_missing_values from parsing webpage using BeautifulSoup (to allow for changes: https://fair-edna.github.io/guidelines.html#missing-values)
class OmeFaireMapper:
    
    def __init__(self, config_yaml: str):

        self.config_file = self.load_config(config_yaml)
        self.google_sheet_json_cred = self.config_file['json_creds']

        self.mapping_file_FAIRe_column = 'faire_field'
        self.mapping_file_metadata_column = 'source_name_or_constant'
        self.mapping_file_mapped_type_column = 'mapping'
        
        self.drop_down_value_df = self.load_faire_template_as_df(file_path=self.config_file['faire_template_file'], sheet_name='Drop-down values', header=0)
        # self.faire_unit_column_dict = {term: term.replace('_unit', '') for term in self.mapping_df[self.mapping_file_FAIRe_column].values if '_unit' in term}
        self.final_faire_template_path = self.config_file['final_faire_template_path']
        self.sheet_header = 2

        self.exact_mapping = 'exact'
        self.related_mapping = 'related'
        self.constant_mapping = 'constant'
        self.faire_missing_values = ["not applicable: control sample",
                                     "not applicable: sample group",
                                     "not applicable",
                                     "missing: not collected: synthetic construct",
                                     "missing: not collected: lab stock",
                                     "missing: not collected: third party data",
                                     "missing: not collected",
                                     "missing: not provided",
                                     "missing: restricted access: endangered species", 
                                     "missing: restricted access: human-identifiable", 
                                     "missing: restricted access"
                                     ]

    def load_config(self, config_path):
        # Load configuration yaml file
    
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
            
    def load_faire_template_as_df(self, file_path: Path, sheet_name: str, header: int) -> pd.DataFrame:
        # Load FAIRe excel template as a data frame based on the specified template sheet name
        
        return pd.read_excel(file_path, sheet_name=sheet_name, header=header)
    
    def load_csv_as_df(self, file_path: Path, header=0) -> pd. DataFrame:
         # Load csv files as a data frame
         
         return pd.read_csv(file_path, header=header)
    
    def load_google_sheet_as_df(self, sheet_id: str, sheet_name: str, header: int) -> pd.DataFrame:

        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        creds = Credentials.from_service_account_file(self.google_sheet_json_cred, scopes=scopes)
        client = gspread.authorize(creds)

        sheet_id = '1PKYStbZN3ygUvjXi9SmaXGwmwt-eJCZyDCZ8xzlQq0E'
        sheet = client.open_by_key(sheet_id)

        worksheet = sheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        headers = data.pop(header)
        
        df = pd.DataFrame(data, columns=headers)
        
        return df
    
    def str_replace_dy2012_cruise(self, samp_name: pd.Series) -> str:
        # if sample is part of the DY2012 cruise, will replace any str of DY20 with DY2012
        samp_name = str(samp_name)
        if '.DY20' in samp_name:
           samp_name = samp_name.replace('.DY20', '.DY2012')
    
        return samp_name
    
    def extract_controlled_vocab(self, faire_attribute: str) -> list:
        
        # filter dataframe by the FAIRe attribute and get all allowable terms
        filtered_row = self.drop_down_value_df[self.drop_down_value_df['term_name'] == faire_attribute]
        # Get a list of columns that have the vocabulary in them
        vocab_columns = [col for col in filtered_row.columns if ('vocab' in col and '_' not in col)]
        # Get a list of the controlled vocabulary by FAIRe attribute term name
        controlled_vocab = [vocab_word for vocab_word in filtered_row[vocab_columns].values.flatten() if pd.notna(vocab_word)]

        return controlled_vocab
    
    
    def check_cv_word(self, value: str, faire_attribute: str) -> dict:
        # Check a word in a list of the controlled voabulary for a FAIRe attribute to see if it exists (accounts for any updates)
        # and if it does, will add to the new row.
        
        controlled_vocab = self.extract_controlled_vocab(faire_attribute=faire_attribute)

        # make static_value a list (to account for more than one value as the static_value)
        value = value.split(' | ') if '|' in value else [value]

        for word in value:
            if word not in controlled_vocab and word not in self.faire_missing_values:
                raise ControlledVocabDoesNotExistError(f'The following {faire_attribute} does not exist in the FAIRe standard controlled vocabulary: {word}, the allowed values are {controlled_vocab}')
            else:
                new_value = ' | '.join(value)
                return new_value
    

    def apply_exact_mappings(self, metadata_row, faire_col):

        if faire_col in self.drop_down_value_df['term_name'].values:
            metadata_row = self.check_cv_word(value=metadata_row, faire_attribute=faire_col)
        else:
            return metadata_row

        return metadata_row
    

    def apply_static_mappings(self, faire_col: str, static_value) -> dict:
        # returns static_value for row
        # TODO: need to adjust this for controls that weren't necessarily collected in the wild (e.g. habitat_natural_articicial_0_1, 
        # geo_loc_name?, env scales, etc.)

        # check controlled vocabulary if column uses controlled vocabulary
        if faire_col in self.drop_down_value_df['term_name'].values:
            static_value = self.check_cv_word(value=static_value, faire_attribute=faire_col)
        # elif faire_col == 'geo_loc_name':
        #     self.check_and_add_geo_loc(formatted_geo_loc=static_value, new_row=new_row, faire_col=faire_col)
        return static_value

    # def normalize_unit_col_names(self, column_name: str) -> str:
    #     # preprocessing to column names with units in them. 

    #     # map u's to µ's
    #     mu_map = {
    #         'u': 'µ'
    #     }
    #     # split column name by underscores
    #     if '_' in column_name:
    #         column_split = column_name.split('_', -1)
    #     # replace any u's with µ's and use in last word of split
    #     normalized_unit_word = ''.join(mu_map.get(u, u) for u in column_split[-1]).lower()

    #     return normalized_unit_word

    # def add_units_to_unit_row_based_on_column_name(self, new_row: dict):
    #     # Adds units to new FAIRe unit attributes if the metadata column name specifies the unit (should be "derived")
    #     # Assumes that all unit related values have already been mapped over; probably should do last step of mapping
    #     #TODO Add Error statements
    #     cutoff = 0.6

    #     for faire_col, metadata_col in self.mapping_dict['derived'].items():

    #         if  'unit' in faire_col:
            
    #              # if the corressponding value column to the unit column is not NA, then proceed to get units
    #              # checks the new row, because this assumes that the corresponding unit column values have already been mapped over
    #             if pd.notna(new_row[self.faire_unit_column_dict.get(faire_col)]):

                   
    #                 allowed_units = self.extract_controlled_vocab(faire_col)

    #                 # need to conver allowed_units to lowercase for better match. This will allow us to map back
    #                 lower_to_original = {u.lower(): u for u in allowed_units}

    #                 # allowed_units will be empty for user_defined unit fields. Can only use matching with existing FAIRe attributes
    #                 if allowed_units:
    #                     normalized_col_name = self.normalize_unit_col_names(metadata_col)
    #                     matches = difflib.get_close_matches(normalized_col_name, [unit.lower() for unit in allowed_units], n=1, cutoff=cutoff)
    #                     new_row[faire_col] = [lower_to_original[u] for u in matches][0]
    #                 # if allowed_units is empty, probably means its a user defined unit attribute - will add through specified static column
    #                 else:

    #                     self.add_static_values(new_row=new_row, faire_col=faire_col)    
                                            
    #     return new_row
    

    def add_final_df_to_FAIRe_excel(self, sheet_name: str, faire_template_df: pd.DataFrame):

        # Step 1 load the workbook to preserve formatting
        workbook = openpyxl.load_workbook(self.config_file['faire_template_file'])
        sheet = workbook[sheet_name]
        
        # step 2: identify new columns added to the DataFrame
        original_columns = []
        for cell in sheet[3]: # Row 3 (0-indexed as 2) contains column names
            if cell.value:
                original_columns.append(cell.value)
        
        new_columns = [col for col in faire_template_df.columns if col not in original_columns]
            
        # new step 3: add new columns to the sheet headers
        last_col = sheet.max_column 
        for i, new_col in enumerate(new_columns, 1):
            col_idx = last_col + i
            col_letter = get_column_letter(col_idx)
            # add column name to row 3
            sheet[f'{col_letter}3'] = new_col
            # Add User defined to row 2
            sheet[f'{col_letter}2'] = 'User defined'

        for row in range(4, sheet.max_row + 1):
            for col in range(1, sheet.max_column + 1):
                sheet.cell(row=row, column=col).value = None
        
        # Write the data frame data to the sheet (starting at row 4)
        for row_idx, row_data in enumerate(faire_template_df.values, 4): 
            for col_idx, value in enumerate(row_data, 1):
                sheet.cell(row=row_idx, column=col_idx).value = value

        # step 4 save the workbook preserved with headers
        workbook.save(self.config_file['final_faire_template_path'])
       


        

       
    

    

