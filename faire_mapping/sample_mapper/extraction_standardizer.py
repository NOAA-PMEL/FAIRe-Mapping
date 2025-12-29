import pandas as pd

from faire_mapping.utils import load_google_sheet_as_df

# TODO: outline what extractions_info dict looks like from config.yaml files in documentions in __init__
class ExtractionStandardizer:

    # extraction_info keys (from config.yaml file
    EXTRACT_INFO_EXTRACTION_NAME_KEY = "extraction_name" # The name of the extraction (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_CRUISE_KEY = "extraction_cruise_key" #the name in the SampleName column of the extractions sheet to filter for the specific cruise (specified in the config.yaml)
    EXTRACT_INFO_EXTRACTION_SAMP_NAME_COL_KEY = "extraction_sample_name_col" # the name of the column in the extraction google sheet for the sample name (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_CONC_COL_NAME_KEY = "extraction_conc_col_name" # The name of the column in the extraction google sheet for the extraction concentration (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_DATE_COL_NAME_KEY = "extraction_date_col_name" # The name of the column in the extraction google sheet for the date of extraction (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_METADATA_SHETT_NAME_KEY = "extraction_metadata_sheet_name" # The name of the google sheet, sheet (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_BLANK_VOL_WE_DNA_EXT_KEY = "extraction_blank_vol_we_dna_ext" # The volume or mass of blanks extracted if there are blanks extracted (specified in the config.yaml)
    EXTRACT_INFO_EXTRACTION_SET_GROUPING_COL_NAME = "extraction_set_grouping_col_name" # The name of the column that groups the samples together if they were extracted together (specified in the config.yaml)

    # common extraction columns created during extraction manipulation. These are the column names in the final extraction df that will be fed into the rest of the sample mapper
    EXTRACT_SAMP_NAME_COL = "samp_name"
    EXTRACT_CONC_COL = "extraction_conc"
    EXTRACT_DATE_COL = "extraction_date"
    EXTRACT_SET_COL = "extraction_set"
    EXTRACT_CRUISE_KEY_COL = "extraction_cruise_key"
    EXTRACT_NAME_COL = "extraction_name"
    EXTRACT_BLANK_VOL_WE_DNA_EXT_COL = "extraction_blank_vol_dna_ext"
    EXTRACT_ID_COL = 'extract_id'

    def __init__(self, extractions_info: dict, google_sheet_json_cred: str):
        """
        extractions_info is the extractions dictionary from the config.yaml file that outlines the extraction spreadsheet(s)
        google_sheet_json_cred is the path to the where the credentialss.json file lives for acessing google sheet programatically. Will be specified in the config.yaml file
        """
        self.extractions_info = extractions_info
        self.google_sheet_json_cred = google_sheet_json_cred
        self.extract_new_old_col_mapping_dict = self.create_extract_old_new_col_master_mapping_dict() # Creates a dictionary with new extraction column names as keys and a set of old extraction column names as values
        
    def create_concat_extraction_df(self) -> pd.DataFrame:
        # Concatenate extractions together and create common column names

        extraction_dfs = []
        # loop through extractions and append extraction dfs to list
        for extraction in self.extractions_info:

            
            extraction_df = load_google_sheet_as_df(google_sheet_id=extraction['extraction_metadata_google_sheet_id'], sheet_name=extraction['extraction_metadata_sheet_name'], header=0)

            # change necessary column names so they will match across extraction dfs and add additional info
            extraction_df = extraction_df.rename(columns=extraction_column_mappings)
            extraction_df[self.EXTRACT_CRUISE_KEY_COL] = extraction.get(self.EXTRACT_INFO_EXTRACTION_CRUISE_KEY)
            extraction_df[self.EXTRACT_BLANK_VOL_WE_DNA_EXT_COL] = extraction.get(self.EXTRACT_INFO_EXTRACTION_BLANK_VOL_WE_DNA_EXT_KEY)
            extraction_df[self.EXTRACT_NAME_COL] = extraction.get(self.EXTRACT_INFO_EXTRACTION_NAME_KEY)

            extraction_df[self.EXTRACT_ID_COL] = extraction_df[self.EXTRACT_NAME_COL].astype(str).replace(r'\s+', '_', regex=True) + "_" + extraction_df[self.EXTRACT_SET_COL].astype(str).replace(r'\s+', '_', regex=True)
            extraction_dfs.append(extraction_df)

        # Concat dataframes
        final_extraction_df = pd.concat(extraction_dfs)

        # if any columns changed names that are in the mapping dict, change them there too
        for maps in self.mapping_dict.values():
            for faire_col, metadata_col in maps.items():
                if metadata_col in extraction_column_mappings.keys():
                    maps[faire_col] = extraction_column_mappings.get(metadata_col)
        
        if self.unwanted_cruise_code and self.desired_cruise_code and self.unwanted_cruise_code != '.NO20': # this gets updates twice because unwanted cruise code is in the desired cruise code so will update later
            final_extraction_df = self.fix_cruise_code_in_samp_names(df=final_extraction_df, sample_name_col=self.EXTRACT_SAMP_NAME_COL)
     
        return final_extraction_df
    
    def create_extract_old_new_col_master_mapping_dict(self) -> dict:
        """
        Creates a master dictionary with new column names as keys (see constants at beginning
        of class), and a set of old column names that map to the new columns names.
        e.g. {"samp_name": ("sample_name", "SAMP_name")}
        """
        extract_col_mapping_dict = {
            self.EXTRACT_SAMP_NAME_COL: set(),
            self.EXTRACT_CONC_COL: set(),
            self.EXTRACT_DATE_COL: set(),
            self.EXTRACT_SET_COL: set()
            }
        for extraction in self.extractions_info:
            extract_col_mapping_dict[self.EXTRACT_SAMP_NAME_COL].add(extraction[self.EXTRACT_INFO_EXTRACTION_SAMP_NAME_COL_KEY])
            extract_col_mapping_dict[self.EXTRACT_CONC_COL].add(extraction[self.EXTRACT_INFO_EXTRACTION_CONC_COL_NAME_KEY])
            extract_col_mapping_dict[self.EXTRACT_DATE_COL].add(extraction[self.EXTRACT_INFO_EXTRACTION_DATE_COL_NAME_KEY])
            extract_col_mapping_dict[self.EXTRACT_SET_COL].add(extraction[self.EXTRACT_INFO_EXTRACTION_SET_GROUPING_COL_NAME])

        return extract_col_mapping_dict
    
    def standardize_extraction_df_col_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes one data frame (an extraction df), and uses the """