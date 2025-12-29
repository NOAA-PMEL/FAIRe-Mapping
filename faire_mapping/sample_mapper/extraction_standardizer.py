import pandas as pd

# TODO: outline what extractions_info dict looks like from config.yaml files in documentions in __init__
class ExtractionStandardizer:

    # extractions_info key names:
    

    # common extraction columns created during extraction manipulation
    EXTRACT_SAMP_NAME_COL = "samp_name"
    EXTRACT_CONC_COL = "extraction_conc"
    EXTRACT_DATE_COL = "extraction_date"
    EXTRACT_SET_COL = "extraction_set"
    EXTRACT_CRUISE_KEY_COL = "extraction_cruise_key"
    EXTRACT_NAME_COL = "extraction_name"
    EXTRACT_BLANK_VOL_WE_DNA_EXT_COL = "extraction_blank_vol_dna_ext"
    EXTRACT_ID_COL = 'extract_id'

    def __init__(self, extractions_info: dict):
        """
        extractions_info is the extractions dictionary from the config.yaml file that outlines the extraction spreadsheet(s)
        """
        self.extractions_info = extractions_info


    def create_concat_extraction_df(self) -> pd.DataFrame:
        # Concatenate extractions together and create common column names

        extraction_dfs = []
        # loop through extractions and append extraction dfs to list
        for extraction in self.extractions_info:
            # mappings from config file to what we want to call it so it has a common name 
            extraction_column_mappings = {extraction['extraction_sample_name_col']: self.EXTRACT_SAMP_NAME_COL,
                                         extraction['extraction_conc_col_name']: self.EXTRACT_CONC_COL,
                                         extraction['extraction_date_col_name']: self.EXTRACT_DATE_COL,
                                         extraction['extraction_set_grouping_col_name']: self.EXTRACT_SET_COL,}
            
            extraction_df = self.load_google_sheet_as_df(google_sheet_id=extraction['extraction_metadata_google_sheet_id'], sheet_name=extraction['extraction_metadata_sheet_name'], header=0)

            # change necessary column names so they will match across extraction dfs and add additional info
            extraction_df = extraction_df.rename(columns=extraction_column_mappings)
            extraction_df[self.EXTRACT_CRUISE_KEY_COL] = extraction.get('extraction_cruise_key')
            extraction_df[self.EXTRACT_BLANK_VOL_WE_DNA_EXT_COL] = extraction.get('extraction_blank_vol_we_dna_ext')
            extraction_df[self.EXTRACT_NAME_COL] = extraction.get('extraction_name')

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