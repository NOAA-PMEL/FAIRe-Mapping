import pandas as pd

from faire_mapping.utils import load_google_sheet_as_df, str_replace_for_samps, fix_cruise_code_in_samp_names, convert_mdy_date_to_iso8061

# TODO: update extraction_mapping_dict in sample_mapper part
# TODO: outline what extractions_info dict looks like from config.yaml files in documentions in __init__
# TODO: in filter_cruise_avg_extraction_conc method, unhardcode the extraction_method_additional to be more flexible for other reasons.
class ExtractionStandardizer:
    """
    Prepares the extraction google sheets applicable to a cruise (or batch of samples being standardized
    together) by concatenating together if multiple extraction sheets provide in config.yaml file (e.g. extraction_info)
    standardizing dates, sample names a bit, calculating concentration averages if samples extractions were pooled, and creates
    a separate blanks_df, as well as a extraction_blank_rel_cont_dict that includes samples associated with the blanks
    """

    # extraction_info keys (from config.yaml file
    EXTRACT_INFO_EXTRACTION_NAME_KEY = "extraction_name" # The name of the extraction (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_CRUISE_KEY = "extraction_cruise_key" #the name in the SampleName column of the extractions sheet to filter for the specific cruise (specified in the config.yaml)
    EXTRACT_INFO_EXTRACTION_SAMP_NAME_COL_KEY = "extraction_sample_name_col" # the name of the column in the extraction google sheet for the sample name (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_CONC_COL_NAME_KEY = "extraction_conc_col_name" # The name of the column in the extraction google sheet for the extraction concentration (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_DATE_COL_NAME_KEY = "extraction_date_col_name" # The name of the column in the extraction google sheet for the date of extraction (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_METADATA_SHETT_NAME_KEY = "extraction_metadata_sheet_name" # The name of the google sheet, sheet (specified in the config.yaml file)
    EXTRACT_INFO_EXTRACTION_BLANK_VOL_WE_DNA_EXT_KEY = "extraction_blank_vol_we_dna_ext" # The volume or mass of blanks extracted if there are blanks extracted (specified in the config.yaml)
    EXTRACT_INFO_EXTRACTION_SET_GROUPING_COL_NAME_KEY = "extraction_set_grouping_col_name" # The name of the column that groups the samples together if they were extracted together (specified in the config.yaml)
    EXTRACT_METADATA_GOOGLE_SHEET_ID_KEY = "extraction_metadata_google_sheet_id" # The name 
    EXTRACT_METADATA_SHEET_NAME_KEY = "extraction_metadata_sheet_name"

    # common extraction columns created during extraction manipulation. These are the column names in the final extraction df that will be fed into the rest of the sample mapper
    EXTRACT_SAMP_NAME_COL = "samp_name"
    EXTRACT_CONC_COL = "extraction_conc"
    EXTRACT_DATE_COL = "extraction_date"
    EXTRACT_SET_COL = "extraction_set"
    EXTRACT_CRUISE_KEY_COL = "extraction_cruise_key"
    EXTRACT_NAME_COL = "extraction_name"
    EXTRACT_BLANK_VOL_WE_DNA_EXT_COL = "extraction_blank_vol_dna_ext"
    EXTRACT_ID_COL = "extract_id"
    EXTRACT_METHOD_ADDITIONAL_COL = "extraction_method_additional"
    EXTRACT_POOL_NUM_COL = "pool_num"

    # Below Range standard value
    BELOW_RANGE_STD_VAL = "BDL"

    def __init__(self, extractions_info: dict, google_sheet_json_cred: str, unwanted_cruise_code: str, desired_cruise_code: str):
        """
        extractions_info is the extractions dictionary from the config.yaml file that outlines the extraction spreadsheet(s)
        google_sheet_json_cred is the path to the where the credentialss.json file lives for acessing google sheet programatically. Will be specified in the config.yaml file
        unwanted_cruise_code is the cruise code in the sample names that is not desireable (see config.yaml files)
        desired_cruise_code is the desired cruise in the sample names (see config.yaml file)
        google_sheet_mapping_file_id is the id of the google sheet mapping file
        """
        self.extractions_info = extractions_info
        self.google_sheet_json_cred = google_sheet_json_cred
        self.unwanted_cruise_code = unwanted_cruise_code
        self.desired_cruise_code = desired_cruise_code
        self.extract_new_old_col_mapping_dict = self.create_extract_old_new_col_master_mapping_dict() # Creates a dictionary with new extraction column names as keys and a set of old extraction column names as values
        self.extraction_df = self.create_finalized_extraction_df() # the standardized extraction_df that will be joined with the sample metadata df in other modules
        self.extraction_blank_rel_cont_dict = {} # Will get filled out in get_extraction_blanks_applicable_to_cruise_samps
        self.extraction_blanks_df = self.get_extraction_blanks_applicable_to_cruise_samps()

    def create_finalized_extraction_df(self) -> pd.DataFrame:
        """
        An orchestrator method of the various methods to create the final extraction df that will
        be joined with the sample df in another module.
        """ 
        # Step 1. Concatenate extraction dfs together and standardize column names
        concat_extraction_df = self.create_concat_extraction_df()

        # Step 2. Get pool_num and average concentration if multiple extractions of the same sample were taken
        pool_num_extract_df = self.filter_cruise_avg_extraction_conc(concated_extraction_df=concat_extraction_df)

        # Step 3. Fix sample names (# update samp name for DY2012 cruises (from DY20) and remove E numbers from any NC samples) and cruise codes
        pool_num_extract_df[self.EXTRACT_SAMP_NAME_COL] = pool_num_extract_df[self.EXTRACT_SAMP_NAME_COL].apply(str_replace_for_samps)
        if self.unwanted_cruise_code and self.desired_cruise_code:
            if 'NO20' not in self.unwanted_cruise_code: # NO20 is updated in str_replace_samps (can't remove it from there because the experimentRunMetadata uses it)
                pool_num_extract_df = fix_cruise_code_in_samp_names(df=pool_num_extract_df, sample_name_col=self.EXTRACT_SAMP_NAME_COL)

        # Step 4. update dates to iso8601 TODO: may need to adjust this for ones that are already in this format
        pool_num_extract_df[self.EXTRACT_DATE_COL] = pool_num_extract_df[self.EXTRACT_DATE_COL].apply(convert_mdy_date_to_iso8061)
    
    def create_concat_extraction_df(self) -> pd.DataFrame:
        # Concatenate extractions together and create common column names

        extraction_dfs = []
        # loop through extractions and append extraction dfs to list
        for extraction in self.extractions_info:

            extraction_df = load_google_sheet_as_df(google_sheet_id=extraction[self.EXTRACT_METADATA_GOOGLE_SHEET_ID_KEY], sheet_name=extraction[self.EXTRACT_METADATA_SHEET_NAME_KEY], header=0)

            # Update column names
            extraction_df = self.standardize_extraction_df_col_names(df=extraction_df)
            extraction_df[self.EXTRACT_CRUISE_KEY_COL] = extraction.get(self.EXTRACT_INFO_EXTRACTION_CRUISE_KEY)
            extraction_df[self.EXTRACT_BLANK_VOL_WE_DNA_EXT_COL] = extraction.get(self.EXTRACT_INFO_EXTRACTION_BLANK_VOL_WE_DNA_EXT_KEY)
            extraction_df[self.EXTRACT_NAME_COL] = extraction.get(self.EXTRACT_INFO_EXTRACTION_NAME_KEY)

            extraction_df[self.EXTRACT_ID_COL] = extraction_df[self.EXTRACT_NAME_COL].astype(str).replace(r'\s+', '_', regex=True) + "_" + extraction_df[self.EXTRACT_SET_COL].astype(str).replace(r'\s+', '_', regex=True)
            extraction_dfs.append(extraction_df)

        # Concat dataframes
        final_extraction_df = pd.concat(extraction_dfs)

        # if any columns changed names that are in the mapping dict, change them there too
        # for maps in self.mapping_dict.values():
        #     for faire_col, metadata_col in maps.items():
        #         if metadata_col in extraction_column_mappings.keys():
        #             maps[faire_col] = extraction_column_mappings.get(metadata_col)
        
     
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
            extract_col_mapping_dict[self.EXTRACT_SET_COL].add(extraction[self.EXTRACT_INFO_EXTRACTION_SET_GROUPING_COL_NAME_KEY])

        return extract_col_mapping_dict
    
    def standardize_extraction_df_col_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Takes one data frame (an extraction df), and uses the self.extract_new_old_col_mapping_dict
        to rename the columns.
        """
        for new_col, old_cols in self.extract_new_old_col_mapping_dict.items():
            for col in df.columns:
                if col in old_cols:
                    df = df.rename(columns={col: new_col})

        return df
    
    def extraction_avg_aggregation(self, extractions_df: pd.DataFrame):
        """
        Calculates the mean if more than one concentration per sample name.
        """

        # Keep Below Range
        if all(isinstance(conc, str) and ("below range" in conc.lower() or "br" == conc.lower() or "bdl" == conc.lower()) for conc in extractions_df):
            return self.BELOW_RANGE_STD_VAL

        # For everything else, convert to numeric and calculate mean
        numeric_series = pd.to_numeric(
            extractions_df, errors='coerce')  # Non numeric becomes NaN

        mean_value = numeric_series.mean()

        return round(mean_value, 2)
    
    def filter_cruise_avg_extraction_conc(self, concated_extraction_df: pd.DataFrame) -> pd.DataFrame:
        """
        If extractions have multiple measurements for extraction concentrations, calculates the avg.
        and creates a column called pool_num to show the number of samples pooled
        First filter extractions df for samples that contain the cruise key in their sample name.
        extract_method_additional_col is hard coded here. TODO: unhardcode if this may differ when being used.
        """
        cruise_key_mask = concated_extraction_df.apply(
            lambda row: str(row[self.EXTRACT_CRUISE_KEY_COL]) in str(row[self.EXTRACT_SAMP_NAME_COL])
            if pd.notna(row[self.EXTRACT_CRUISE_KEY_COL]) and pd.notna(row[self.EXTRACT_SAMP_NAME_COL])
            else False,
            axis = 1
        )
        
        # Then calculate average concentration
        extract_avg_df = concated_extraction_df[cruise_key_mask].groupby(
            self.EXTRACT_SAMP_NAME_COL).agg({
                self.EXTRACT_CONC_COL: self.extraction_avg_aggregation,
                **{col: 'first' for col in concated_extraction_df.columns if col != self.EXTRACT_SAMP_NAME_COL and col != self.EXTRACT_CONC_COL}
            }).reset_index()

        # Add pool_num column that shows how many samples were averaged for each group
        sample_counts = concated_extraction_df[cruise_key_mask].groupby(
            self.EXTRACT_SAMP_NAME_COL).size().reset_index(name=self.EXTRACT_POOL_NUM_COL)

        # merge sample_counts into dataframe
        extract_avg_df = extract_avg_df.merge(
            sample_counts, on=self.EXTRACT_SAMP_NAME_COL, how='left')

        # Add extraction_method_additional for samples that pooled more than one extract
        extract_avg_df[self.EXTRACT_METHOD_ADDITIONAL_COL] = extract_avg_df[self.EXTRACT_POOL_NUM_COL].apply(
            lambda x: "One sample, but two filters were used because sample clogged. Two extractions were pooled together and average concentration calculated." if x > 1 else "missing: not provided")
        
        return extract_avg_df
    
    def get_extraction_blanks_applicable_to_cruise_samps(self):
        """
        Get extraction blank df (applicable to RC0083 cruise)
        """

        blank_df = pd.DataFrame(columns=self.extraction_df.columns)

        grouped = self.extraction_df.groupby(
            self.EXTRACT_SET_COL
        )

        try:
            for extraction_set, group_df in grouped:
                # Check if any in the group contains the extraction cruise key
                has_cruise_samps = group_df.apply(
                    lambda row: str(row[self.EXTRACT_CRUISE_KEY_COL]) in str(row[self.EXTRACT_SAMP_NAME_COL]),
                    axis = 1
                ).any()
                
                if has_cruise_samps:
                    # find blank samples in this group ('Larson NC are extraction blanks for the SKQ23 cruise)
                    extraction_blank_samps = group_df[
                        (group_df[self.EXTRACT_SAMP_NAME_COL].str.contains('blank', case=False, na=False)) | 
                        (group_df[self.EXTRACT_SAMP_NAME_COL].str.contains('Larson NC', case=False, na=False))]
                    
                    try:
                        # get list of samples associated with blanks and put into dict for rel_cont_id
                        valid_samples = group_df[self.EXTRACT_SAMP_NAME_COL].tolist()
                        for sample in valid_samples:
                            if 'blank' in sample.lower() or 'Larson NC' in sample:
                                other_samples = [samp for samp in valid_samples if samp != sample]
                                # update cruise codes in sample names
                                if self.unwanted_cruise_code and self.desired_cruise_code:
                                    sample = sample.replace(self.unwanted_cruise_code, self.desired_cruise_code)
                                    other_samples = [samp.replace(self.unwanted_cruise_code, self.desired_cruise_code) for samp in other_samples]
                                self.extraction_blank_rel_cont_dict[sample] = other_samples
                    except:
                        raise ValueError("blank dictionary mapping not working!")


                    blank_df = pd.concat([blank_df, extraction_blank_samps])

            blank_df[self.EXTRACT_CONC_COL] = blank_df[self.EXTRACT_CONC_COL].replace(
                "BR", self.BELOW_RANGE_STD_VAL).replace("Below Range", self.BELOW_RANGE_STD_VAL).replace("br", self.BELOW_RANGE_STD_VAL)
        except:
            raise ValueError(
                "Warning: Extraction samples are not grouped, double check this")
        
        return blank_df
    