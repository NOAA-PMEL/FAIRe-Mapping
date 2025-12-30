import pandas as pd
from faire_mapping.dataframe_builders.base_df_builder import BaseDfBuilder
from faire_mapping.utils import fix_cruise_code_in_samp_names

# TODO: _check_nc_samp_name_has_nc method is specific to OME for sample naming problems. When we standardize sample naming, can remove.
# TODO: _fix_samp_names specific to OME and for certain cruises. Can remove after data for these cruises has been submitted and sample naming becomes standardized. 
class SampleMetadataDfBuilder(BaseDfBuilder):
    """
    Builds the raw sample metadata data frame. Cleans some things and joins with the extraction df to complete the sample metadata df
    """
    EXTRACT_SAMP_NAME_COL = "samp_name"
    
    def __init__(self,
                 sample_name_metadata_col_name: str,
                 sample_metadata_file_neg_control_col_name: str,
                 sample_metadata_cast_no_col_name: str,
                 extraction_df: pd.DataFrame,
                 header: int = 0, 
                 sep: str = ',',
                 csv_path: str = None,
                 google_sheet_id: str = None, 
                 json_creds_path: str = None,
                 sheet_name: str = None,
                 unwanted_cruise_code: str = None,
                 desired_cruise_code: str = None):
        """
        sample_name_metadata_col_name is the name of the column in the sample metadata for the sample name (in the config.yaml)
        sample_metadata_file_neg_control_col_name is the name of the column in the sample metadata that specifies if its a negative control (specific to OME and in the config.yaml)
        sample_metadata_cast_no_col_name is the name of the column in the sample metadata file that refers to the cast (from the config.yaml file)
        unwanted_cruise_code is optional and is the cruise code in the sample names that needs to be changed (from the config.yaml).
        desired_cruise_code is optional and is the cruise code in the sample names that is desired (from the config.yaml)
        extraction_df is the extraction data frame (from the extraction_builder)
        extract_samp_name_col is the name of the sample name column in
        """
        
        super().__init__(header=header, sep=sep, csv_path=csv_path, google_sheet_id=google_sheet_id, json_creds_path=json_creds_path, sheet_name=sheet_name)

        self.sample_name_metadata_col_name = sample_name_metadata_col_name
        self.sample_metadata_file_neg_control_col_name = sample_metadata_file_neg_control_col_name
        self.sample_metadata_cast_no_col_name = sample_metadata_cast_no_col_name
        self.unwanted_cruise_code = unwanted_cruise_code
        self.desired_cruise_code = desired_cruise_code
        self.extraction_df = extraction_df
        self.sample_metadata_df, self.nc_metadata_df = self.filter_metadata_dfs()
        

    def filter_metadata_dfs(self):

        # Join sample metadata with extraction metadata to get samp_df
        samp_df = self.join_sample_and_extract_df()

        try:
            nc_mask = samp_df[self.sample_name_metadata_col_name].astype(
                str).str.contains('.NC', case=True)
            nc_df = samp_df[nc_mask].copy()
            samp_df_filtered = samp_df[~nc_mask].copy()

            # Replace any - with NaN
            nc_df = nc_df.replace('-', pd.NA)
            nc_df = nc_df.reset_index()
            samp_df_filtered = samp_df_filtered.replace('-', pd.NA)
            samp_df_filtered = samp_df_filtered.reset_index()
            
            return samp_df_filtered, nc_df
        
        except:
            print(
                "Looks like there are no negatives in the sample df, returning an empty nc_df")
            nc_df = pd.DataFrame()
            return samp_df_filtered, nc_df
    
    def join_sample_and_extract_df(self):
        """
        Join the extraction df with the sample df to create the finalized sample
        metadata df
        """
        samp_df = self.transform_metadata_df()

        metadata_df = pd.merge(
            left=self.extraction_df,
            right=samp_df,
            left_on=self.EXTRACT_SAMP_NAME_COL,
            right_on=self.sample_name_metadata_col_name,
            how='left'
        )

        # Drop rows where the sample name column value is NA. This is for cruises where samples were split up
        # e.g. PPS samples that were deployed from the DY2306 cruise. They will be a separate sample metadata file.
        metadata_df = metadata_df.dropna(
            subset=[self.sample_name_metadata_col_name])

        return metadata_df
    
    def transform_metadata_df(self):
        """
        Converts sample metadata to a data frame and checks to make sure NC samples have NC in the name (dy2206 had this problem)
        Also fixes sample names for SKQ23 cruise where sample metadata has _, but extraction metadata has -
        Add .NC to sample name if the Negative Control column is True and its missing (in DY2206 cruise)
        """
        self.df[self.sample_name_metadata_col_name] = self.df.apply(
            lambda row: self._check_nc_samp_name_has_nc(metadata_row=row),
            axis=1)

        # Fix sample names that have _ with -. For SKQ23 cruises where run and extraction metadata has -, and sample metadata has _
        self.df[self.sample_name_metadata_col_name] = self.df[self.sample_name_metadata_col_name].apply(
            self._fix_samp_names)

        # Remove 'CTD' from Cast_No. value if present
        self.df[self.sample_metadata_cast_no_col_name] = self.df[self.sample_metadata_cast_no_col_name].apply(
            self.remove_extraneous_cast_no_chars)
        
        if self.unwanted_cruise_code and self.desired_cruise_code:
            self.df = fix_cruise_code_in_samp_names(df=self.df, 
                                                             unwanted_cruise_code=self.unwanted_cruise_code,
                                                             desired_cruise_code=self.desired_cruise_code,
                                                             sample_name_col=self.sample_name_metadata_col_name)

        return self.df
    
    def _check_nc_samp_name_has_nc(self, metadata_row: pd.Series) -> str:
        # Checks to make sure sample names have .NC if the negative control column is True, if not adds the .NC
        sample_name = metadata_row[self.sample_name_metadata_col_name]
        if metadata_row[self.sample_metadata_file_neg_control_col_name] == 'TRUE' or metadata_row[self.sample_metadata_file_neg_control_col_name] == True:
            if '.NC' not in metadata_row[self.sample_name_metadata_col_name]:
                samp_name_bits = sample_name.split('.')
                samp_name = f'{samp_name_bits[0]}.NC.{samp_name_bits[1]}'
                return samp_name
            else:
                return sample_name
        else:
            return sample_name
        
    def _fix_samp_names(self, sample_name: str) -> str:
        # Fixes samples names (really just for SKQ23 sample names to replace _ with -. As the extraction and run sheets use -)
        if '_' in sample_name:
            sample_name = sample_name.replace('_', '-')
        if '.DY23-06' in sample_name:
            sample_name = sample_name.replace('-', '')
        if '.SKQ2021' in sample_name: 
            sample_name = sample_name.replace('.SKQ2021', '.SKQ21-15S')

        return sample_name
    
    def remove_extraneous_cast_no_chars(self, cast_no: str) -> int:
        # If Cast_No. is in the format of CTD001, then returns just the int
        if pd.notna(cast_no) and isinstance(cast_no, str) and 'CTD' in cast_no:
            cast_no = int(cast_no.replace('CTD', ''))

        return cast_no