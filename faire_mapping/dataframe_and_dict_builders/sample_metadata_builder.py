import pandas as pd
import re
from faire_mapping.dataframe_and_dict_builders.base_df_builder import BaseDfBuilder
from faire_mapping.utils import fix_cruise_code_in_samp_names

pd.set_option('future.no_silent_downcasting', True)

# TODO: _check_nc_samp_name_has_nc method is specific to OME for sample naming problems. When we standardize sample naming, can remove.
# TODO: _fix_samp_names specific to OME and for certain cruises. Can remove after data for these cruises has been submitted and sample naming becomes standardized. 
class SampleMetadataBuilder(BaseDfBuilder):
    """
    Builds the raw sample metadata data frame. Cleans some things and joins with the extraction df to complete the sample metadata df
    """
    EXTRACT_SAMP_NAME_COL = "samp_name"
    REPLICATE_PARENT_KEY_NAME = "replicate_parent" # They key name created in the replicates_dict
    
    def __init__(self,
                 sample_name_metadata_col_name: str,
                 sample_metadata_file_neg_control_col_name: str,
                 extraction_df: pd.DataFrame,
                 header: int = 0, 
                 sep: str = ',',
                 net_tow_weirness: str = False,
                 sample_metadata_cast_no_col_name: str = None,
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
        self.net_tow_weirness = net_tow_weirness
        self.sample_metadata_df, self.nc_metadata_df = self.filter_metadata_dfs()
        self.replicates_dict = self.create_biological_replicates_dict()
        
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

        # For WCOA21 net tow samples with all kinds of crazy sample name changess
        if self.net_tow_weirness:
            metadata_df = self.join_crazy_wcoa21net_tow_samp_extract_df(samp_df=samp_df)
        else:
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

        if metadata_df.empty:
            raise ValueError(f"Something went wrong in sample df join with extraction df - most likely cruise code or sample naming issues!")
        
        metadata_df.to_csv('/home/poseidon/zalmanek/FAIRe-Mapping/projects/WCOA/wcoa21_net_tow.joined_data.csv', index=False)

        return metadata_df

    def join_crazy_wcoa21net_tow_samp_extract_df(self, samp_df):
        """Joins the crazy sample naming changes for WCOA net tow"""
        
        def classify_and_normalize(sample_name, force_pe_lookup=None): 
            """
            Returns a dict with:
            - p_num: e.g. P188
            - family: 'PE' or 'standard'
            - ab_suiffix: 'A' or 'B'
            - canonical_key: the key to merge on (p_num + family)
            """
            s = str(sample_name).strip()
            is_pe = bool(re.search(r'\.PE', s, re.IGNORECASE))
            
            p_match = re.match(r'(P\d+)', s, re.IGNORECASE)
            p_num = p_match.group(1).upper() if p_match else None
            
            # Capture the PE pattern: E.PE or just .PE
            e_pe_match = re.search(r'(\.E\.PE|\.PE)', s, re.IGNORECASE)
            pe_pattern = e_pe_match.group(1).upper() if e_pe_match else None

            if force_pe_lookup is not None:
                is_pe = p_num in force_pe_lookup
            
            ab_match = re.match(r'P\d+([AB])(?:\.|$)', s, re.IGNORECASE)
            ab_suffix = ab_match.group(1).upper() if ab_match else None

            # Handel P184.a.PE / P184.b.PE - the a/b here is mid-string
            mid_ab_match = re.search(r'\.(A|B)\.', s, re.IGNORECASE)
            if mid_ab_match:
                ab_suffix = mid_ab_match.group(1).upper()

            family = 'PE' if is_pe else 'standard'
            canonical_key = f"{p_num}_{family}" if p_num else None

            return {
                'p_num': p_num,
                'family': family,
                'ab_suffix': ab_suffix,
                'canonical_key': canonical_key,
                'pe_pattern': pe_pattern
                }
        
        # --------- Normalize extraction df ------------
        extraction_meta = self.extraction_df[self.EXTRACT_SAMP_NAME_COL].apply(classify_and_normalize).apply(pd.Series)
        self.extraction_df = pd.concat([self.extraction_df, extraction_meta], axis=1)
        # Filter out B suffix rows because we are only matching A rows
        self.extraction_df = self.extraction_df[self.extraction_df['ab_suffix'] != 'B'].copy()
        # Drop sample name column from extraction df before merging
        self.extraction_df = self.extraction_df.drop(columns=[self.EXTRACT_SAMP_NAME_COL])

        # Build lookups from extraction_df now that its classified
        pe_pe_numbers = set(self.extraction_df[self.extraction_df['family'] == 'PE']['p_num'])
        pe_pattern_lookup = (
            self.extraction_df[self.extraction_df['family'] == 'PE'].groupby('p_num')['pe_pattern'].first().to_dict()
        )
        def build_canonical_samp_name(row):
            if row['family'] == 'PE':
                # P126.E.WCOA21 -? P126.E.PE.WCOA21
                pattern = pe_pattern_lookup.get(row['p_num'], 'PE')
                return re.sub(r'(\.WCOA21)', f'{pattern}\\1', row[self.sample_name_metadata_col_name], flags=re.IGNORECASE)
            return row[self.sample_name_metadata_col_name]

    
        # -------- Normalize sample df -------------
        samp_meta = samp_df[self.sample_name_metadata_col_name].apply(
            lambda x: classify_and_normalize(x, force_pe_lookup=pe_pe_numbers)).apply(pd.Series)
        samp_df = pd.concat([samp_df, samp_meta], axis=1)
        samp_df[self.sample_name_metadata_col_name] = samp_df.apply(build_canonical_samp_name, axis=1)

        # -------- Merge on Canonical P number ---------
        metadata_df = self.extraction_df.merge(
            samp_df, 
            on='canonical_key', 
            how='right', # same number of 
            )

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
        if self.sample_metadata_cast_no_col_name:
            self.df[self.sample_metadata_cast_no_col_name] = self.df[self.sample_metadata_cast_no_col_name].apply(
                self.remove_extraneous_cast_no_chars)
        
        if self.desired_cruise_code:
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
    
    def create_biological_replicates_dict(self) -> dict:
        # Creates a dictionary of the parent E number as a key and the replicate sample names as the values
        # e.g. {'E26': ['E26.2B.DY2012', 'E26.1B.NC.DY2012']}

        # Extract the parent E number and add to column called replicate_parent
        # Uses set() to remove any technical replicates (they will have the same)
        self.sample_metadata_df[self.REPLICATE_PARENT_KEY_NAME] = self.sample_metadata_df[self.sample_name_metadata_col_name].apply(
            self.extract_replicate_sample_parent)
        # Group by replicate parent
        replicate_dict = self.sample_metadata_df.groupby(self.REPLICATE_PARENT_KEY_NAME)[
            self.sample_name_metadata_col_name].apply(set).to_dict()
        # remove any key, value pairs where there aren't replicates and convert back to list
        replicate_dict = {replicate_parent: list(set(
            sample_name)) for replicate_parent, sample_name in replicate_dict.items() if len(sample_name) > 1}


        return replicate_dict
    
    def extract_replicate_sample_parent(self, sample_name):
        # Extracts the E number in the sample name
        if pd.notna(sample_name):
            return sample_name.split('.')[0]