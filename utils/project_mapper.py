from .faire_mapper import OmeFaireMapper
from .lists import marker_shorthand_to_pos_cont_gblcok_name
import pandas as pd
import re

# TODO: Filter experiment run metadata to remove samples not in any of the sample_metadata (after sample_metadata is appropriatey filtered and new samples added)
class ProjectMapper(OmeFaireMapper):
    
    faire_sample_name_col = "samp_name"
    faire_rel_cont_id_col_name = "rel_cont_id"
    faire_pos_cont_type_col = 'pos_cont_type'
    faire_sample_category_col_name = "samp_category"
    faire_sample_composed_of_col_name = "sample_composed_of"
    
    def __init__(self, config_yaml: str):

        super().__init__(config_yaml)

        self.config_file = self.load_config(config_yaml)
        # self.samp_metadata_dataframes = {} # store all loaded sample metadata dataframes
        self.combined_seq_runs_dataframes = {} # store all dataframes whose sequencing runs are combined (E.g. for cruises on multiple runs)
        self.filtered_samp_dataframes = {} # store all filtered sample dataframes

    def process_sample_run_data(self):
        # Process all csv sets defined in the config file.
        # For each dataset:
        # 1. Load the sample dataframe
        # 2. Load and combine all associated sequencing dataframes
        # 3. Filter sample dataframe based on samp_name value run dataframes
        # Returns a dictionary mapping dataset names to tuples of: (sample_df, combined_seq_run_df, and filtered_primary_df)

        combined_sample_metadata_df = self.create_sample_metadata_df()
        combined_exp_run_df = self.create_exp_run_metadata_df()

        # Filter primary dataframe based on samp_name values in sequencing run dataframe
        filtered_samp_df, missing_samples = self._filter_samp_df_by_samp_name(samp_df=combined_sample_metadata_df, associated_seq_df=combined_exp_run_df)
        if missing_samples:
            print(f"missing samples are: {missing_samples}")

        
        # Add positive samples to rel_cont_id and sample_df
        pos_samp_map = self.create_positive_sample_map(run_df=combined_exp_run_df, sample_df=filtered_samp_df)
        filtered_samp_df[self.faire_rel_cont_id_col_name] = filtered_samp_df.apply(
            lambda row: self.add_pos_samps_to_rel_cont_id(metadata_row=row, positive_sample_map=pos_samp_map),
            axis=1
        )

        # Add PCR samples to sample_composed_of
        compose_of_updated_df = self.add_pcr_samps_to_composed_of(sample_df=filtered_samp_df, exp_run_df=combined_exp_run_df)

        updated_samp_df = self.add_positive_samps_to_samp_df(samp_df=compose_of_updated_df, positive_sample_map=pos_samp_map)

        self.save_final_df_as_csv(final_df=updated_samp_df, sheet_name='sampleMetadata', header=2, csv_path = '/home/poseidon/zalmanek/FAIRe-Mapping/projects/EcoFoci/ecoFoci_sampleMetadata.csv')
            
    def create_sample_metadata_df(self) -> pd.DataFrame:
   
        sample_metadata_datasets = self.config_file['datasets']['sample_metadata_csvs']

        # combine sample metadata from specified cruises in config
        samp_metadata_dfs = []
        for samp_metadata in sample_metadata_datasets:
            
            cruise_sample_csv_path = samp_metadata.get('sample_metadata_csv_path')

            cruise_samp_df = self.load_csv_as_df(file_path=cruise_sample_csv_path)
            samp_metadata_dfs.append(cruise_samp_df)

        combined_sample_metadata_df = pd.DataFrame()
        combined_sample_metadata_df = pd.concat(samp_metadata_dfs, ignore_index=True)

        return combined_sample_metadata_df
   
    def create_exp_run_metadata_df(self) -> pd.DataFrame:

        associated_exp_run_datasets = self.config_file['datasets']['associated_sequencing_csvs']

        # combine experimentRunMetadata from specific cruises
        associated_exp_run_dfs = []
        for exp_run in associated_exp_run_datasets:

            exp_run_csv_path = exp_run.get('sequence_run_csv_path')
            exp_run_df = self.load_csv_as_df(file_path=exp_run_csv_path)
            associated_exp_run_dfs.append(exp_run_df)
        
        combined_exp_run_df = pd.DataFrame()
        combined_exp_run_df = pd.concat(associated_exp_run_dfs, ignore_index=True)

        return combined_exp_run_df

    def _filter_samp_df_by_samp_name(self, samp_df: pd.DataFrame, associated_seq_df: pd.DataFrame) -> pd.DataFrame:
        # filter sample dataframe to keep only rows where sample names exist in associated seq data frame
        
        missing_samples = []
        # Get unique sample name values from associated Dataframe
        valid_samp_names_raw = set(associated_seq_df['samp_name'].unique())

        # Remove .PCR so they can match appropriately
        valid_samp_names = set()
        for name in valid_samp_names_raw:
            if '.PCR' in name:
                subbed_name = re.sub(r'\.PCR\d*$', '', name)
                valid_samp_names.add(subbed_name)
            else:
                valid_samp_names.add(name)

        # Identify missing sample names (those in primary but not in associated seq runs)
        cruise_sample_samp_names = samp_df['samp_name'].unique()
        missing_samples = [name for name in cruise_sample_samp_names if name not in valid_samp_names]
        
        filtered_df = samp_df[samp_df['samp_name'].isin(valid_samp_names)].copy()

        return filtered_df, missing_samples
    
    def create_positive_sample_map(self, run_df, sample_df):
        # Create a dictionary of positive samps: associated samps
        positive_sample_map = {}
        
        grouped = run_df.groupby('assay_name')
        
        for assay_name, group in grouped:
            # Find samples with positive in their name
            positive_samples = group[group['samp_name'].str.contains('POSITIVE', case=False)]['samp_name'].unique()
            # Get all sample names in this gropu that are also in the sample_df
            valid_samples = group[group['samp_name'].isin(sample_df['samp_name'])]['samp_name'].unique().tolist()

            # for each positive sample add an entry to dictionary
            for pos_sample in positive_samples:
                other_samples = [sample for sample in valid_samples if sample != pos_sample]
                if other_samples:
                    positive_sample_map[pos_sample] = other_samples

        return positive_sample_map
    
    def add_pos_samps_to_rel_cont_id(self, metadata_row:pd.Series, positive_sample_map: dict) -> str:
        # Add the positive sample names to the rel_cont_id and returns the string list with | separator
        sample_name = metadata_row[self.faire_sample_name_col]
        
        # to account for na values - I think these are just empty rows
        try:
            rel_cont_ids = metadata_row[self.faire_rel_cont_id_col_name].split(' | ')
        except:
            print(f"sample name {sample_name} does not appear to have a rel_cont_id: {metadata_row[self.faire_rel_cont_id_col_name]}")

        for pos_samp, associated_samps in positive_sample_map.items():
            if sample_name in associated_samps:
                rel_cont_ids.append(pos_samp)

        # if 'not applicable' in rel_cont_id and len > 0 then remove that str.
        if len(rel_cont_ids) > 1:
            filtered_list = [id for id in rel_cont_ids if "not applicable" not in id.lower()]
        else:
            filtered_list = rel_cont_ids

        # rejoin into format
        rel_cont_ids = ' | '.join(filtered_list)
        
        return rel_cont_ids

    def get_pos_cont_type(self, pos_cont_sample_name: str) -> str:

        # Get the marker from the positive control sample name (e.g. run2.ITS1.POSITIVE - this will give you ITS1)
        shorthand_marker = pos_cont_sample_name.split('.')[1]

        g_block = marker_shorthand_to_pos_cont_gblcok_name.get(
            shorthand_marker)

        pos_cont_type = f"PCR positive of synthetic DNA. Gblock name: {g_block}"

        return pos_cont_type
   
    def add_positive_samps_to_samp_df(self, samp_df: pd.DataFrame, positive_sample_map: dict) -> pd.DataFrame:
        # Add positive samples as their own rows in the data frame
        
        new_rows = []
        for pos_samp, samples in positive_sample_map.items():
            # check that at least on sample matches from pos_sample_map in sample_df and if it does, add positive sample to sample df
            matching_samps = samp_df[samp_df[self.faire_sample_name_col].isin(samples)]
            if not matching_samps.empty:
                pos_cont_type = self.get_pos_cont_type(pos_cont_sample_name=pos_samp)
                new_pos_samp_row = {
                    self.faire_sample_name_col: pos_samp, 
                    self.faire_sample_category_col_name: 'positive control', 
                    self.faire_pos_cont_type_col: pos_cont_type
                    }
                new_rows.append(new_pos_samp_row)

        if new_rows:
            new_pos_rows_df = pd.DataFrame(new_rows)
            # drop any duplicate positive control rows
            new_df = new_pos_rows_df.drop_duplicates()
            # add positive samples to samp df
            updated_samp_df_with_pos = pd.concat([samp_df, new_df], ignore_index=True)

        # update positive sample empty values with "not applicable: control sample"
        # pos_mask = updated_samp_df_with_pos[self.faire_sample_category_col_name].str.contains('positive control', case=False, na=False)

        # # update other columns to be "not applicable: control sample" for positive controls
        # for col in updated_samp_df_with_pos.columns:
        #     updated_samp_df_with_pos.loc[pos_mask & updated_samp_df_with_pos[col].isna(), col] = 'not applicable: control sample'

        return updated_samp_df_with_pos

    def add_pcr_samps_to_composed_of(self, sample_df: pd.DataFrame, exp_run_df: pd.DataFrame) -> pd.DataFrame:
    # def remove_ids_from_rel_cont_id_that_dont_exist(self, samp_df: pd.DataFrame)
        results_df = sample_df.copy()

        pcr_samp_map_dict = {}
        for samp in exp_run_df[self.faire_sample_name_col]:
            if 'PCR' in samp:
                #extract the base name by Removin PCR suffix
                base_samp_name = re.sub(r'\.PCR\d*$', '', samp)
                if base_samp_name not in pcr_samp_map_dict:
                    pcr_samp_map_dict[base_samp_name] = {samp}
                else:
                    pcr_samp_map_dict[base_samp_name].add(samp)


        for idx, row in results_df.iterrows():
            sample_name = row[self.faire_sample_name_col]
            if sample_name in pcr_samp_map_dict:
                samp_composed_of = list(pcr_samp_map_dict.get(sample_name))
                samp_composed_of.sort() # sort so PCRs are in order 1, 2, 3
                sample_composed_of = (' | ').join(samp_composed_of)
                results_df.at[idx, self.faire_sample_composed_of_col_name] = sample_composed_of

        return results_df