from .faire_mapper import OmeFaireMapper
from .lists import marker_shorthand_to_pos_cont_gblcok_name
import pandas as pd
import re

# TODO: Filter experiment run metadata to remove samples not in any of the sample_metadata (after sample_metadata is appropriatey filtered and new samples added)
# TODO: add pooled samples to sample metadata and and sample_compose_of fo rthe samples they include (filter ^ after this step?)
# TODO: uncomment the code in add_positive_samps_to_samp_df function when ready to fully run, or just fix ahead of time before next pandas release
# TODO: Add code to add_pooled_samps_to_sample_metadata to add in not applicable to empty values for pooled samples
class ProjectMapper(OmeFaireMapper):
    
    faire_sample_name_col = "samp_name"
    faire_rel_cont_id_col_name = "rel_cont_id"
    faire_pos_cont_type_col = 'pos_cont_type'
    faire_sample_category_col_name = "samp_category"
    faire_sample_composed_of_col_name = "sample_composed_of"
    
    def __init__(self, config_yaml: str):

        super().__init__(config_yaml)

        self.config_file = self.load_config(config_yaml)
        self.mismatch_samp_names_dict = self.config_file['mismatch_sample_names']
        self.pooled_samps_dict = self.config_file['pooled_samps']
        # self.samp_metadata_dataframes = {} # store all loaded sample metadata dataframes
        self.combined_seq_runs_dataframes = {} # store all dataframes whose sequencing runs are combined (E.g. for cruises on multiple runs)
        self.filtered_samp_dataframes = {} # store all filtered sample dataframes

    def process_sample_run_data(self):
        # Process all csv sets defined in the config file.
        # 1. Combine all sample metadata spreadsheets into one and combine all experiment run metadata spreadsheets into one
        # 2. f

        combined_sample_metadata_df = self.create_sample_metadata_df()
        combined_exp_run_df = self.create_exp_run_metadata_df()

        # Update sample names to match for sample name and rel_cont_id
        combined_sample_metadata_df[self.faire_sample_name_col] = combined_sample_metadata_df[self.faire_sample_name_col].apply(self.update_mismatch_sample_names)
        combined_sample_metadata_df[self.faire_rel_cont_id_col_name] = combined_sample_metadata_df[self.faire_rel_cont_id_col_name].apply(self.update_rel_cont_id_for_mismatch_samps)
        
        # Add in pooled samples and add to rel_cont_id
        sample_metadata_with_pooled = self.add_pooled_samps_to_sample_metadata(sample_df=combined_sample_metadata_df, experiment_run_df=combined_exp_run_df)
        sample_metadata_with_pooled_rel_cont_id = self.update_rel_cont_id_with_pool_samp(sample_df=sample_metadata_with_pooled)

         # Add positive samples to rel_cont_id and sample_df
        pos_samp_map = self.create_positive_sample_map(run_df=combined_exp_run_df, sample_df=sample_metadata_with_pooled_rel_cont_id)
        sample_metadata_with_pooled_rel_cont_id[self.faire_rel_cont_id_col_name] = sample_metadata_with_pooled_rel_cont_id.apply(
            lambda row: self.add_pos_samps_to_rel_cont_id(metadata_row=row, positive_sample_map=pos_samp_map),
            axis=1
        )
        updated_pos_samp_df = self.add_positive_samps_to_samp_df(samp_df=sample_metadata_with_pooled, positive_sample_map=pos_samp_map)

        # Add PCR samples to sample_composed_of
        compose_of_updated_df = self.add_pcr_samps_to_composed_of(sample_df=updated_pos_samp_df, exp_run_df=combined_exp_run_df)
    
        # Filter primary dataframe based on samp_name values in sequencing run dataframe
        filtered_samp_df, missing_samples = self._filter_samp_df_by_samp_name(samp_df=compose_of_updated_df, associated_seq_df=combined_exp_run_df)
        if missing_samples:
            print(f"samples that will be eliminated from sample metadata/missing from experiment run metadata: {missing_samples}")

        self.save_final_df_as_csv(final_df=filtered_samp_df, sheet_name='sampleMetadata', header=2, csv_path = '/home/poseidon/zalmanek/FAIRe-Mapping/projects/EcoFoci/ecoFoci_sampleMetadata.csv')
            
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

    def update_mismatch_sample_names(self, sample_name: str) -> str:
        # Update sample metadata sample name to be the sample name that needs to match in experiment run metadata
        if sample_name in self.mismatch_samp_names_dict:
            sample_name = self.mismatch_samp_names_dict.get(sample_name)
        return sample_name

    def update_rel_cont_id_for_mismatch_samps(self, rel_cont_id: str) -> str:
        # Update the rel_cont_id to samples that had mistmatch names
        rel_cont_id_list = rel_cont_id.split(' | ')
        updated_rel_cont_id_list = [self.mismatch_samp_names_dict.get(samp) if samp in self.mismatch_samp_names_dict else samp for samp in rel_cont_id_list]
        return ' | '.join(updated_rel_cont_id_list)

    def add_pooled_samps_to_sample_metadata(self, sample_df: pd.DataFrame, experiment_run_df: pd.DataFrame) -> pd.DataFrame:
        # add pooled samples that are relevant to sample metadata spreadsheet
        all_valid_seq_samps = set(experiment_run_df[self.faire_sample_name_col].unique())
        all_valid_sample_samps = set(sample_df[self.faire_sample_name_col].unique())

        # Check if sample name is in pooled sample dictionary and if is add pooled sample to rel_cont_id
        new_rows = []
        for pooled_info in self.pooled_samps_dict:
            pooled_samp = pooled_info['pooled_samp_name']
            for samp in pooled_info['samps_that_were_pooled']:
                if samp in all_valid_sample_samps and pooled_samp in all_valid_seq_samps:
                    sample_category = pooled_info['sample_category']
                    if sample_category == 'negative control':
                        dict_key = 'neg_cont_type'
                    elif sample_category == 'positive control':
                        dict_key = 'pos_cont_type'
                    elif sample_category == 'sample':
                        dict_key = None
                    else:
                        raise ValueError(f"Don't have functionality for this sample category {sample_category} - please see code and possibly update")
                
                    # Create new row for pooled sample - add neg_cont_type or pos_conty_type
                    new_pooled_samp_row = {
                        self.faire_sample_name_col: pooled_samp,
                        self.faire_sample_category_col_name: sample_category,
                        self.faire_sample_composed_of_col_name: ' | '.join([samp for samp in pooled_info['samps_that_were_pooled'] if samp in all_valid_sample_samps])
                    }
                    if dict_key:
                        new_pooled_samp_row[dict_key] = pooled_info['cont_type']
                        new_rows.append(new_pooled_samp_row)

                    # add pooled sample name to subsmaple's rel_cont_id
                    rel_cont_id = sample_df.loc[sample_df[self.faire_sample_name_col] == samp, self.faire_rel_cont_id_col_name].values
                    if ' | ' in rel_cont_id:
                        rel_cont_id = rel_cont_id.split(' | ')
                    else:
                        rel_cont_id = [rel_cont_id]
                    rel_cont_id.append(pooled_samp)
                    # filter to remove not applicable if it existed before
                    if len(rel_cont_id) > 1:
                        filtered_rel_cont_id_list = [id for id in rel_cont_id if "not applicable" not in str(id).lower()]
                    else:
                        filtered_rel_cont_id_list = rel_cont_id
                    sample_df.loc[sample_df[self.faire_sample_name_col] == samp, self.faire_rel_cont_id_col_name] = ' | '.join(filtered_rel_cont_id_list)
                else:
                    print(f"sample {samp} exists in a pooled set according to the config, but may not exist in the sample metadata or the pooled sample may not exist in the experiment run metadata. Please double check this! But moving on without adding it.")
                    
        # Add new pooled samples to sample_df
        if new_rows:
            new_pooled_rows_df = pd.DataFrame(new_rows)
            new_pooled_rows_df = new_pooled_rows_df.drop_duplicates()
            updated_samp_df_with_pools = pd.concat([sample_df, new_pooled_rows_df], ignore_index=True)

        return updated_samp_df_with_pools
    
    def update_rel_cont_id_with_pool_samp(self, sample_df: pd.DataFrame) -> pd.DataFrame:
        # if a subsample of a pooled sample exists in the rel_cont_id column, add the pooled sample to rel_cont_id
        for i, r in sample_df.iterrows():
            rel_cont_ids = str(r[self.faire_rel_cont_id_col_name]).split(' | ')
            updated_rel_ids = []
            for rel_id in rel_cont_ids:
                # check if rel id exists as a subsample of a pooled sample
                for pooled_info in self.pooled_samps_dict:
                    if rel_id in pooled_info['samps_that_were_pooled']:
                        pooled_samp_name = pooled_info['pooled_samp_name']
                        updated_rel_ids.append(pooled_samp_name)
                    else:
                        updated_rel_ids.append(rel_id)
            # remove duplicated rel_ids that happend in the loop with set 
            updated_rel_ids = ' | '.join(list(set(updated_rel_ids)))
            sample_df.at[i, self.faire_rel_cont_id_col_name] = updated_rel_ids
        return sample_df

    def _filter_samp_df_by_samp_name(self, samp_df: pd.DataFrame, associated_seq_df: pd.DataFrame) -> pd.DataFrame:
        # filter sample dataframe to keep only rows where sample names exist in associated seq data frame
        
        missing_samples = []
        # Get unique sample name values from associated Dataframe
        exp_valid_samps = set(associated_seq_df[self.faire_sample_name_col].unique())

        # Also valid samples are pooled subsamples that won't necessarily exist in the experiment run metadata
        other_valid_samps = set()
        for pooled_info in self.pooled_samps_dict:
            for samp in pooled_info['samps_that_were_pooled']:
                if samp in set(samp_df[self.faire_sample_name_col]):
                    other_valid_samps.add(samp)

        # combine valid samps
        valid_samp_names_raw = exp_valid_samps | other_valid_samps

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
            positive_samples = group[group[self.faire_sample_name_col].str.contains('POSITIVE', case=False)][self.faire_sample_name_col].unique()
            # Get all sample names in this gropu that are also in the sample_df
            valid_samples = group[group[self.faire_sample_name_col].isin(sample_df[self.faire_sample_name_col])][self.faire_sample_name_col].unique().tolist()

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
            rel_cont_ids = []

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
            # check that at least one sample matches from pos_sample_map in sample_df and if it does, add positive sample to sample df
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