from .faire_mapper import OmeFaireMapper
import pandas as pd

class ProjectMapper(OmeFaireMapper):
    
    def __init__(self, config_yaml: str):

        super().__init__(config_yaml)

        self.config_file = self.load_config(config_yaml)
        self.dataframes = {} # store all loaded dataframes
        self.combined_seq_runs_dataframes = {} # store all dataframes whose sequencing runs are combined (E.g. for cruises on multiple runs)
        self.filtered_samp_dataframes = {} # store all filtered sample dataframes

    def process_sample_run_data(self):
        # Process all csv sets defined in the config file.
        # For each dataset:
        # 1. Load the sample dataframe
        # 2. Load and combine all associated sequencing dataframes
        # 3. Filter sample dataframe based on samp_name value run dataframes
        # Returns a dictionary mapping dataset names to tuples of: (sample_df, combined_seq_run_df, and filtered_primary_df)

        datasets = self.config_file['datasets']
        result = {}

        for dataset in datasets:
            cruise_name = dataset.get('cruise_name')

            sample_csv_path = dataset.get('faire_sample_metadata_csv_path')
            associated_seq_run_csvs = dataset.get('associated_sequencing_csvs', [])

            cruise_samp_df = self.load_csv_as_df(file_path=sample_csv_path)
            self.dataframes[f"{cruise_name}_samp_df"] = cruise_samp_df
        
            # Load all associated sequencing dataframes
            associated_seq_dfs = []
            for seq_runs in associated_seq_run_csvs:
                assoc_seq_path = seq_runs.get('sequence_run_csv_path')

                df = self.load_csv_as_df(file_path=assoc_seq_path)
                associated_seq_dfs.append(df)

            # combine associated sequencing dfs
            combined_seq_df = pd.DataFrame()
            combined_seq_df = pd.concat(associated_seq_dfs, ignore_index=True)
            
            # store the combined associated sequencing df
            self.combined_seq_runs_dataframes[cruise_name] = combined_seq_df

            # Filter primary dataframe based on samp_name values in sequencing run dataframe
            self._filter_samp_df_by_samp_name(cruise_samp_df, combined_seq_df)
            # if missing_samples:
            #     print(missing_samples)
    
    def _filter_samp_df_by_samp_name(self, cruise_samp_df: pd.DataFrame, associated_seq_df: pd.DataFrame) -> pd.DataFrame:
        # filter sample dataframe to keep only rows where sample names exist in associated seq data frame
        
        missing_samples = []
        # Get unique sample name values from associated Dataframe
        valid_samp_names = set(associated_seq_df['samp_name'].unique())

        # Identify missing sample names (those in primary but not in associated seq runs)
        cruise_sample_samp_names = cruise_samp_df['samp_name'].unique()
        missing_samples = [name for name in cruise_sample_samp_names if name not in valid_samp_names]
        
        filtered_df = cruise_samp_df[cruise_samp_df['samp_name'].isin(valid_samp_names)].copy()
        print(missing_samples)

        return filtered_df, missing_samples