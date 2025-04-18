from .faire_mapper import OmeFaireMapper
from pathlib import Path
from .lists import assay_names
from .custom_exception import NoAcceptableAssayMatch
import yaml
import pandas as pd
import difflib

# TODO: add run3_marrker_POSITIVE to positive controls sample names (and blanks?) - double check with Sean and Zack about doing this

class ExperimentRunMetadataMapper(OmeFaireMapper):

    sheet_name = "experimentRunMetadata"

    def __init__(self, config_yaml: yaml, faire_sample_metadata_df: pd.DataFrame):
        super().__init__(config_yaml)

        self.jv_run_sample_name_column = self.config_file['jv_run_sample_name_column']
        self.jv_index_name_col = self.config_file['jv_index_name_col']
        self.short_cruise_name = self.config_file['short_cruise_name'] # The short cruise name (in the sample name)
        self.faire_sample_samp_name_col = 'samp_name' # The name of the column for sample name in the 
        self.jv_raw_data_path = self.config_file['jv_raw_data_path']

        self.mapping_dict = self.create_experiment_run_mapping_dict()
        self.jv_run_metadata_df = self.create_experiment_metadata_df()
        

    def create_experiment_run_mapping_dict(self):

        experiment_mapping_df = self.load_csv_as_df(file_path=Path(self.config_file['jv_experiment_mapping_file']), header=1)

        # Group by the mapping type
        group_by_mapping = experiment_mapping_df.groupby(self.mapping_file_mapped_type_column)

        # Create nested dictionary {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
        mapping_dict = {}
        for mapping_value, group in group_by_mapping:
            column_map_dict = {k: v for k, v in zip(group[self.mapping_file_FAIRe_column], group[self.mapping_file_metadata_column]) if pd.notna(v)}
            mapping_dict[mapping_value] = column_map_dict
        
        return mapping_dict
    
    def create_experiment_metadata_df(self) -> pd.DataFrame:

        exp_df = self.load_csv_as_df(file_path=Path(self.config_file['jv_run_sample_metadata_file']))

        # if DY2012 cruise, replace strings DY20 with DY2012
        exp_df[self.jv_run_sample_name_column] = exp_df[self.jv_run_sample_name_column].apply(self.str_replace_dy2012_cruise)

        # only keep rows with cruise name in sample name and Postive and Blank
        cruise_df = exp_df[(exp_df[self.jv_run_sample_name_column].str.contains(self.short_cruise_name) | (exp_df[self.jv_run_sample_name_column] == 'POSITIVE') | (exp_df[self.jv_run_sample_name_column].str.contains('Blank')))].reset_index(drop=True)

        return cruise_df
    
    def convert_assay_to_standard(self, marker_assay: str) -> str:
        # checks the marker/assay and finds the closest match based on teh assay_names list
        
        # parada matches to dloop for some reason, so adding this logic
        if 'parada' in marker_assay.lower():
            marker_assay = 'Parada_universal_SSU16S_V4'
        
        matches = difflib.get_close_matches(marker_assay, assay_names, n=1, cutoff=0.2)
        print(f'{marker_assay} matched tp {matches[0]}')

        if matches:
            return matches[0]
        else:
            raise NoAcceptableAssayMatch(f'The marker/assay {marker_assay} does not match any of the {assay_names}. Try changing cutoff or normalizing.')

    def jv_create_seq_id(self, metadata_row: pd.Series) -> str:
        # create lib_id by concatenating index and sample name together

        index_name = metadata_row[self.jv_index_name_col]
        sample_name = metadata_row[self.jv_run_sample_name_column]

        lib_id = sample_name + "_" + index_name

        return lib_id
        