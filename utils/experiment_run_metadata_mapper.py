from .faire_mapper import OmeFaireMapper
from pathlib import Path
from .lists import marker_to_assay_mapping , marker_mapping
from .custom_exception import NoAcceptableAssayMatch
import yaml
import pandas as pd
import os
import re

# TODO: add run3_marrker_POSITIVE to positive controls sample names (and blanks?) - double check with Sean and Zack about doing this

class ExperimentRunMetadataMapper(OmeFaireMapper):

    experiment_run_mapping_sheet_name = "experimentRunMetadata"
    jv_metadata_sample_sheet_name = "Sample Data Sheet Long"
    jv_metadata_marker_col_name = "Metabarcoding Marker"

    def __init__(self, config_yaml: yaml):
        super().__init__(config_yaml)

        self.jv_run_sample_name_column = self.config_file['jv_run_sample_name_column']
        self.jv_index_name_col = self.config_file['jv_index_name_col']
        self.faire_sample_samp_name_col = 'samp_name' # The name of the column for sample name in the 
        self.jv_raw_data_path = self.config_file['jv_raw_data_path']
        self.jv_run_sample_metadata_file_id = self.config_file['jv_run_sample_metadata_file_id']
        self.jv_run_name = self.config_file['jv_run_name']

        self.mapping_dict = self.create_experiment_run_mapping_dict()
        self.jv_run_metadata_df = self.create_experiment_metadata_df()
        self.jv_sample_marker_raw_data_dict = self.create_marker_sample_raw_data_file_dict()
        

    def create_experiment_run_mapping_dict(self):

        experiment_mapping_df = self.load_google_sheet_as_df(google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.experiment_run_mapping_sheet_name, header=1)

        # Group by the mapping type
        group_by_mapping = experiment_mapping_df.groupby(self.mapping_file_mapped_type_column)

        # Create nested dictionary {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
        mapping_dict = {}
        for mapping_value, group in group_by_mapping:
            column_map_dict = {k: v for k, v in zip(group[self.mapping_file_FAIRe_column], group[self.mapping_file_metadata_column]) if pd.notna(v)}
            mapping_dict[mapping_value] = column_map_dict
        
        return mapping_dict
    
    def create_experiment_metadata_df(self) -> pd.DataFrame:
        # TODO: maybe rethink th

        exp_df = self.load_google_sheet_as_df(google_sheet_id=self.jv_run_sample_metadata_file_id, sheet_name=self.jv_metadata_sample_sheet_name, header=0)

        # if DY2012 cruise, replace strings DY20 with DY2012
        exp_df[self.jv_run_sample_name_column] = exp_df[self.jv_run_sample_name_column].apply(self.str_replace_dy2012_cruise)

        # only keep rows with cruise name in sample name and Postive
        cruise_df = exp_df[(exp_df[self.jv_run_sample_name_column].str.contains(self.run_short_cruise_name) | (exp_df[self.jv_run_sample_name_column] == 'POSITIVE'))].reset_index(drop=True)

        # group by marker
        # grouped = cruise_df.groupby(self.jv_metadata_marker_col_name)
        
        return cruise_df
    
    def convert_assay_to_standard(self, marker: str) -> str:
        # matches the marker to the corresponding assay and returns standardized assay name
        
        assay = marker_to_assay_mapping.get(marker, None)

        if assay == None:
            raise NoAcceptableAssayMatch(f'The marker/assay {marker} does not match any of the {[v for v in marker_to_assay_mapping.values()]}. Try changing cutoff or normalizing.')
        else:
            return assay

    def jv_create_seq_id(self, metadata_row: pd.Series) -> str:
        # create lib_id by concatenating index and sample name together

        index_name = metadata_row[self.jv_index_name_col]
        sample_name = metadata_row[self.jv_run_sample_name_column]

        lib_id = sample_name + "_" + index_name

        return lib_id
    
    def create_marker_sample_raw_data_file_dict(self):
        # Finds all matching data files by marker for each sample returns a nested dict in the format of
        # {marker1: {samp_name: [file_r1, file_r2]}, marker2: {samp_name: [file_r1, file_r2]}}

        # group run metadata by marker
        grouped = self.jv_run_metadata_df.groupby(self.jv_metadata_marker_col_name)

        results = {}
        for marker, group in grouped:
            folder_name = marker_mapping.get(marker, marker)
            marker_dir = os.path.join(self.jv_raw_data_path, folder_name)

            # TODO: consider adding raise Error here?
            if not os.path.exists(marker_dir):
                print(f"Warning: Directory for marker {marker} ({marker_dir}) not found")
                continue

            # Get all filenames in the direcotry
            all_files = os.listdir(marker_dir)

            # For each sample in this marker group
            for sample_name in group[self.jv_run_sample_name_column].unique():
                # pattern to match both R1 and R2 files
                pattern = re.compile(f"^{re.escape(sample_name)}_R[12].+")
                matching_files = [f for f in all_files if pattern.match(f)]

                if matching_files:
                    if marker not in results:
                        results[marker] = {}
                    results[marker][sample_name] = matching_files
                else:
                    print(f"Warning: No matching files found for sample {sample_name} in marker {marker}")

        return results

        

        