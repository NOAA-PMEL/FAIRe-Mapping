from .faire_mapper import OmeFaireMapper
from pathlib import Path
from .lists import marker_to_assay_mapping , marker_to_raw_folder_mapping
from .custom_exception import NoAcceptableAssayMatch
import yaml
import pandas as pd
import os
import re
import hashlib

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
        self.run_short_cruise_name = self.config_file['run_short_cruise_name']

        self.mapping_dict = self._create_experiment_run_mapping_dict()
        self.jv_run_metadata_df = self._create_experiment_metadata_df()
        
        # populate raw_filename_dicts
        self.raw_filename_dict = {} # dictionary of forward raw data files with checksums
        self.raw_filename2_dict = {} # dictionary of reverse raw data files with checksums
        self._create_marker_sample_raw_data_file_dicts()
        

    def _create_experiment_run_mapping_dict(self):

        experiment_mapping_df = self.load_google_sheet_as_df(google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.experiment_run_mapping_sheet_name, header=1)

        # Group by the mapping type
        group_by_mapping = experiment_mapping_df.groupby(self.mapping_file_mapped_type_column)

        # Create nested dictionary {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
        mapping_dict = {}
        for mapping_value, group in group_by_mapping:
            column_map_dict = {k: v for k, v in zip(group[self.mapping_file_FAIRe_column], group[self.mapping_file_metadata_column]) if pd.notna(v)}
            mapping_dict[mapping_value] = column_map_dict
        
        return mapping_dict
    
    def _create_experiment_metadata_df(self) -> pd.DataFrame:
        # TODO: maybe rethink th

        exp_df = self.load_google_sheet_as_df(google_sheet_id=self.jv_run_sample_metadata_file_id, sheet_name=self.jv_metadata_sample_sheet_name, header=0)

        # if DY2012 cruise, replace strings DY20 with DY2012
        exp_df[self.jv_run_sample_name_column] = exp_df[self.jv_run_sample_name_column].apply(self.str_replace_dy2012_cruise)

        # only keep rows with cruise name in sample name and Postive
        cruise_df = exp_df[(exp_df[self.jv_run_sample_name_column].str.contains(self.run_short_cruise_name) | (exp_df[self.jv_run_sample_name_column] == 'POSITIVE'))].reset_index(drop=True)
        
        return cruise_df
    
    def _get_md5_checksum(self, filepath: str):

        md5_hash = hashlib.md5()

        with open(filepath, 'rb') as f:
            # Read files in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b''):
                md5_hash.update(chunk)

        return md5_hash.hexdigest()
    
    def _outline_raw_data_dict(self, sample_name: str, file_num: int, all_files: list, marker: str, marker_dir: str) -> dict:
        # outlines raw filename dictionary used by create_marker_sample_raw_data_files for filename and filename2 in FAIRe

        pattern = re.compile(f"^{re.escape(sample_name)}_R[{file_num}].+")

        matching_files = [f for f in all_files if pattern.match(f)]

        if file_num == 1:
            target_dict = self.raw_filename_dict
        else:
            target_dict = self.raw_filename2_dict

        if matching_files:
            if marker not in target_dict:
                target_dict[marker] = {}

            for filename in matching_files:
                filepath = os.path.join(marker_dir, filename)
                md5_checksum = self._get_md5_checksum(filepath)
                target_dict[marker][sample_name] = {
                    "filename": filename,
                    "checksum": md5_checksum
                }

        else:
            print(f"Warning: No matching files found for sample {sample_name} in marker {marker}")

    
    def _create_marker_sample_raw_data_file_dicts(self):
        # Finds all matching data files by marker for each sample returns two nested dict one for forward raw data files, and one for reverse raw data files
        # {marker1: {sample1: {filename: file1.gz, checksum: 56577}}

        # group run metadata by marker
        grouped = self.jv_run_metadata_df.groupby(self.jv_metadata_marker_col_name)

        for marker, group in grouped:
            folder_name = marker_to_raw_folder_mapping.get(marker, marker)
            marker_dir = os.path.join(self.jv_raw_data_path, folder_name)

            # TODO: consider adding raise Error here?
            if not os.path.exists(marker_dir):
                print(f"Warning: Directory for marker {marker} ({marker_dir}) not found")
                continue

            # Get all filenames in the direcotry
            all_files = os.listdir(marker_dir)

            # For each sample in this marker group
            for sample_name in group[self.jv_run_sample_name_column].unique():
                
                self._outline_raw_data_dict(sample_name=sample_name, file_num=1, all_files=all_files, marker=marker, marker_dir=marker_dir)
                self._outline_raw_data_dict(sample_name=sample_name, file_num=2, all_files=all_files, marker=marker ,marker_dir=marker_dir)

    
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

    def get_raw_file_names(self, metadata_row: pd.Series, raw_file_dict: dict) -> tuple:
        # Gets the name of the raw file based on sample name and marker
     
        marker_name = metadata_row[self.jv_metadata_marker_col_name]
        sample_name = metadata_row[self.jv_run_sample_name_column]

        # Get approrpiate nested marker dict and correspongind nested sample list with dictionary of files and checksums
        filename = raw_file_dict.get(marker_name).get(sample_name).get('filename')

        return filename
    
    def get_cheksums(self, metadata_row: pd.Series, raw_file_dict: dict) -> tuple:
        # Gets the checksum of the raw file based on sample name and marker
     
        marker_name = metadata_row[self.jv_metadata_marker_col_name]
        sample_name = metadata_row[self.jv_run_sample_name_column]

        # Get approrpiate nested marker dict and correspongind nested sample list with dictionary of files and checksums
        checksum = raw_file_dict.get(marker_name).get(sample_name).get('checksum')

        return checksum

        

        

        