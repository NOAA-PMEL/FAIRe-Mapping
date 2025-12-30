from .faire_mapper import OmeFaireMapper
from .lists import marker_to_assay_mapping, marker_to_shorthand_mapping, mismatch_sample_names_metadata_to_raw_data_files_dict, update_cruise_codes
from .custom_exception import NoAcceptableAssayMatch
from pathlib import Path
from faire_mapping.utils import str_replace_for_samps
import yaml
import pandas as pd
import os
import subprocess
import re
import hashlib
import shutil 

# TODO: update for PCR replicates? - MAke sample name the same, change lib_id?
# TODO: add associatedSequences functionlity after submittting to NCBI
# TODO: rerun all runs after code is final

class ExperimentRunMetadataMapper(OmeFaireMapper):

    experiment_run_mapping_sheet_name = "experimentRunMetadata"
    faire_template_exp_run_sheet_name = "experimentRunMetadata"
    faire_seq_run_id_col = 'seq_run_id'

    def __init__(self, config_yaml: yaml):
        super().__init__(config_yaml)

        self.run_metadata_sample_name_column = self.config_file['run_metadata_sample_name_column']
        self.run_metadata_marker_col_name = self.config_file['run_metadata_marker_col_name']
        self.run_metadata_sample_sheet_name = self.config_file['run_metadata_sample_sheet_name']
        self.faire_sample_samp_name_col = 'samp_name' # The name of the column for sample name in the 
        self.run_raw_data_path = self.config_file['run_raw_data_path']
        self.run_sample_metadata_file_id = self.config_file['run_sample_metadata_file_id']
        self.run_name = self.config_file['run_name']
        self.asv_counts_tsvs_for_run = self.config_file['asv_counts_tsvs_for_run']
        self.otu_num_tax_assigned_files_for_run = self.config_file['otu_num_tax_assigned_files_for_run']
        self.ignore_markers = self.config_file['ignore_markers']
        self.google_sheet_mapping_file_id = self.config_file['google_sheet_mapping_file_id']
        self.merged = self.config_file['merged']  if 'merged' in self.config_file else False

        self.mapping_dict = self._create_experiment_run_mapping_dict()
        self.run_metadata_df = self._create_experiment_metadata_df()
        
        # populate data dicts
        self.raw_filename_dict = {} # dictionary of forward raw data files with checksums
        self.raw_filename2_dict = {} # dictionary of reverse raw data files with checksums
        self.asv_data_dict =  {}
        self.asv_samp_name_dict = {} # dictionary to store sample names and updated samples names since they don't match
        self._create_marker_sample_raw_data_file_dicts() 
        self._create_count_asv_dict(revamp_blast=self.config_file['revamp_blast'])

        self.exp_run_faire_template_df = self.load_faire_template_as_df(file_path=self.config_file['faire_template_file'], sheet_name=self.faire_template_exp_run_sheet_name, header=self.faire_sheet_header).dropna()
    
    def generate_run_metadata(self) -> pd.DataFrame :
        # Works for run2, need to check other jv runs - maybe can potentially use for OSU runs, if mapping file is generically the same?
        exp_metadata_results = {}
    
        # Step 1: Add exact mappings
        for faire_col, metadata_col in self.mapping_dict[self.exact_mapping].items():
            exp_metadata_results[faire_col] = self.run_metadata_df[metadata_col].apply(
                lambda row: self.apply_exact_mappings(metadata_row=row, faire_col=faire_col))
        
        # Step 2: Add constants
        for faire_col, static_value in self.mapping_dict[self.constant_mapping].items():
            exp_metadata_results[faire_col] = self.apply_static_mappings(faire_col=faire_col, static_value=static_value)

        # Step 3: Add related mappings
        for faire_col, metadata_col in self.mapping_dict[self.related_mapping].items():
            # Add assay_name
            if faire_col == 'assay_name':
                exp_metadata_results[faire_col] = self.run_metadata_df[metadata_col].apply(self.convert_assay_to_standard)

            elif faire_col == 'lib_id':
                lib_ids = self.run_metadata_df.apply(
                    lambda row: self.create_lib_id(metadata_row=row),
                    axis=1
                )
                exp_metadata_results[faire_col] = lib_ids
                self.run_metadata_df['lib_id'] = lib_ids # save to run_metadata_df so can access to update file names

            elif faire_col == 'filename':
                exp_metadata_results[faire_col] = self.run_metadata_df.apply(
                    lambda row: self.get_raw_file_names(metadata_row=row, raw_file_dict=self.raw_filename_dict),
                    axis=1
                )
            elif faire_col =='filename2':
                exp_metadata_results[faire_col] = self.run_metadata_df.apply(
                    lambda row: self.get_raw_file_names(metadata_row=row, raw_file_dict=self.raw_filename2_dict),
                    axis=1
                )
            elif faire_col == 'checksum_filename':
                exp_metadata_results[faire_col] = self.run_metadata_df.apply(
                    lambda row: self.get_cheksums(metadata_row=row, raw_file_dict=self.raw_filename_dict),
                    axis = 1
                )
            elif faire_col == 'checksum_filename2':
                exp_metadata_results[faire_col] = self.run_metadata_df.apply(
                    lambda row: self.get_cheksums(metadata_row=row, raw_file_dict=self.raw_filename2_dict),
                    axis = 1
                )
            elif faire_col == 'input_read_count':
                exp_metadata_results[faire_col] = self.run_metadata_df.apply(
                    lambda row: self.process_paired_end_fastq_files(metadata_row=row),
                    axis=1
                )
            elif faire_col == 'output_read_count' or faire_col == faire_col == 'output_otu_num' or faire_col == 'otu_num_tax_assigned':
                exp_metadata_results[faire_col] = self.run_metadata_df.apply(
                    lambda row: self.process_asv_counts(metadata_row=row, faire_col=faire_col),
                    axis=1
                )
            elif faire_col == 'associatedSequences':
                bio_accession_cols = metadata_col.split(' | ')
                exp_metadata_results[faire_col] = self.run_metadata_df.apply(
                    lambda row: self.get_bioaccession_nums_from_metadata(metadata_row=row, bioaccession_cols=bio_accession_cols),
                    axis=1
                )
           
        exp_df = pd.DataFrame(exp_metadata_results)
        faire_exp_df = pd.concat([self.exp_run_faire_template_df, exp_df])

        # Create non-curated asv tables (rawOtu)
        self.create_non_curated_osu_tables(final_exp_df=faire_exp_df)

        return faire_exp_df

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
    
    def transform_pos_samp_name_in_metadata(self, metadata_row: pd.Series) -> str:
        # update positive sample names
        if 'POSITIVE' in metadata_row[self.run_metadata_sample_name_column]:
            marker = marker_to_shorthand_mapping.get(metadata_row[self.run_metadata_marker_col_name])
            if 'osu' not in self.run_name.lower():
                return f"{self.run_name}.{marker}.POSITIVE"
            else: # for osu controls that have camel and ferret in them
                return f"{self.run_name}.{marker}.{metadata_row[self.run_metadata_sample_name_column]}"
        else:
            return metadata_row[self.run_metadata_sample_name_column]
    
    def _create_experiment_metadata_df(self) -> pd.DataFrame:

        exp_df = self.load_google_sheet_as_df(google_sheet_id=self.run_sample_metadata_file_id, sheet_name=self.run_metadata_sample_sheet_name, header=0)

        # filter rows if ignore_markers is present using str.contains with regex OR operator (|)
        if self.ignore_markers:
            pattern = '|'.join(self.ignore_markers)
            exp_df = exp_df[~exp_df[self.run_metadata_marker_col_name].str.contains(pattern, case=True, na=False)]
      
        # Fix sample names that mismatch from sequencing metatdata, to raw files, to asv counts in the metadata file
        exp_df[self.run_metadata_sample_name_column] = exp_df[self.run_metadata_sample_name_column].apply(str_replace_for_samps)

        # Change positive control sample names
        exp_df[self.run_metadata_sample_name_column] = exp_df.apply(
            lambda row: self.transform_pos_samp_name_in_metadata(metadata_row=row),
            axis=1
        )

        # Remove E1-23 samples from Machida in the OSU 867 and 873 runs
        if '867' in self.run_name or '873' in self.run_name:
            exp_df = self.drop_E1_23_Machida_samples(df=exp_df)

        # Update cruise code by e-number
        exp_df = self.change_or_add_cruise_codes_by_e_num(df = exp_df)

        # Remove rows with EMPTY, NA, or '' as sample name, and 'sample/primer' (for Run3 the random statistic row in the metadata file)
        exp_df = exp_df[
            (~exp_df[self.run_metadata_sample_name_column].str.contains('EMPTY|sample/primer', case=False, na=False)) &
            (exp_df[self.run_metadata_sample_name_column].notna()) &
            (exp_df[self.run_metadata_sample_name_column] != '')
        ].reset_index(drop=True)
       
        return exp_df

    def _get_md5_checksum(self, filepath: str):

        md5_hash = hashlib.md5()

        with open(filepath, 'rb') as f:
            # Read files in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b''):
                md5_hash.update(chunk)

        return md5_hash.hexdigest()
    
    def _outline_raw_data_dict(self, sample_name: str, file_num: int, all_files: list, marker: str, marker_dir: str) -> dict:
        # outlines raw filename dictionary used by create_marker_sample_raw_data_files for filename and filename2 in FAIRe
        # Account for sample name changes in positive samples
        if 'POSITIVE' not in sample_name:
            # checks for mismatch in sample names using mismatch_sample_names_metadata_to_raw_data_files_dict
            for k, v in mismatch_sample_names_metadata_to_raw_data_files_dict.items():
                if k in sample_name:
                    sample_name_lookup = sample_name.replace(k, v)
                    # break out of loop after first match
                    break
                else:
                    sample_name_lookup = sample_name

            # Had to add this because the .NO201 in the mismatch_sample_names_metadata_to_raw_data_files_dict was causing problems
            # same with the Mid.NC.SKQ21
            # TODO: remove this after NO20 is finalized and moving onto new version of FAIRe-Mapper (wont' be backwards compatible)
            if 'E265.1B' in sample_name:
                sample_name = 'E265.1B.NO20-01'
                sample_name_lookup = 'E265.IB.NO20'
            if sample_name == 'MID.NC.SKQ21-15S':
                sample_name_lookup = 'Mid.NC.SKQ21'
            
            pattern = re.compile(f"^{re.escape(sample_name_lookup)}[_.]R[{file_num}].+")

        else:
            pattern = re.compile(f"^{re.escape('POSITIVE')}[_.]R[{file_num}].+")

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
                target_dict[marker][sample_name.strip()] = {
                    "filename": filename,
                    "checksum": md5_checksum,
                    "filepath": filepath
                }
        elif not matching_files:
            # try reformatting sample name (for osu runs mainly) and redo steps above
            sample_name_lookup = self._try_diff_sample_name_for_raw_data_lookup(sample_name)
            if 'camel' in sample_name.lower() or 'ferett' in sample_name.lower() or 'ferret' in sample_name.lower():
                sample_name_lookup = sample_name_lookup.split('_')[-1]
                pattern = re.compile(f"^{re.escape(f'MP_{sample_name_lookup}')}[_.]R[{file_num}].+")
            else:
                pattern = re.compile(f"^{re.escape(sample_name_lookup)}[_.]R[{file_num}].+")
            matching_files = [f for f in all_files if pattern.match(f)]
            
            # Sometimes Ferett is oppositive of _try_diff_sample_name function :/
            if not matching_files and 'Ferett' in sample_name_lookup:
                sample_name_lookup = sample_name_lookup.replace('Ferett', 'Ferret')
                pattern = re.compile(f"^{re.escape(f'MP_{sample_name_lookup}')}[_.]R[{file_num}].+")
                matching_files = [f for f in all_files if pattern.match(f)]


            if marker not in target_dict:
                target_dict[marker] = {}

            for filename in matching_files:
                filepath = os.path.join(marker_dir, filename)
                md5_checksum = self._get_md5_checksum(filepath)
                # change sample name back for key of dict for DY2012 samps
                target_dict[marker][sample_name.strip()] = {
                    "filename": filename,
                    "checksum": md5_checksum,
                    "filepath": filepath
                }
        else:
            print(f"Warning: No matching files found for sample {sample_name} in marker {marker}, using sample lookup name {sample_name_lookup}, pattern {pattern}.")
    
    def _create_marker_sample_raw_data_file_dicts(self):
        # Finds all matching data files by marker for each sample returns two nested dict one for forward raw data files, and one for reverse raw data files
        # {marker1: {sample1: {filename: file1.gz, checksum: 56577, filepath: eff/dsdf.fastq.gz}}
        # group run metadata by marker
        grouped = self.run_metadata_df.groupby(self.run_metadata_marker_col_name)

        for marker, group in grouped:
           # Get the raw data folder name by shorthand marker - see dict in lists.py. If 
            shorthand_marker = marker_to_shorthand_mapping.get(marker, None)
            if shorthand_marker == None:
                raise ValueError(f"No shorthand marker exists for {marker}, please update shorthand_marker dict in lists file")
            marker_raw_data_dir = self.run_raw_data_path.get(shorthand_marker)
            if os.path.exists(marker_raw_data_dir):
                print(f"raw data for {shorthand_marker} in folder: {marker_raw_data_dir}")
            else:
                raise ValueError(f"{marker_raw_data_dir} does not exist!")
            
            # Get all filenames in the direcotry
            all_files = os.listdir(marker_raw_data_dir)

            # For each sample in this marker group
            for sample_name in group[self.run_metadata_sample_name_column].unique():
                sample_name = sample_name.strip() # remove white spaces from sample name - having issues with this
                
                self._outline_raw_data_dict(sample_name=sample_name, file_num=1, all_files=all_files, marker=marker, marker_dir=marker_raw_data_dir)
                self._outline_raw_data_dict(sample_name=sample_name, file_num=2, all_files=all_files, marker=marker ,marker_dir=marker_raw_data_dir)

    def convert_assay_to_standard(self, marker: str) -> str:
        # matches the marker to the corresponding assay and returns standardized assay name
        
        potential_assays = marker_to_assay_mapping.get(marker, None)
        if isinstance(potential_assays, list):
            # Get the OSU assay name if OSU in the run name
            if 'osu' in self.run_name.lower():
                for assay in potential_assays:
                    if 'osu' in assay.lower():
                        return assay
                raise NoAcceptableAssayMatch(f'The marker/assay {marker} does not match any of the {[v for v in marker_to_assay_mapping.values()]}, for an osu assay, please update marker_to_assay_mapping dict in list file.')
            # else get the other assay name that does not have OSU in the run name if the run name is not an osu assay, but has an osu sibling assay
            else:
                for assay in potential_assays:
                    if 'osu' not in assay.lower():
                        return assay
                raise NoAcceptableAssayMatch(f'The marker/assay {marker} does not match any of the {[v for v in marker_to_assay_mapping.values()]} for a non-osu assay, please update marker_to_assay_mapping dict in list file.')
        # else if potential_assays is not a list, then just get the value
        else:
            assay = potential_assays
            if assay == None:
                raise NoAcceptableAssayMatch(f'The marker/assay {marker} does not match any of the {[v for v in marker_to_assay_mapping.values()]}, please update marker_to_assay_mapping dict in list file.')
            else:
                return assay

    def create_lib_id(self, metadata_row: pd.Series) -> str:
        # create lib_id by concatenating sample name, index, marker and run name together

        sample_name = metadata_row[self.run_metadata_sample_name_column]
        shorthand_marker = marker_to_shorthand_mapping.get(metadata_row[self.run_metadata_marker_col_name])

        lib_id = sample_name + "_" + shorthand_marker + "_" + self.run_name

        return lib_id

    def get_raw_file_names(self, metadata_row: pd.Series, raw_file_dict: dict) -> tuple:
        # Gets the name of the raw file based on sample name and marker
     
        marker_name = metadata_row[self.run_metadata_marker_col_name].strip()
        sample_name = metadata_row[self.run_metadata_sample_name_column].strip()
        lib_id = metadata_row['lib_id']

        try:
            # Get approrpiate nested marker dict and corresponding nested sample list with dictionary of files and checksums
            filepath = raw_file_dict.get(marker_name).get(sample_name).get('filepath')
        except:
            raise ValueError(f"sample name {sample_name} with marker {marker_name} does not appear to have a value in the raw data dict, please look into")
        try:     
            # Update file name to be unique (for the ncbi sumbissions) and copy raw data file to /home/poseidon/zalmanek/raw_data_copies if it doesn't yet exist
            updated_filename = self.create_raw_file_copy_with_unique_name(original_file_path=filepath, lib_id=lib_id)
        except:
            raise ValueError(f"Problem creating a copy of the raw file for {sample_name} for {marker_name}")

        return updated_filename
    
    def get_cheksums(self, metadata_row: pd.Series, raw_file_dict: dict) -> tuple:
        # Gets the checksum of the raw file based on sample name and marker
     
        marker_name = metadata_row[self.run_metadata_marker_col_name].strip()
        sample_name = metadata_row[self.run_metadata_sample_name_column].strip()

        # Get approrpiate nested marker dict and correspongind nested sample list with dictionary of files and checksums
        checksum = raw_file_dict.get(marker_name).get(sample_name).get('checksum')

        return checksum

    def _count_fastq_files_bioawk(self, fastq_file):
        # Use Bioawk to count FASTQ records accurately regardless of line wrapping
        # Returns the count of records in the FASTQ file

        if not os.path.exists(fastq_file):
            print(f"Error: File {fastq_file} does not exist.")

        cmd = f"bioawk -t -c fastx 'END {{print NR}}' {fastq_file}"

        try:
            # Run the command and capture the output
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

            # check if command executed successfully
            if result.returncode != 0:
                print(f"Error executing bioawk {result.stderr}")
            
            # parse the output to get the count
            count = int(result.stdout.strip())
            return count
        
        except Exception as e:
            print(f"Error executing command: {e}")
    
    def _clean_asv_samp_names(self, sample_name: str, marker: str) -> str:
        # Cleans sample names in asv data to match other data (e.g. removed MP_ and replaces other _ with .)
        if 'positive' not in sample_name.lower():
            if 'pool' in sample_name:
                return sample_name.replace('MP_', '')
            if 'ferret' in sample_name.lower() or 'ferett' in sample_name.lower() or 'camel' in sample_name.lower(): # for osu positive samples
                sample_name = sample_name.replace('Ferett', 'Ferret').replace('MP_','')
                sample_name = f'{self.run_name}.{marker}.POSITIVE.{sample_name}'
                return sample_name
            else: # For regular samples
                sample_name =  sample_name.replace('MP_', '').replace('_', '.').replace('.12S', '-12S').replace('Mid', 'MID').replace('DY2306', 'DY23-06').replace('DY2209', 'DY22-09').replace('DY2206', 'DY22-06') # replace .12S to -12S for SKQ23-12S samples.
                for old, new in update_cruise_codes.items():
                    sample_name = sample_name.replace(old, new)
                # update based on E number
                sample_name = self._clean_asv_samp_names_by_e_num(sample_name=sample_name)
                return sample_name
        else: # for regular positive samples (JV runs)
            sample_name = sample_name.replace('MP_', '')
            return f'{self.run_name}.{marker}.{sample_name}' 
    
    def _try_diff_sample_name_for_raw_data_lookup(self, sample_name: str) -> str:
        # Transform sample names to different value and try with raw data files - for OSU runs
        if 'POSITIVE' in sample_name:
            sample_name = sample_name.replace('POSITIVE', '')
            if 'ferret' in sample_name.lower():
                sample_name = sample_name.replace('Ferret', 'Ferett')
        # try diffierent (old cruise code) cruise code because it may have been updated
        if '.DY20-12' in sample_name:
            sample_name = sample_name.replace('.DY20-12', '.DY20')
        else:
            for old, new in update_cruise_codes.items():
                sample_name = sample_name.replace(new, old)
        sample_name = 'MP_' + sample_name.replace('.', '_')
   
        return sample_name
    
    def _fix_sample_names_for_asv_lookup(self, sample_name: str) -> str:
        for old, new in update_cruise_codes.items():
            sample_name = sample_name.replace(old, new)
        return sample_name

    def _read_asv_counts_tsv(self, asv_tsv_file: str) -> pd.DataFrame:
        # Reads the asv counts tsv and returns a df
        asv_tsv_file = os.path.abspath(asv_tsv_file)
        if not os.path.exists(asv_tsv_file):
            raise ValueError(f"File not found: {asv_tsv_file}")

        # Read the TSV file with pandas
        asv_df = pd.read_csv(asv_tsv_file, sep='\t')

        # set the asv column as the index
        asv_df = asv_df.set_index('x')

        return asv_df
    
    def _create_count_asv_dict_for_non_merged_asvs(self, asv_df: pd.DataFrame, marker: str):
        # Creates the count asv dict for singular asvs per marker (not merged), so everything except Run3, OSU runs. 
        # calculate sum for each filtered column and the otu num
        for sample_name in asv_df.columns:
            
            # fix sample names if they mismatch in metadata for asv counts look up
            sample_name = self._fix_sample_names_for_asv_lookup(sample_name)
        
            # sum the column values for output read count
            output_read_count = asv_df[sample_name].sum()

            # get the output_otu_num which is the total number asvs for each sample (non zero count number)
            non_zero_output_asv_num = (asv_df[sample_name] > 0).sum()

            # update sample names to match rest of data
            updated_sample_name = self._clean_asv_samp_names(sample_name=sample_name, marker=marker)
            
            # store output_read_count in the nested dictionary
            self.asv_data_dict[marker][updated_sample_name] = {'output_read_count': output_read_count.item(), 
                                                            'output_otu_num': non_zero_output_asv_num.item()}
            

            # Add sample names to dictionary (for curating otu tables)
            if marker not in self.asv_samp_name_dict:
                self.asv_samp_name_dict[marker] = {sample_name: updated_sample_name}
            else:
                self.asv_samp_name_dict[marker][sample_name] = updated_sample_name

    def _create_count_asv_dict_for_merged_asvs(self, single_asv_df: pd.DataFrame, merged_asv_df: pd.DataFrame, marker: str):
        # Creates the count asv dict for merged  asvs per marker. So the output_read_count will be from the origin run asv table and the 
        # output_otu_num will come from the merged tsv
        # Get the output_read_count from the single file
        for sample_name in single_asv_df.columns:
            
            # fix sample names if they mismatch in metadata for asv counts look up
            sample_name = self._fix_sample_names_for_asv_lookup(sample_name)
        
            # sum the column values for output read count
            output_read_count = single_asv_df[sample_name].sum()

            # update sample names to match rest of data
            updated_sample_name = self._clean_asv_samp_names(sample_name=sample_name, marker=marker)
            
            # store output_read_count in the nested dictionary
            self.asv_data_dict[marker][updated_sample_name] = {'output_read_count': output_read_count.item()}

            # Add sample names to dictionary (for curating otu tables)
            if marker not in self.asv_samp_name_dict:
                self.asv_samp_name_dict[marker] = {sample_name: updated_sample_name}
            else:
                self.asv_samp_name_dict[marker][sample_name] = updated_sample_name

        
        # Get the output_otu_num from the merged file
        for sample_name in merged_asv_df.columns:
            # fix sample names if they mismatch in metadata for asv counts look up
            sample_name = self._fix_sample_names_for_asv_lookup(sample_name)
            
            # get the output_otu_num which is the total number asvs for each sample (non zero count number)
            non_zero_output_asv_num = (merged_asv_df[sample_name] > 0).sum()

             # update sample names to match rest of data
            updated_sample_name = self._clean_asv_samp_names(sample_name=sample_name, marker=marker)

            # store output_otu_num in the nested dictionary
            try:
                # will fail if tryingto add samples from other runs (e.g. Run3 positive sample because have not been added to the dictionary)
                self.asv_data_dict[marker][updated_sample_name]['output_otu_num'] = non_zero_output_asv_num.item()
            except:
                # If it didn't exist in the dictionary then it will add the output_read_count of 0 and then add the ouptut_otu_num
                self.asv_data_dict[marker][updated_sample_name] = {'output_read_count': 0,
                                                                   'output_otu_num': non_zero_output_asv_num.item()}

    def _create_count_asv_dict(self, revamp_blast: bool = True) -> dict:
        # TODO: add functionality of revamp_bast is not True?
        # Create  a nested dictionary of marker, sample names and counts
        # {marker: {E56. : {output_read_count: 5, output_otu_num: 8}}}
        # if revamp_blast is True will run _count_otu_tax_assigned_using_REVAMP_blast_based_tax to add the otu_num_tax_assigned

        for marker, asv_tsv_file_s in self.asv_counts_tsvs_for_run.items():
            if isinstance(asv_tsv_file_s, str):
                asv_tsv_file = asv_tsv_file_s
                # Initialize marker in the nested dictionary
                self.asv_data_dict[marker] = {}
                asv_df = self._read_asv_counts_tsv(asv_tsv_file=asv_tsv_file)
                self._create_count_asv_dict_for_non_merged_asvs(asv_df=asv_df, marker=marker)
                
            elif isinstance(asv_tsv_file_s, dict):
                # Initialize marker in the nested dictionary
                self.asv_data_dict[marker] = {}
                singular_asv_df = self._read_asv_counts_tsv(asv_tsv_file=asv_tsv_file_s['secondary_path']) # for calculating output_read_count on the run database level
                merged_asv_df = self._read_asv_counts_tsv(asv_tsv_file=asv_tsv_file_s['merged']) # for calculating output_otu_num (will include merged numbers from other runs - not necessary for NCBI)
                self._create_count_asv_dict_for_merged_asvs(single_asv_df=singular_asv_df, merged_asv_df=merged_asv_df, marker=marker)

        # Since getting the otu_num_tax_assigned depends on which version of the taxonomy we are using for the submission
        # if revamp_blast == True then use the ASVs_counts_mergedOnTaxonomy.tsv file to get this info.
        if revamp_blast == True:
            self._count_otu_tax_assigned_using_REVAMP_blast_based_tax(merged = self.merged)

    def _count_otu_tax_assigned_using_REVAMP_blast_based_tax(self, merged = False):
        # Calculates the otu_num_tax_assined from the ASVs_counts_mergedOnTaxonomy.tsv file
        # merged = True only if using multiple files for counts (e.g. Run3 and OSU runs that are merged) because there will be samples that do not need to get added to dict
        # (e.g. the Positive sample in the Run3 merged file for the OSU runs - not relevant to those runs)
        for marker, asv_tax_file in self.otu_num_tax_assigned_files_for_run.items():

            # Read the TSV file with pandas
            asv_tax_df = pd.read_csv(asv_tax_file, sep='\t')

            # set the asv column as the index
            asv_tax_df = asv_tax_df.set_index('x')

            for sample_name in asv_tax_df.columns:

                # fix sample names if they mismatch in metadata for asv counts look up
                sample_name = self._fix_sample_names_for_asv_lookup(sample_name)

                # get the output_otu_num which is the total number asvs for each sample (non zero count number)
                non_zero_otu_num_tax_assinged = (asv_tax_df[sample_name] > 0).sum()

                # update sample names to match rest of data
                updated_sample_name = self._clean_asv_samp_names(sample_name=sample_name, marker=marker)

                if merged == False:
                    self.asv_data_dict[marker][updated_sample_name]["otu_num_tax_assigned"] = non_zero_otu_num_tax_assinged.item()
                else:
                    try: 
                        self.asv_data_dict[marker][updated_sample_name]["otu_num_tax_assigned"] = non_zero_otu_num_tax_assinged.item()
                    except:
                        print(f"sample: {updated_sample_name} is missing from asv_data_dict - double check that this sample is not part of this merged Run!")
                        pass

    def process_paired_end_fastq_files(self, metadata_row: pd.Series) -> int:
        # Process R1 FASTQ file using exact file path

        # Get the filepath to the 
        r1_file_path = self.raw_filename_dict.get(metadata_row[self.run_metadata_marker_col_name].strip()).get(metadata_row[self.run_metadata_sample_name_column].strip()).get('filepath')

        input_read_count = self._count_fastq_files_bioawk(r1_file_path)

        return input_read_count
    
    def process_asv_counts(self, metadata_row: pd.Series, faire_col) -> str or int:
        # Get the count (either output_read_count, output_otu_num, or otu_num_tax_assigned) from the asv_data_dict based on sample name and marker

        # normalize marker to shorthand
        marker = marker_to_shorthand_mapping.get(metadata_row[self.run_metadata_marker_col_name])
        sample_name = metadata_row[self.run_metadata_sample_name_column]
        # Get the count
        try:
            count = self.asv_data_dict.get(marker).get(sample_name.strip()).get(faire_col)
        except AttributeError:
            # If there is a key error because sample_name does not exist in the dictionary, try searching for a part of the sample name (the sample names most likely mismatch a little bit)
            # This breaks the sample name into its E number, Tech rep (if exists), and bio rep (if exists) and puts those pieces into a list, then checks the asv_data_dict for keys that include
            # all of those pieces (so will essentially check the dict for keys with the same E number, bio rep, and tech rep (will ignore cruise code or underscores/periods)
            sample_name = sample_name.strip()
            if '.' in sample_name:
                sample_name_bits = sample_name.split('.')
                e_number = sample_name_bits[0]

                samp_bit_without_cruise_code = [e_number]
                for i, bit in enumerate(sample_name_bits):
                    if 'PCR' in bit:
                        tech_rep = bit
                        samp_bit_without_cruise_code.append(tech_rep)
                    if 'B' in bit:
                        if tech_rep and i != len(sample_name_bits) - 2: # if there is a technical rep, the cruise code comes before the PCR part, so will be -2 (don't want B to be in the cruise code) 
                            bio_rep = bit
                        if not tech_rep and i != len(sample_name_bits) - 1: # check for B, but make sure its not the last item (this could be in the cruise code)
                            bio_rep = bit
                        samp_bit_without_cruise_code.append(bio_rep)

                for samp_name, count_data in self.asv_data_dict[marker].items():
                    if all(samp_bit in samp_name for samp_bit in samp_bit_without_cruise_code):
                        count = count_data.get(faire_col)
                        if count:
                            print(f'\033[32m{sample_name} cant find a match in the asv_data_dict exactly for marker {marker}, but did find a match based on similarity: {samp_name}. If this is incorrect please look into!\033[0m')
                        break
                    else:
                        count = 0
                        
        
        except Exception as e:
            print(f"No count data for {sample_name} with {marker} {e}")
            count = 0

        return count

    def drop_E1_23_Machida_samples(self, df: pd.DataFrame) -> pd.DataFrame:
        # Removes samples from metadata. For example sample E1-23 need to be removed from OUS867 and 873 runs because the had low reads
        
        # creates filter for df for rows that start with E followed by numbers 1-23
        e_number_filter = df[self.run_metadata_sample_name_column].apply(lambda x: bool(re.match(r'^E(?:[1-9]|1[0-9]|2[0-3])\b', str(x))))
        # marker filter
        marker_filter = df[self.run_metadata_marker_col_name] == '18S Machida'
        # Combine filters
        rows_to_remove = e_number_filter & marker_filter
        # keep all rows that dont' match above condition
        df_filtered = df[~rows_to_remove]
        return df_filtered

    def get_bioaccession_nums_from_metadata(self, metadata_row: pd.Series, bioaccession_cols: list) -> str:

        srr_url = 'https://www.ncbi.nlm.nih.gov/sra/' #run (SRR)
        samn_url = 'https://www.ncbi.nlm.nih.gov/biosample/' #biosample (SAMN)
        prjna_url = 'https://www.ncbi.nlm.nih.gov/bioproject/' #bioproject (PRJNA)

        # Gets the bio accession numbers from various columns in the metadata row
        bioaccessions = []
        for col in bioaccession_cols:
            bioaccession_id = metadata_row.get(col)
            if bioaccession_id != '':
                # prepend url to id
                if 'SRR' in bioaccession_id:
                    bioaccession_id = srr_url + bioaccession_id
                elif 'SAMN' in bioaccession_id:
                    bioaccession_id = samn_url + bioaccession_id
                elif 'PRJNA' in bioaccession_id:
                    bioaccession_id = prjna_url + bioaccession_id
                else:
                    raise ValueError(f"bioaccession id {bioaccession_id} does not seem to have SRR, SAMN, or PRJNA in its name, so can't prepend urls")
                
                bioaccessions.append(bioaccession_id)

        if bioaccessions:
            formatted_bioaccessions = ' | '.join(bioaccessions)
            return formatted_bioaccessions

    def create_non_curated_osu_tables(self, final_exp_df: pd.DataFrame):
        # creates the non_curated osu_tables - it will be a otu_table.csv per seq_run_id per assay_name - so multiple per sequencing run

        seq_run_id = self.mapping_dict[self.constant_mapping].get(self.faire_seq_run_id_col)
        
        for marker, file_path in self.asv_counts_tsvs_for_run.items(): 
            # for OSU and run3, need to use merged path to get the asv tables
            if isinstance(file_path, dict):
                file_path = file_path.get('merged')
            else:
                file_path = file_path
           
            asv_table = self._read_asv_counts_tsv(asv_tsv_file=file_path)
            
            # Get ASVs.fa file to get DNA sequences
            asv_tsv_path = Path(file_path)
            asvs_fa_path = asv_tsv_path.parent / "ASVs.fa"

            # rename samples to match the updated sample names
            asv_table_samples_updated = asv_table.rename(columns=self.asv_samp_name_dict.get(marker))

            # replace ASV names with hash
            asv_hash_dict = self.create_asv_hash_dict(asvs_fasta_path=asvs_fa_path)
            asv_table_hash_updated = asv_table_samples_updated.rename(index=asv_hash_dict)

            # This part is primarly for OSU/run3 where there will be extra samples that aren't related
            # Drops all samples in non_curated osu_table that does not exist in the final exp_df. And removes any hashes with 0 for all columns
            valid_samples = final_exp_df[self.faire_sample_samp_name_col].tolist()
            asv_table_has_updated_filtered = asv_table_hash_updated[asv_table_hash_updated.columns.intersection(valid_samples)]
            asv_table_final = asv_table_has_updated_filtered[(asv_table_has_updated_filtered != 0).any(axis=1)]
            asv_table_final.index.name = 'seq_id'

            # save the csv file
            assay_name = self.convert_assay_to_standard(marker=marker)
            non_curated_csv_path = (Path(self.final_faire_template_path)).parent / f"otuRaw_{assay_name}_{seq_run_id}.csv"
            asv_table_final.to_csv(non_curated_csv_path, index='seq_id')
            print(f"Saved {assay_name} otuRaw.csv")

    def create_asv_hash_dict(self, asvs_fasta_path: str):
        # creates a dictionary like {'ASV1': laksdjfalksdjfalksdjf}
        asv_hash_dict = {}
        current_asv = None
        sequence = ''

        print(f"Creating ASV hash dict for {asvs_fasta_path}")
        with open(asvs_fasta_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('>'):
                    #Process previous sequence if exists
                    if current_asv and sequence:
                        seq_hash = hashlib.md5(sequence.encode()).hexdigest()
                        asv_hash_dict[current_asv] = seq_hash

                    # Extract ASV number from header
                    current_asv = line[1:] # Remove '>' character
                    sequence = ""
                else:
                    sequence += line

            # Process the last sequence
            if current_asv and sequence:
                seq_hash = hashlib.md5(sequence.encode()).hexdigest()
                asv_hash_dict[current_asv] = seq_hash

        return asv_hash_dict

    def change_or_add_cruise_codes_by_e_num(self, df: pd.DataFrame) -> pd.DataFrame:
        # Changes the cruise code, or possibly adds the cruise code to the sample name based on the E number
        e_nums = df[self.run_metadata_sample_name_column].str.extract(r'E(\d+)')[0].astype('Int64')

        # for M2-PPS-0423 sample names E1820 - E1842
        mask = (e_nums >= 1820) & (e_nums <= 1842)
        df.loc[mask, self.run_metadata_sample_name_column] = df.loc[mask, self.run_metadata_sample_name_column].str.replace('.DY23-06', '.M2-PPS-0423')

        # for AquaM sample names
        mask = (e_nums == 2084) | (e_nums == 2090) | (e_nums == 2097)
        df.loc[mask, self.run_metadata_sample_name_column] = df.loc[mask, self.run_metadata_sample_name_column].str.replace('.SKQ23-12S', '.CEO-AquaM-0923')  

        return df

    def _clean_asv_samp_names_by_e_num(self, sample_name: str) -> str:
        # updates sample name cruise codes by eNumber
        # extract E number
        e_num = re.search(r'E(\d+)', sample_name)
        if e_num:
            number = int(e_num.group(1))
            if 1820 <= number <= 1842: # For the M2-PPS-0423 cruise will need to specifically update these E-numbers
                sample_name = sample_name.replace('DY23-06', 'M2-PPS-0423')
            if 2084 == number or 2090 == number or 2097 == number: # For AquaM samples
                sample_name = sample_name.replace('SKQ23-12S', 'CEO-AquaM-0923')
            if 2030 == number:
                sample_name = 'E2030.NC.SKQ23-12S'

        return sample_name
        
    def create_raw_file_copy_with_unique_name(self, original_file_path: str, lib_id: str):
        # Cretes a copy of the raw data file in zalmanek/raw_data_copies/run_name if it doesn't exist with a new unique name that is the lib_id
        original_file_path = Path(original_file_path)
        
        # Check if file exists
        if not original_file_path.exists():
            raise ValueError(f"Error: File: {original_file_path} does not exist to make a copy of")
        
        filename = original_file_path.name

        # Use Regex to find _R1 or _R2 pattern
        match = re.search(r'(_R[12].*)', filename)

        if not match:
            # Run1 has periods instead of underscores, sigh, so we have to check that next
            match = re.search(r'(\.R[12].*)', filename)
            if not match:
                raise ValueError(f"File: {filename} does not contain _R1 or _R2 in the file name. Can not make a copy of the file!")
        
        # Extract suffix (everything from _R1 or _R2 onwards)
        suffix = match.group(1)

        # Create new filename with the lib id instead
        new_filename = f"{lib_id}{suffix}"

        # Create target directory structure
        target_dir = Path('/home/poseidon/zalmanek/raw_data_copies') / self.run_name
        target_file = target_dir / new_filename

        # Check if targe_file already exists
        if target_file.exists():
            print(f"Skipping copying raw data file: File '{new_filename} already exists")
            return new_filename

        # Create directory if it doesn't exist
        target_dir.mkdir(parents=True, exist_ok=True)
        try:
            # Copy file with new name
            shutil.copy2(original_file_path, target_file)
            print(f"\033[92mSuccussfully copied '{filename} to {target_file}\033[0m")
            return new_filename
        except Exception as e:
            print(f"\033[91mError copying file: {e}\033[0m")
            return False

        

        