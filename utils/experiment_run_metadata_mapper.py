from .faire_mapper import OmeFaireMapper
from .lists import marker_to_assay_mapping, marker_to_shorthand_mapping
from .custom_exception import NoAcceptableAssayMatch
import yaml
import pandas as pd
import os
import subprocess
import re
import hashlib

# TODO: update for PCR replicates? - MAke sample name the same, change lib_id?
# TODO: add associatedSequences functionlity after submittting to NCBI

class ExperimentRunMetadataMapper(OmeFaireMapper):

    experiment_run_mapping_sheet_name = "experimentRunMetadata"
    jv_metadata_sample_sheet_name = "Sample Data Sheet Long"
    jv_metadata_marker_col_name = "Metabarcoding Marker"
    faire_template_exp_run_sheet_name = "experimentRunMetadata"

    def __init__(self, config_yaml: yaml):
        super().__init__(config_yaml)

        self.jv_run_sample_name_column = self.config_file['jv_run_sample_name_column']
        self.jv_index_name_col = self.config_file['jv_index_name_col']
        self.faire_sample_samp_name_col = 'samp_name' # The name of the column for sample name in the 
        self.jv_raw_data_path = self.config_file['jv_raw_data_path']
        self.jv_run_sample_metadata_file_id = self.config_file['jv_run_sample_metadata_file_id']
        self.run_name = self.config_file['run_name']
        self.asv_counts_tsvs_for_run = self.config_file['asv_counts_tsvs_for_run']
        self.otu_num_tax_assigned_files_for_run = self.config_file['otu_num_tax_assigned_files_for_run']
        self.ignore_markers = self.config_file['ignore_markers']
        self.google_sheet_mapping_file_id = self.config_file['google_sheet_mapping_file_id']

        self.mapping_dict = self._create_experiment_run_mapping_dict()
        self.jv_run_metadata_df = self._create_experiment_metadata_df()
        
        # populate data dicts
        self.raw_filename_dict = {} # dictionary of forward raw data files with checksums
        self.raw_filename2_dict = {} # dictionary of reverse raw data files with checksums
        self.asv_data_dict =  {}
        self._create_marker_sample_raw_data_file_dicts() 
        self._create_count_asv_dict(revamp_blast=self.config_file['revamp_blast'])
        self.rel_pos_cont_id_dict = self._create_positive_samp_dict()

        self.exp_run_faire_template_df = self.load_faire_template_as_df(file_path=self.config_file['faire_template_file'], sheet_name=self.faire_template_exp_run_sheet_name, header=self.faire_sheet_header).dropna()

    def generate_jv_run_metadata(self) -> pd.DataFrame :
        # Works for run2, need to check other jv runs - maybe can potentially use for OSU runs, if mapping file is generically the same?
        exp_metadata_results = {}
    
        # Step 1: Add exact mappings
        for faire_col, metadata_col in self.mapping_dict[self.exact_mapping].items():
            exp_metadata_results[faire_col] = self.jv_run_metadata_df[metadata_col].apply(
                lambda row: self.apply_exact_mappings(metadata_row=row, faire_col=faire_col))
        
        # Step 2: Add constants
        for faire_col, static_value in self.mapping_dict[self.constant_mapping].items():
            exp_metadata_results[faire_col] = self.apply_static_mappings(faire_col=faire_col, static_value=static_value)

        # Step 3: Add related mappings
        for faire_col, metadata_col in self.mapping_dict[self.related_mapping].items():
            # Add assay_name
            if faire_col == 'assay_name':
                exp_metadata_results[faire_col] = self.jv_run_metadata_df[metadata_col].apply(self.convert_assay_to_standard)

            elif faire_col == 'lib_id':
                exp_metadata_results[faire_col] = self.jv_run_metadata_df.apply(
                    lambda row: self.jv_create_lib_id(metadata_row=row),
                    axis=1
                )
            elif faire_col == 'filename':
                exp_metadata_results[faire_col] = self.jv_run_metadata_df.apply(
                    lambda row: self.get_raw_file_names(metadata_row=row, raw_file_dict=self.raw_filename_dict),
                    axis=1
                )
            elif faire_col =='filename2':
                exp_metadata_results[faire_col] = self.jv_run_metadata_df.apply(
                    lambda row: self.get_raw_file_names(metadata_row=row, raw_file_dict=self.raw_filename2_dict),
                    axis=1
                )
            elif faire_col == 'checksum_filename':
                exp_metadata_results[faire_col] = self.jv_run_metadata_df.apply(
                    lambda row: self.get_cheksums(metadata_row=row, raw_file_dict=self.raw_filename_dict),
                    axis = 1
                )
            elif faire_col == 'checksum_filename2':
                exp_metadata_results[faire_col] = self.jv_run_metadata_df.apply(
                    lambda row: self.get_cheksums(metadata_row=row, raw_file_dict=self.raw_filename2_dict),
                    axis = 1
                )
            elif faire_col == 'input_read_count':
                exp_metadata_results[faire_col] = self.jv_run_metadata_df.apply(
                    lambda row: self.process_paired_end_fastq_files(metadata_row=row),
                    axis=1
                )
            elif faire_col == 'output_read_count' or faire_col == faire_col == 'output_otu_num' or faire_col == 'otu_num_tax_assigned':
                exp_metadata_results[faire_col] = self.jv_run_metadata_df.apply(
                    lambda row: self.process_asv_counts(metadata_row=row, faire_col=faire_col),
                    axis=1
                )

        exp_df = pd.DataFrame(exp_metadata_results)
        faire_exp_df = pd.concat([self.exp_run_faire_template_df, exp_df])

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
       
        if 'POSITIVE' in metadata_row[self.jv_run_sample_name_column]:
            marker = marker_to_shorthand_mapping.get(metadata_row[self.jv_metadata_marker_col_name])
            return f"{self.run_name}.{marker}.POSITIVE"
        else:
            return metadata_row[self.jv_run_sample_name_column]

    def _create_experiment_metadata_df(self) -> pd.DataFrame:
        # TODO: maybe rethink th

        exp_df = self.load_google_sheet_as_df(google_sheet_id=self.jv_run_sample_metadata_file_id, sheet_name=self.jv_metadata_sample_sheet_name, header=0)

        # filter rows if ignore_markers is present using str.contains with regex OR operator (|)
        if self.ignore_markers:
            pattern = '|'.join(self.ignore_markers)
            exp_df = exp_df[~exp_df[self.jv_metadata_marker_col_name].str.contains(pattern, case=True, na=False)]
      
        # if DY2012 cruise, replace strings DY20 with DY2012
        exp_df[self.jv_run_sample_name_column] = exp_df[self.jv_run_sample_name_column].apply(self.str_replace_dy2012_cruise)

        
        # Change positive control sample names
        exp_df[self.jv_run_sample_name_column] = exp_df.apply(
            lambda row: self.transform_pos_samp_name_in_metadata(metadata_row=row),
            axis=1
        )

        # Remove rows with EMPTY as sample name
        exp_df = exp_df[exp_df[self.jv_run_sample_name_column] != 'EMPTY']

        return exp_df
    
    def _create_positive_samp_dict(self):
        # This will create a dictionary of all the positive samples and their associated samples (to be used for the rel_cont_id in the SampleMetadata)
        # eg. {E54.SKQ21: [run1.POSITIVE.IOT, run1.POSITIVE.18S4]}

        # Get dictionary mapping each sample to the markers it belongs to
        sample_to_markers = self.jv_run_metadata_df.groupby(self.jv_run_sample_name_column)[self.jv_metadata_marker_col_name].apply(list).to_dict()

       # Get dictionary mapping each marker to its positive samples
        marker_to_positives = self.jv_run_metadata_df[self.jv_run_metadata_df[self.jv_run_sample_name_column].str.contains('POSITIVE')].groupby(self.jv_metadata_marker_col_name)[self.jv_run_sample_name_column].apply(list).to_dict()

        result = {}
        for sample, markers in sample_to_markers.items():
            # Collect all positive sample from all the markers this sample belongs to
            positives = []
            for marker in markers:
                if marker in marker_to_positives:
                    positives.extend(marker_to_positives[marker])

            # Rmove duplicates
            result[sample] = list(dict.fromkeys(positives))

        return result

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
            pattern = re.compile(f"^{re.escape(sample_name)}_R[{file_num}].+")
        else:
            pattern = re.compile(f"^{re.escape('POSITIVE')}_R[{file_num}].+")

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
                    "checksum": md5_checksum,
                    "filepath": filepath
                }

        else:
            print(f"Warning: No matching files found for sample {sample_name} in marker {marker}")

    def _create_marker_sample_raw_data_file_dicts(self):
        # Finds all matching data files by marker for each sample returns two nested dict one for forward raw data files, and one for reverse raw data files
        # {marker1: {sample1: {filename: file1.gz, checksum: 56577, filepath: eff/dsdf.fastq.gz}}

        # group run metadata by marker
        grouped = self.jv_run_metadata_df.groupby(self.jv_metadata_marker_col_name)

        for marker, group in grouped:
            print(marker)
           # Get the raw data folder name by shorthand marker - see dict in lists.py. If 
            shorthand_marker = marker_to_shorthand_mapping.get(marker)
            marker_raw_data_dir = self.jv_raw_data_path.get(shorthand_marker)
            print(marker_raw_data_dir)
            if os.path.exists(marker_raw_data_dir):
                print(f"raw data for {shorthand_marker} in folder: {marker_raw_data_dir}")
            else:
                raise ValueError(f"{marker_raw_data_dir} does not exist!")
            
            # Get all filenames in the direcotry
            all_files = os.listdir(marker_raw_data_dir)

            # For each sample in this marker group
            for sample_name in group[self.jv_run_sample_name_column].unique():
                
                self._outline_raw_data_dict(sample_name=sample_name, file_num=1, all_files=all_files, marker=marker, marker_dir=marker_raw_data_dir)
                self._outline_raw_data_dict(sample_name=sample_name, file_num=2, all_files=all_files, marker=marker ,marker_dir=marker_raw_data_dir)

    def convert_assay_to_standard(self, marker: str) -> str:
        # matches the marker to the corresponding assay and returns standardized assay name
        
        assay = marker_to_assay_mapping.get(marker, None)

        if assay == None:
            raise NoAcceptableAssayMatch(f'The marker/assay {marker} does not match any of the {[v for v in marker_to_assay_mapping.values()]}. Try changing cutoff or normalizing.')
        else:
            return assay

    def jv_create_lib_id(self, metadata_row: pd.Series) -> str:
        # create lib_id by concatenating sample name, index, marker and run name together

        index_name = metadata_row[self.jv_index_name_col]
        sample_name = metadata_row[self.jv_run_sample_name_column]
        shorthand_marker = marker_to_shorthand_mapping.get(metadata_row[self.jv_metadata_marker_col_name])

        lib_id = sample_name + "_" + index_name + "_" + shorthand_marker + "_" + self.run_name

        return lib_id

    def get_raw_file_names(self, metadata_row: pd.Series, raw_file_dict: dict) -> tuple:
        # Gets the name of the raw file based on sample name and marker
     
        marker_name = metadata_row[self.jv_metadata_marker_col_name]
        sample_name = metadata_row[self.jv_run_sample_name_column]

        # Get approrpiate nested marker dict and corresponding nested sample list with dictionary of files and checksums
        filename = raw_file_dict.get(marker_name).get(sample_name).get('filename')

        return filename
    
    def get_cheksums(self, metadata_row: pd.Series, raw_file_dict: dict) -> tuple:
        # Gets the checksum of the raw file based on sample name and marker
     
        marker_name = metadata_row[self.jv_metadata_marker_col_name]
        sample_name = metadata_row[self.jv_run_sample_name_column]

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
            # For regular samples
            else:
                return sample_name.replace('MP_', '').replace('_', '.').replace('.12S', '-12S') # replace .12S to -12S for SKQ23-12S samples
        else:
            sample_name = sample_name.replace('MP_', '')
            return f'{self.run_name}.{marker}.{sample_name}' 
    
    def _create_count_asv_dict(self, revamp_blast: bool = True) -> dict:
        # TODO: add functionality of revamp_bast is not True?
        # Create  a nested dictionary of marker, sample names and counts
        # {marker: {E56. : {output_read_count: 5, output_otu_num: 8}}}
        # if revamp_blast is True will run _count_otu_tax_assigned_using_REVAMP_blast_based_tax to add the otu_num_tax_assigned

        for marker, asv_tsv_file in self.asv_counts_tsvs_for_run.items():
            asv_tsv_file = os.path.abspath(asv_tsv_file)

            if not os.path.exists(asv_tsv_file):
                raise ValueError(f"File not found: {asv_tsv_file}")

            # Initialize marker in the nested dictionary
            self.asv_data_dict[marker] = {}

            # Read the TSV file with pandas
            asv_df = pd.read_csv(asv_tsv_file, sep='\t')

            # set the asv column as the index
            asv_df = asv_df.set_index('x')
            
            # calculate sum for each filtered column and the otu num
            for sample_name in asv_df.columns:
                
                # sum the column values for output read count
                output_read_count = asv_df[sample_name].sum()

                # get the output_otu_num which is the total number asvs for each sample (non zero count number)
                non_zero_output_asv_num = (asv_df[sample_name] > 0).sum()

                # update sample names to match rest of data
                updated_sample_name = self._clean_asv_samp_names(sample_name=sample_name, marker=marker)
                
                # store output_read_count in the nested dictionary
                self.asv_data_dict[marker][updated_sample_name] = {'output_read_count': output_read_count.item(), 
                                                                   'output_otu_num': non_zero_output_asv_num.item()}

        # Since getting the otu_num_tax_assigned depends on which version of the taxonomy we are using for the submission
        # if revamp_blast == True then use the ASVs_counts_mergedOnTaxonomy.tsv file to get this info.
        if revamp_blast == True:
            self._count_otu_tax_assigned_using_REVAMP_blast_based_tax()

    def _count_otu_tax_assigned_using_REVAMP_blast_based_tax(self):
        # Calculates the otu_num_tax_assined from the ASVs_counts_mergedOnTaxonomy.tsv file
        
         for marker, asv_tax_file in self.otu_num_tax_assigned_files_for_run.items():

            # Read the TSV file with pandas
            asv_tax_df = pd.read_csv(asv_tax_file, sep='\t')

            # set the asv column as the index
            asv_tax_df = asv_tax_df.set_index('x')

            for sample_name in asv_tax_df.columns:

                # get the output_otu_num which is the total number asvs for each sample (non zero count number)
                non_zero_otu_num_tax_assinged = (asv_tax_df[sample_name] > 0).sum()

                # update sample names to match rest of data
                updated_sample_name = self._clean_asv_samp_names(sample_name=sample_name, marker=marker)

                self.asv_data_dict[marker][updated_sample_name]["otu_num_tax_assigned"] = non_zero_otu_num_tax_assinged.item()

    def process_paired_end_fastq_files(self, metadata_row: pd.Series) -> int:
        # Process R1 FASTQ file using exact file path

        # Get the filepath to the 
        r1_file_path = self.raw_filename_dict.get(metadata_row[self.jv_metadata_marker_col_name]).get(metadata_row[self.jv_run_sample_name_column]).get('filepath')

        input_read_count = self._count_fastq_files_bioawk(r1_file_path)

        return input_read_count
    
    def process_asv_counts(self, metadata_row: pd.Series, faire_col) -> str or int:
        # Get the count (either output_read_count, output_otu_num, or otu_num_tax_assigned) from the asv_data_dict based on sample name and marker

        # normalize marker to shorthand
        marker = marker_to_shorthand_mapping.get(metadata_row[self.jv_metadata_marker_col_name])
        sample_name = metadata_row[self.jv_run_sample_name_column]

        # Get the count
        try:
            count = self.asv_data_dict.get(marker).get(sample_name).get(faire_col)
        except Exception as e:
            print(f"No count data for {sample_name} with {marker} {e}")
            count = "missing: not collected"

        return count

       

        

    
        

        

        