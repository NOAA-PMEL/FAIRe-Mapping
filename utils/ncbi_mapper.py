import pandas as pd
import shutil
import frontmatter
from .lists import faire_to_ncbi_units, ncbi_faire_to_ncbi_column_mappings_exact, ncbi_faire_sra_column_mappings_exact
from openpyxl import load_workbook

# TODO: add title to get_sra_df method once hear back from Sean
# TODO: Figure out what is going on when submitting PCR samples - added a custom column to differentiate them and the submitter isn't recognizing it.
# TODO: check on pos_cont_type and neg_cont_type, and sample_Title in ncbi sample mappings stuff?
# TODO: figure out title for SRA mappings - need to ask Sean and Zack

class NCBIMapper:

    ncbi_sample_excel_template_path = '/home/poseidon/zalmanek/FAIRe-Mapping/MIMARKS.survey.water.6.0.xlsx'
    ncbi_sra_excel_template_path = '/home/poseidon/zalmanek/FAIRe-Mapping/SRA_metadata.xlsx'
    ncbi_organism = "marine metagenome"
    ncbi_sample_sheet_name = "MIMARKS.survey.water.6.0"
    ncbi_sra_sheet_name = 'SRA_data'
    ncbi_sample_header = 11
    ncbi_sra_header = 0
    ncbi_library_strategy = 'AMPLICON'
    ncbi_library_source = 'METAGENOMIC'
    ncbi_library_selection = 'PCR'
    ncbi_library_layout = 'Paired-end'
    ncbi_file_type = 'fastq'
    faire_missing_values = ["not applicable: control sample",
                            "not applicable: sample group",
                            "not applicable",
                            "missing: not collected: synthetic construct",
                            "missing: not collected: lab stock",
                            "missing: not collected: third party data",
                            "missing: not collected",
                            "missing: not provided",
                            "missing: restricted access: endangered species", 
                            "missing: restricted access: human-identifiable", 
                            "missing: restricted access"
                            ]

    def __init__(self, final_faire_sample_metadata_df: pd.DataFrame, 
                 final_faire_experiment_run_metadata_df: pd.DataFrame,
                 ncbi_sample_excel_save_path: str,
                 ncbi_sra_excel_save_path: str, 
                 library_prep_bebop_path: str):
        # Initialize with the final faire_sample_metadata_df and final_faire_experiment_run_metadata_df
        # Can get these by initializing ProjectMapper and running process_sample_run_data method. Be sure to include config file
        # see run4/ncbi_mapping/ncbi_main.py for example

        self.faire_sample_df = self.clean_samp_df(df=final_faire_sample_metadata_df)
        self.faire_experiment_run_df = final_faire_experiment_run_metadata_df
        self.ncbi_sample_template_df = self.load_ncbi_template_as_df(file_path=self.ncbi_sample_excel_template_path, sheet_name=self.ncbi_sample_sheet_name, header=self.ncbi_sample_header)
        self.ncbi_sample_excel_save_path = ncbi_sample_excel_save_path
        self.ncbi_sra_excel_save_path = ncbi_sra_excel_save_path
        self.library_prep_bebop = self.load_beBop_yaml_terms(library_prep_bebop_path)

    def create_ncbi_submission(self):
        # Will output two excel files

        # Create NCBI Sample Template and fill out and save
        final_ncbi_sample_df = self.get_ncbi_sample_df()
        self.save_to_excel_template(template_path=self.ncbi_sample_excel_template_path,
                                                    ncbi_excel_save_path=self.ncbi_sample_excel_save_path, 
                                                    sheet_name=self.ncbi_sample_sheet_name,
                                                    header=self.ncbi_sample_header,
                                                    final_ncbi_df=final_ncbi_sample_df)
        
        # Creat NCBI SRA Template and fill out and save
        final_sra_df = self.get_sra_df()
        self.save_to_excel_template(template_path=self.ncbi_sra_excel_template_path,
                                    ncbi_excel_save_path=self.ncbi_sra_excel_save_path,
                                    sheet_name=self.ncbi_sra_sheet_name,
                                    header=self.ncbi_sra_header,
                                    final_ncbi_df=final_sra_df)
    
    def load_ncbi_template_as_df(self, file_path: str, sheet_name: str, header: int) -> pd.DataFrame:
        # Load FAIRe excel template as a data frame based on the specified template sheet name
        
        return pd.read_excel(file_path, sheet_name=sheet_name, header=header)
    
    def clean_samp_df(self, df:pd.DataFrame):
        # removes all FAIRe missing values and returns empty values
        # And removes all rows that are not 'sample' for samp_category (so removed positive and negative controls)
        
        df_clean = df.replace(self.faire_missing_values, '')
        df_filtered = df_clean[df_clean['samp_category'] == 'sample']
        return df_filtered
    
    def get_ncbi_sample_df(self):
        
        updated_df = pd.DataFrame()

        # First handle unit transormations
        for ncbi_col_name, faire_mapping in faire_to_ncbi_units.items():
            # Get FAIRe column mame
            faire_col = faire_mapping['faire_col']

            if faire_col not in self.faire_sample_df:
                continue
            else:
                # Check if we have a constant unit or a unit column
                if 'constant_unit_val' in faire_mapping:
                    unit_val = faire_mapping['constant_unit_val']
                    updated_df[ncbi_col_name] = (self.faire_sample_df[faire_col].astype(str) + ' ' + unit_val).where(self.faire_sample_df[faire_col].astype(str).str.strip() != '', '')
                elif 'faire_unit_col' in faire_mapping:
                    unit_col = faire_mapping['faire_unit_col']
                    updated_df[ncbi_col_name] = self.faire_sample_df[faire_col].astype(str) + ' ' + self.faire_sample_df[unit_col].astype(str).where(self.faire_sample_df[faire_col].astype(str).str.strip() != '', '')
                else:
                    updated_df[ncbi_col_name] = self.faire_sample_df[faire_col]

        # Second handle direct column mappings
        for old_col_name, new_col_name in ncbi_faire_to_ncbi_column_mappings_exact.items():
            if old_col_name in self.faire_sample_df:
                updated_df[new_col_name] = self.faire_sample_df[old_col_name]

        # Third handle logic cases (depth and lat/lon)
        updated_df['*depth'] = self.faire_sample_df.apply(
                lambda row: self.get_ncbi_depth(metadata_row=row),
                axis=1
            )
        updated_df['*lat_lon'] = self.faire_sample_df.apply(
            lambda row: self.get_ncbi_lat_lon(metadata_row=row),
            axis=1
        )

        # Fourth add organism
        updated_df['*organism'] = self.ncbi_organism

        # Fifth add description for PCR technical replicates and additional column - needed to differentiate their metadata for submission to be accepted
        updated_df['description'] = self.faire_sample_df['samp_name'].apply(self.add_description_for_pcr_reps)
        updated_df['technical_rep_id'] = self.faire_sample_df['samp_name'].apply(self.add_pcr_technical_rep)
            
        return updated_df

    def get_sra_df(self) -> pd.DataFrame:

        updated_df = pd.DataFrame()

        # First handle direct mappings
        for old_col_name, new_col_name in ncbi_faire_sra_column_mappings_exact.items():
            if old_col_name in self.faire_experiment_run_df:
                updated_df[new_col_name] = self.faire_experiment_run_df[old_col_name]

        # Second add values that are the same across all samples
        updated_df['library_strategy'] = self.ncbi_library_strategy
        updated_df['library_source'] = self.ncbi_library_source
        updated_df['library_selection'] = self.ncbi_library_selection
        updated_df['library_layout'] = self.ncbi_library_layout
        updated_df['platform'] = self.library_prep_bebop['platform']
        updated_df['instrument_model'] = self.library_prep_bebop['instrument']
        updated_df['design_description'] = f"Sequencing performed at {self.library_prep_bebop['sequencing_location']}"
        updated_df['filetype'] = self.ncbi_file_type

        return updated_df

    def load_beBop_yaml_terms(self, path_to_bebop: str):
        # read BeBOP yaml terms
        with open(path_to_bebop, 'r', encoding='utf-8') as f:
            post = frontmatter.load(f)
            return post

    def get_ncbi_depth(self, metadata_row: pd.Series) -> str:
        # Get the ncbi formatted value for depth, which is the interval of minimumDepthInMeters - maximumDepthInMeters
        min_depth = metadata_row['minimumDepthInMeters']
        max_depth = metadata_row['maximumDepthInMeters']

        # If min_depth and max_depth are not the same, report as interval
        if min_depth != max_depth and max_depth != '':
            ncbi_depth = min_depth + ' ' + 'm' + ' - ' + max_depth + ' ' + 'm'
        # if min_depth and max_depth are the same, report just one because they will be the same
        elif min_depth == max_depth and max_depth != '':
            ncbi_depth = min_depth + ' ' + 'm'
        else:
            raise ValueError(f"Something wrong with the depth. Min depth is {min_depth} and max_depth is {max_depth}. {metadata_row}")

        return ncbi_depth

    def get_ncbi_lat_lon(self, metadata_row: pd.Series) -> str:
        lat = float(metadata_row['decimalLatitude'])
        lon = float(metadata_row['decimalLongitude'])

        lat_dir = 'N' if lat >= 0 else 'S'
        lon_dir = 'E' if lon >= 0 else 'W'

        # Take absolute values
        lat_abs = abs(lat)
        lon_abs = abs (lon)

        lat_formatted = f"{lat_abs:.4f} {lat_dir}"
        lon_formatted = f"{lon_abs:.4f} {lon_dir}"

        ncbi_lat_lon = f"{lat_formatted} {lon_formatted}"

        return ncbi_lat_lon
    
    def add_description_for_pcr_reps(self, sample_name: str) -> str:
        # Add a description for the PCR technical replicates (they have to be distinguishable to submit)
        if 'PCR' in sample_name:
            samp_parts = sample_name.split('.')
            # PCR number is last character of the last part
            pcr_num = samp_parts[-1][-1]
            original_sample = '.'.join(samp_parts[:-1])
            return f"PCR technical replicate number {pcr_num} of {original_sample}"
        else:
            return ''

    def add_pcr_technical_rep(self, sample_name: str) -> str:
        if 'PCR' in sample_name:
            samp_parts = sample_name.split('.')
            # PCR number is last character of the last part
            pcr_num = samp_parts[-1][-1]
            return pcr_num
        else:
            return ''
    
    def save_to_excel_template(self, template_path: str, ncbi_excel_save_path: str, sheet_name: str, header: int, final_ncbi_df: pd.DataFrame):

        # Copy template file at saved locationsample_ncbi_excel_save_path
        shutil.copy2(template_path, ncbi_excel_save_path)

        # Load the template to get the column structure
        template_df = pd.read_excel(template_path, sheet_name=sheet_name, header=header)
        template_columns = template_df.columns.tolist()
        
        # Load workbook to preserve formatting
        book = load_workbook(ncbi_excel_save_path)
        ws = book[sheet_name]

        # find column in final_ncbi_df that don't exist in template df (e.g. technical replicate)
        missing_cols = [col for col in final_ncbi_df.columns if col not in template_columns]

        # Add missing columns
        if missing_cols:
            next_col_idx = len(template_columns) + 1
            for i, col_name in enumerate(missing_cols):
                # Add header for new column
                ws.cell(row=header+1, column=next_col_idx + i, value=col_name)
                template_columns.append(col_name)

        for col_idx, template_col_name in enumerate(template_columns, start=1):
            if template_col_name in final_ncbi_df.columns:
                #write data for this column
                column_data = final_ncbi_df[template_col_name]
                for row_idx, value in enumerate(column_data, start = header+2):
                    cell = ws.cell(row=row_idx, column=col_idx)
                    cell.value = value
        # save workbook
        book.save(ncbi_excel_save_path)

        # # Save as tsv
        # df = pd.read_excel(sample_ncbi_excel_save_path, sheet_name=sheet_name)
        # df.to_csv(sample_ncbi_excel_save_path.replace('.xlsx', '.tsv'), sep='\t', index=False)

        print(f'NCBI sample saved to {ncbi_excel_save_path}')

