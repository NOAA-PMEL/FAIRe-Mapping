import pandas as pd
import openpyxl
import copy
from openpyxl.styles import PatternFill
from faire_mapping.faire_mapper import OmeFaireMapper
from faire_mapping.utils import retrive_github_bebop, load_google_sheet_as_df
from faire_mapping.constants import bioinformatics_bebop, run_mapping


# TODO: will need to update anlayis_run_id to be project_id_assay_name_seq_run_id (but we need to have db in here too). Right now have assay_name_run_db
# TODO: Add trim_param special code
# TODO: Double check with Sean about error_rate_cutoff - did I do it the way he envisioned?

class AnalysisMetadataMapper(OmeFaireMapper):

    REVAMP_CONFIG_ASSAY_COL_NAME = 'assay'
    REVAMP_CONFIG_RUN_COL_NAME = 'Run'
    BEBOP_ASSAY_FIELD_NAME = 'assay_name'
    BEBOP_TAX_METHOD_FIELD_NAME = 'taxonomy_method_list'
    BEBOP_TAX_METHOD_OTHER_NAME = 'taxonomy_method'
    FAIRE_ANALYSIS_RUN_NAME = 'analysis_run_name'
    FAIRE_TRIM_PARAM = 'trim_param'

    BEBOP_SOURCE_FILE = "source_file"
    BEBOP_SOURCE_TERM = "source_term"
    BEBOP_DEFAULT = "default"

    faire_template_analysis_sheet_name = 'analysisMetadata'
    project_id_col = 'project_id'
    assay_name_col = 'assay_name'
    analysis_run_name_col = 'analysis_run_name'

    def __init__(self, config_yaml, project_id: str, experiment_run_metadata_df: pd.DataFrame, tax_method_dict: dict, gh_token: str, google_sheet_json_cred: str):
                #  project_id: str, bioinformatics_software_name: str, bebop_config_run_col_name: str, bebop_config_marker_col_name: str):
        
        super().__init__(config_yaml)

        self.gh_token = gh_token
        self.project_id = project_id
        self.tax_method_dict = tax_method_dict # A dictionary of assays and lists of taxonomy methods to filter the analysis metadadata_df by

        self.bio_bebop = retrive_github_bebop(owner=bioinformatics_bebop.get('bebop').get('owner'),
                                            repo=bioinformatics_bebop.get('bebop').get('repo'),
                                            file_path=bioinformatics_bebop.get('bebop').get('file_path'),
                                            gh_token=self.gh_token,
                                            branch=bioinformatics_bebop.get('bebop').get('branch'))
        self.bioinformatics_config_df = load_google_sheet_as_df(google_sheet_id=bioinformatics_bebop.get('bebop_config_file_google_sheet_id'), sheet_name='Sheet1', header=0, google_sheet_json_cred=google_sheet_json_cred)
        self.experiment_run_df = experiment_run_metadata_df


        # dictionary of assay to list of runs its associated with (e.g. {parada: [run1, run2]})
        self.assay_runs = self.bioinformatics_config_df.groupby(self.REVAMP_CONFIG_ASSAY_COL_NAME)[self.REVAMP_CONFIG_RUN_COL_NAME].apply(list).to_dict()
        # Get dict of assys and dbs {assay: [db1, db2]}
        self.assay_dbs = {
            assay: values[self.BEBOP_TAX_METHOD_FIELD_NAME] 
            for assay, values in self.bio_bebop.get(self.BEBOP_ASSAY_FIELD_NAME, {}).items()
        }

        self.analysis_metadata_df = self.process_analysis_metadata()
        

    def process_analysis_metadata(self):

        # 1. Create all the analysis in a single df based on the BeBOP
        analysis_metadata_df = self.format_analysis_metadata_from_bebop()

        # 2. Filter the df by the analyses that are desired
        final_analysis_df = self.filter_df_to_desired_analyses(analysis_metadata_df=analysis_metadata_df)

        return final_analysis_df

        # self.save_to_excel(final_analysis_metadata_df=analysis_metadata_df, excel_file_to_save_to=self.final_faire_template_path)

    def format_analysis_metadata_from_bebop(self):
        """ Formats into all the different analysis metadata from the single BeBOP """

        analysis_metadata_df = self.load_analyis_metadata_df()

        bebops_untangled = []
        for assay, runs in self.assay_runs.items():
            for run in runs:
                dbs = self.assay_dbs.get(assay)
                std_run = run_mapping.get(run) # standardize run
                for db in dbs:
                    #TODO: Update analysis_run_name to match FAIRe - project_id_assay_name_seq_run_id (but problem is that we need the db in there too)
                    analysis_run_name = f"{assay}_{std_run}_{db}"
                    
                    # Initialize analysis metadata dictionary
                    analysis_metadata = {
                        self.BEBOP_ASSAY_FIELD_NAME: assay,
                        self.FAIRE_ANALYSIS_RUN_NAME: analysis_run_name
                        }

                    # copy non-lists/dicts values over
                    analysis_metadata.update({k: v for k, v in self.bio_bebop.items() if not isinstance(v, (list, dict))})

                    # Tackle default/source_value/source_term
                    analysis_metadata = self.get_source_term_value_from_revamp_config(assay=assay, run=run, db=db, analysis_metadata_dict=analysis_metadata)
              
                    bebops_untangled.append(analysis_metadata)


        analysis_metadata_df = pd.concat([analysis_metadata_df, pd.DataFrame(bebops_untangled)], ignore_index=True)[analysis_metadata_df.columns]
        analysis_metadata_df['project_id'] = self.project_id
        return analysis_metadata_df

    def get_source_term_value_from_revamp_config(self, assay: str, run: str, db: str, analysis_metadata_dict: dict) -> dict:
        """
        Uses the revamp config to get the values for any terms that have source_term source_file listed. trim_param is 
        a special case. Anything with | in the source_term is special, because requerires searching more than one term in 
        the revamp config.
        """
        # Tackle default/source_value/source_term
        for faire_field, faire_value in self.bio_bebop.items():
            if isinstance(faire_value, dict) and self.BEBOP_SOURCE_TERM in faire_value.keys() and self.BEBOP_SOURCE_FILE in faire_value.keys():
                source_term = faire_value.get(self.BEBOP_SOURCE_TERM)
                
                # #TODO: trim param special add code here
                if faire_field == self.FAIRE_TRIM_PARAM:
                    default = faire_value.get(self.BEBOP_DEFAULT)
                    source_terms = source_term.split(' | ')
                    for term in source_terms:
                        actual_faire_value = self.bioinformatics_config_df.loc[(self.bioinformatics_config_df[self.REVAMP_CONFIG_RUN_COL_NAME] == run) & (self.bioinformatics_config_df[self.REVAMP_CONFIG_ASSAY_COL_NAME] == assay), term].values[0]
                        default = default.replace(term, actual_faire_value)
                    default = default.replace('{', '').replace('}', '')
                    analysis_metadata_dict[faire_field] = default

                elif '|' in source_term:
                    source_terms = source_term.split(' | ')
                    actual_faire_values = []
                    for term in source_terms:
                        actual_faire_value = self.bioinformatics_config_df.loc[(self.bioinformatics_config_df[self.REVAMP_CONFIG_RUN_COL_NAME] == run) & (self.bioinformatics_config_df[self.REVAMP_CONFIG_ASSAY_COL_NAME] == assay), term].values[0]
                        actual_faire_values.append(actual_faire_value)
                    final_actual_faire_value = ' | '.join(actual_faire_values)
                    analysis_metadata_dict[faire_field] = final_actual_faire_value
                
                else:
                    actual_faire_value = self.bioinformatics_config_df.loc[(self.bioinformatics_config_df[self.REVAMP_CONFIG_RUN_COL_NAME] == run) & (self.bioinformatics_config_df[self.REVAMP_CONFIG_ASSAY_COL_NAME] == assay), source_term].values[0]
                    analysis_metadata_dict[faire_field] = actual_faire_value

            # Get db specific info.
            elif isinstance(faire_value, dict) and faire_field == self.BEBOP_TAX_METHOD_OTHER_NAME and db in faire_value.keys():
                taxa_info = faire_value.get(db)
                # Update with just regular key/value pairs
                analysis_metadata_dict.update({k: v for k, v in taxa_info.items() if not isinstance(v, (list, dict))})
                for nested_faire_field, nested_faire_value in taxa_info.items():

                    # Update just key/value pairs
                    if not isinstance(nested_faire_value, (list, dict)):
                        analysis_metadata_dict[nested_faire_value] = nested_faire_field

                    elif self.BEBOP_SOURCE_TERM in nested_faire_value.keys() and self.BEBOP_SOURCE_FILE in nested_faire_value.keys():
                        actual_faire_value = self.bioinformatics_config_df.loc[(self.bioinformatics_config_df[self.REVAMP_CONFIG_RUN_COL_NAME] == run) & (self.bioinformatics_config_df[self.REVAMP_CONFIG_ASSAY_COL_NAME] == assay), source_term].values[0]
                        analysis_metadata_dict[nested_faire_field] = actual_faire_value

                    # If just default here / TODO: not sure why default is even needed, can remove and just have the key value pair
                    elif self.BEBOP_DEFAULT in nested_faire_value.keys() and self.BEBOP_SOURCE_TERM not in nested_faire_value.keys() and self.BEBOP_SOURCE_FILE not in nested_faire_value.keys():
                        analysis_metadata_dict[nested_faire_field] = nested_faire_value.get(self.BEBOP_DEFAULT)

        return analysis_metadata_dict
    

    def load_analyis_metadata_df(self):
        df = self.load_faire_template_as_df(file_path=self.faire_template_file,sheet_name=self.faire_template_analysis_sheet_name, header=0)
        cols = df['term_name'].tolist()
        empty_analysis_metadata_df = pd.DataFrame(columns=cols)
        
        return empty_analysis_metadata_df
    

    def filter_df_to_desired_analyses(self, analysis_metadata_df: pd.DataFrame):

        mask = pd.Series(False, index=analysis_metadata_df.index)
    
        for assay, tax_methods in self.tax_method_dict.items():
            for tax_method in tax_methods:
                # 2. Identify rows that match the current pair
                match = (
                    analysis_metadata_df[self.analysis_run_name_col].str.contains(assay, na=False) &
                    analysis_metadata_df[self.analysis_run_name_col].str.contains(tax_method, na=False)
                )
                # 3. Use OR (|=) to add these matches to our mask
                mask |= match

        # 4. Now filtered_df contains ONLY the matches
        filtered_df = analysis_metadata_df[mask]

        return filtered_df
    
        
    def save_to_excel(self, final_analysis_metadata_df: pd.DataFrame, excel_file_to_save_to: str):
        # Save analysisMetadata df rows to their own excel file sheets
        
        # for formatting
        template_wb = openpyxl.load_workbook(self.faire_template_file)
        template_sheet = template_wb[self.faire_template_analysis_sheet_name] 
        
        # Create new workbook
        new_wb = openpyxl.load_workbook(excel_file_to_save_to)
        
        for idx, data_row in final_analysis_metadata_df.iterrows():
            sheet_name = f'analysisMetadata_{data_row[self.analysis_run_name_col]}'

            new_sheet = new_wb.create_sheet(title=sheet_name)

            # Copy all cells and formatting from template
            for row in template_sheet.iter_rows():
                for cell in row:
                    new_cell = new_sheet.cell(row=cell.row, column = cell.column)
                    new_cell.value = cell.value

                    # Copy formatting
                    if cell.fill:
                        new_cell.fill = copy.copy(cell.fill)
                    if cell.font:
                        new_cell.font = copy.copy(cell.font)
                    if cell.alignment:
                        new_cell.alignment = copy.copy(cell.alignment)
                    if cell.border:
                        new_cell.border = copy.copy(cell.border)
                    if cell.number_format:
                        new_cell.number_format = copy.copy(cell.number_format)

            for row in new_sheet.iter_rows(min_row=2):
                #skip header row
                term_name_cell = row[2]
                values_cell = row[3]
                
                if term_name_cell.value and term_name_cell.value in final_analysis_metadata_df.columns:
                    values_cell.value = data_row[term_name_cell.value]

            new_wb.save(excel_file_to_save_to)
            template_wb.close()
