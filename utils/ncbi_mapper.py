import pandas as pd
from .lists import faire_to_ncbi_units, ncbi_faire_to_ncbi_column_mappings_exact

# TODO: check on pos_cont_type and neg_cont_type, and sample_Title in ncbi sample mappings stuff?

class NCBIMapper:

    ncbi_organism = "marine metagenome"

    def __init__(self, final_faire_sample_metadata_df: pd.DataFrame, 
                 final_faire_experiment_run_metadata_df: pd.DataFrame,
                 ncbi_sample_excel_template_path: str):
        # Initialize with the final faire_sample_metadata_df and final_faire_experiment_run_metadata_df
        # Can get these by initializing ProjectMapper and running process_sample_run_data method. Be sure to include config file
        # see run4/ncbi_mapping/ncbi_main.py for example

        self.faire_sample_df = final_faire_sample_metadata_df
        self.faire_experiment_run_df = final_faire_experiment_run_metadata_df
        self.ncbi_sample_template_df = self.load_ncbi_template_as_df(file_path=ncbi_sample_excel_template_path, sheet_name='MIMARKS.survey.water.6.0', header=11)

    def load_ncbi_template_as_df(self, file_path: str, sheet_name: str, header: int) -> pd.DataFrame:
        # Load FAIRe excel template as a data frame based on the specified template sheet name
        
        return pd.read_excel(file_path, sheet_name=sheet_name, header=header)
    
 

