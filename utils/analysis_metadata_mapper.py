import pandas as pd
from .faire_mapper import OmeFaireMapper

class AnalysisMetadataMapper(OmeFaireMapper):

    faire_template_analysis_sheet_name = 'analysisMetadata'

    def __init__(self, config_yaml, bioinformatics_bebop_path: str, bioinformatics_config_google_sheet_id: str, experiment_run_metadata_df: pd.DataFrame,
                 project_id: str, bioinformatics_software_name: str):
        
        super().__init__(config_yaml)

        self.bioiformatics_bebop = self.load_beBop_yaml_terms(path_to_bebop=bioinformatics_bebop_path)
        self.bioinformatics_config_df = self.load_google_sheet_as_df(google_sheet_id=bioinformatics_config_google_sheet_id, sheet_name='Sheet1', header=0)
        self.analysis_metadata_df = self.load_analyis_metadata_df()
        self.experiment_run_df = experiment_run_metadata_df
        self.bioinformatics_software_name = bioinformatics_software_name
        self.analysis_run_dict = self.create_analysis_run_dict()

    def load_analyis_metadata_df(self):
        df = self.load_faire_template_as_df(file_path=self.faire_template_file,sheet_name=self.faire_template_analysis_sheet_name, header=0)
        cols = df['term_name'].tolist()
        empty_analysis_metadata_df = pd.DataFrame(columns=cols)
        
        return empty_analysis_metadata_df
    
    def create_analysis_run_dict(self):
        # creates a dictionary of all unique analysis run name (E.g. REVAMP_Parada16S_osu876) as the key and the assay name as the value
        df = self.experiment_run_df.copy()
        df['analysis_run_name'] = self.bioinformatics_software_name + '_' + df['lib_id'].apply(lambda x: '_'.join(x.split('_')[-2:]))
        analysis_run_dict = df.drop_duplicates(['analysis_run_name', 'assay_name']).set_index('analysis_run_name')['assay_name'].to_dict()
        return analysis_run_dict
