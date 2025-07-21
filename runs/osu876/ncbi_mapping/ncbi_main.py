import pandas as pd
import sys
sys.path.append("../../..")
from utils.project_mapper import ProjectMapper
from utils.ncbi_mapper import NCBIMapper

def main() -> None:

    # Create final FAIRe experimentRunMetadata and final FAIRe sampleMetadata
    project_creator = ProjectMapper(config_yaml='ncbi_config.yaml')
    sample_metadata_df, experiment_run_metadata_df = project_creator.process_sample_run_data()

    # saves faire sample metadata and experimentRunMetadata as csvs for viewing purposes (uncomment if needed)
    # project_creator.save_final_df_as_csv(final_df=sample_metadata_df, sheet_name='sampleMetadata', header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/runs/run4/ncbi_mapping/sampleMetadata.csv')
    # project_creator.save_final_df_as_csv(final_df=experiment_run_metadata_df, sheet_name='experimentRunMetadata', header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/runs/run4/ncbi_mapping/experimentRunMetadata.csv')

    library_prep_dict = {'owner': 'marinednadude',
                         'repo': 'OSU-Library-Preparation-Sequencing',
                         'file_path': 'OSU-Library-Preparation-Sequencing-BeBOP.md'}
    
    ncbi_mapper = NCBIMapper(final_faire_sample_metadata_df=sample_metadata_df, 
                             final_faire_experiment_run_metadata_df=experiment_run_metadata_df,
                             ncbi_sample_excel_save_path='/home/poseidon/zalmanek/FAIRe-Mapping/runs/osu876/ncbi_mapping/ncbi_submission_data/osu876_MIMARKS.survey.water.6.0.xlsx',
                             ncbi_sra_excel_save_path = '/home/poseidon/zalmanek/FAIRe-Mapping/runs/osu876/ncbi_mapping/ncbi_submission_data/osu876_SRA_metadata.xlsx',
                             library_prep_dict=library_prep_dict)
    ncbi_mapper.create_ncbi_submission()

if __name__ == "__main__":
    main()