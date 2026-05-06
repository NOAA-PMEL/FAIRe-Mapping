from faire_mapping.project_mapper import ProjectMapper
import argparse
import pandas as pd

def main() -> None:

    parser = argparse.ArgumentParser(description='Process project configuration path for FAIRe mapping of projects for GBIF/OBIS')
    parser.add_argument('gh_token', type=str, help='Github personal access token') 


    args = parser.parse_args()

    project_creator = ProjectMapper(config_yaml="/home/poseidon/zalmanek/FAIRe-Mapping/projects/WCOA/wcoa21_net_tow/for_clement/config.yaml", 
                                    gh_token=args.gh_token, 
                                    google_sheet_json_cred='/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json')

    # Calling spearatly because need to fix zenodo links
    sample_df, exp_df = project_creator.process_sample_run_data()

    project_creator.process_whole_project_and_save_to_excel(sample_metadata_df=sample_df, experiment_run_metadata_df=exp_df)

if __name__ == "__main__":
    main()