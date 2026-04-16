import argparse
from .initial_google_push import GoogleSheetConcatenator
from faire_mapping.project_mapper import ProjectMapper

# def main() -> None:

#     parser = argparse.ArgumentParser(description='Push faire standardized metadata to google sheet')
#     parser.add_argument('root_dir', type=str, help='The root dir with all the *_faire.csv files to concatenate')
#     parser.add_argument('metadata_type', type=str, help='The metadata type that is being pushed: sampleMetadata or experimentRunMetadata exactly.') 

#     args = parser.parse_args()

#     GoogleSheetConcatenator(root_dir=args.root_dir, metadata_type=args.metadata_type)

# if __name__ == "__main__":
#     main()


def main() -> None:

    parser = argparse.ArgumentParser(description='Process project configuration path for FAIRe mapping of projects for GBIF/OBIS')
    parser.add_argument('gh_token', type=str, help='Github personal access token') 


    args = parser.parse_args()

    project_creator = ProjectMapper(config_yaml="/home/poseidon/zalmanek/FAIRe-Mapping/scripts/push_to_google/config.yaml", 
                                    gh_token=args.gh_token,
                                    google_sheet_json_cred='/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json')

    # Calling spearatly because need to fix zenodo links
    sample_df, exp_df = project_creator.process_sample_run_data()

    GoogleSheetConcatenator(df=sample_df, metadata_type="sampleMetadata")
    GoogleSheetConcatenator(df=exp_df, metadata_type='experimentRunMetadata')

if __name__ == "__main__":
    main()