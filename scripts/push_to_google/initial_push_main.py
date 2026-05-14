import argparse
from .initial_google_push import GoogleSheetConcatenator
from faire_mapping.project_mapper import ProjectMapper
from projects.EcoFoci.main import fix_nc_dates, fix_zenodo_version
from projects.AK_Carbon.main import fix_nc_dates_carbon, fix_zenodo_version_carbon



def main() -> None:

    parser = argparse.ArgumentParser(description='Process project configuration path for FAIRe mapping of projects for GBIF/OBIS')
    parser.add_argument('gh_token', type=str, help='Github personal access token') 


    args = parser.parse_args()

    project_creator = ProjectMapper(config_yaml="/home/poseidon/zalmanek/FAIRe-Mapping/scripts/push_to_google/config.yaml", 
                                    gh_token=args.gh_token,
                                    google_sheet_json_cred='/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json')

    # Calling spearatly because need to fix zenodo links
    sample_df, exp_df, analysis_df = project_creator.process_sample_run_data()

    # Fixes for EcoFoci (also in .main() for EcoFoci project) and Carbon (in Carbon's .main())
    sample_df_zenodo_fixed = fix_zenodo_version(df=sample_df)
    sample_df_zenodo_fixed = fix_nc_dates_carbon(df=sample_df_zenodo_fixed)
    sample_df_nc_dates_fixed = fix_nc_dates(df=sample_df_zenodo_fixed)
    sample_df_nc_dates_fixed = fix_zenodo_version_carbon(df=sample_df_nc_dates_fixed)
    

    GoogleSheetConcatenator(df=sample_df_nc_dates_fixed, metadata_type="sampleMetadata")
    GoogleSheetConcatenator(df=exp_df, metadata_type='experimentRunMetadata')
    GoogleSheetConcatenator(df=analysis_df, metadata_type='analysisMetadata')

if __name__ == "__main__":
    main()