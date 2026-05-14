from faire_mapping.analysis_metadata_mapper import AnalysisMetadataMapper
from faire_mapping.utils import load_google_sheet_as_df
import argparse

parser = argparse.ArgumentParser(description='Process project configuration path for FAIRe mapping of projects for GBIF/OBIS')
parser.add_argument('gh_token', type=str, help='Github personal access token') 


args = parser.parse_args()


# Just use master sheet to test
experiment_run_df = load_google_sheet_as_df(google_sheet_id='1askd-wDorl-YVh7jk6vBEVtzGMLKp9oXGC7k-SqSrtc',
                                            sheet_name='experimentRunMetadata',
                                            header=0,
                                            google_sheet_json_cred='/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json')

analysis_mapper = AnalysisMetadataMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/scripts/push_to_google/config.yaml',
                       experiment_run_metadata_df=experiment_run_df,
                       gh_token=args.gh_token,
                       google_sheet_json_cred='/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json',
                       )

analysis_mapper.format_analysis_metadata_from_bebop()

