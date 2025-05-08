import argparse
import pandas as pd
import sys
sys.path.append("..")
from utils.experiment_run_metadata_mapper import ExperimentRunMetadataMapper

def create_exp_run_metadata(config_yaml: str) -> pd.DataFrame:

    exp_mapper = ExperimentRunMetadataMapper(config_yaml=config_yaml)
    faire_exp_df = exp_mapper.generate_jv_run_metadata()

    csv_path = exp_mapper.config_file['final_faire_template_path']
    exp_mapper.save_final_df_as_csv(final_df=faire_exp_df, sheet_name = exp_mapper.faire_template_exp_run_sheet_name, header=2, csv_path=csv_path)

    # save to excel
    # exp_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=exp_mapper.faire_template_file,
    #                                        sheet_name=exp_mapper.faire_template_exp_run_sheet_name, 
    #                                        faire_template_df=faire_exp_df)


def main() -> None:

    parser = argparse.ArgumentParser(description='Process experiment run configuration path for FAIRe mapping of sequencing runs')
    parser.add_argument('experiment_run_config_path', type=str, help='Path to the experiment run configuration file.') 

    args = parser.parse_args()

    faire_exp_run_metadata_df = create_exp_run_metadata(config_yaml=args.experiment_run_config_path)
    print(faire_exp_run_metadata_df)

if __name__ == "__main__":
    main()
