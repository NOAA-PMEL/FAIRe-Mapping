import argparse
import pandas as pd
import sys
sys.path.append("..")
from utils.project_mapper import ProjectMapper

def fix_zenodo_version(df: pd.DataFrame) -> pd.DataFrame:
    nucl_acid_ext_and_samp_method_updates = {"https://doi.org/10.5281/zenodo.11398154": "https://doi.org/10.5281/zenodo.17655148",
                             "https://doi.org/10.5281/zenodo.15793435": "https://doi.org/10.5281/zenodo.17655184",
                             "https://doi.org/10.5281/zenodo.16850033": "https://doi.org/10.5281/zenodo.17401131",
                             "https://doi.org/10.5281/zenodo.16740832": "https://doi.org/10.5281/zenodo.17655213",
                             "https://doi.org/10.5281/zenodo.11398178": "https://doi.org/10.5281/zenodo.17655027",
                            "https://doi.org/10.5281/zenodo.16755251": "https://doi.org/10.5281/zenodo.17655056"}
    
    df_replaced = df.replace(nucl_acid_ext_and_samp_method_updates)

    return df_replaced
    



def main() -> None:

    parser = argparse.ArgumentParser(description='Process project configuration path for FAIRe mapping of projects for GBIF/OBIS')
    parser.add_argument('project_config_path', type=str, help='Path to the project configuration file.') 
    parser.add_argument('gh_token', type=str, help='Github personal access token') 


    args = parser.parse_args()

    project_creator = ProjectMapper(config_yaml=args.project_config_path, gh_token=args.gh_token)
    project_creator.process_whole_project_and_save_to_excel()

if __name__ == "__main__":
    main()
