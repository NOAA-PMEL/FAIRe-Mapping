import argparse
import pandas as pd
import sys
sys.path.append("..")
from utils.project_mapper import ProjectMapper

def main() -> None:

    parser = argparse.ArgumentParser(description='Process project configuration path for FAIRe mapping of projects for GBIF/OBIS')
    parser.add_argument('project_config_path', type=str, help='Path to the project configuration file.') 

    args = parser.parse_args()

    project_creator = ProjectMapper(config_yaml=args.project_config_path)
    project_creator.process_whole_project_and_save_to_excel()

if __name__ == "__main__":
    main()
