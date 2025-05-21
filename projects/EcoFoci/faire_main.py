import sys
sys.path.append("../..")

from utils.project_mapper import ProjectMapper
import pandas as pd

def main() -> None:

    project_creator = ProjectMapper(config_yaml='faire_project_config.yaml')
    project_creator.process_sample_run_data()

if __name__ == "__main__":
    main()
