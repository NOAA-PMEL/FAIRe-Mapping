import sys
sys.path.append("../..")

from utils.project_mapper import ProjectMapper
from utils.analysis_metadata_mapper import AnalysisMetadataMapper
import pandas as pd

def main() -> None:

    project_creator = ProjectMapper(config_yaml='faire_project_config.yaml')
    project_creator.process_whole_project_and_save_to_excel()

if __name__ == "__main__":
    main()
