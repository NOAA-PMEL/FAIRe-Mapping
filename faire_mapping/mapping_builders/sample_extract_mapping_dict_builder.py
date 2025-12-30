import pandas as pd
from faire_mapping.mapping_builders.base_mapping_builder import BaseMappingBuilder
from faire_mapping.utils import load_google_sheet_as_df

class SampleExtractionMappingDictBuilder(BaseMappingBuilder):
    """
     Creates the mapping dictionary for the sample metadata. E.g. {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
     Note that this combines the sample metadata mapping with the extraction metadata mapping!!
     """

    SAMPLE_MAPPING_SHEET_NAME = "sampleMetadata" # The name of the google sheet for the sample metadata mapping (should always be sampleMetadata)

    def __init__(self, google_sheet_mapping_file_id):
        super().__init__(google_sheet_mapping_file_id)

        self.sample_mapping_dict = self.create_sample_mapping_dict()

    def create_sample_mapping_dict(self) -> dict:
        # creates a mapping dictionary and saves as self.mapping_dict

        # First concat sample_mapping_df with extractions_mapping_df
        sample_mapping_df = load_google_sheet_as_df(
            google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.SAMPLE_MAPPING_SHEET_NAME, header=0)
        extractions_mapping_df = load_google_sheet_as_df(
            google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.EXTRACT_MAPPING_SHEET_NAME, header=1)
 
        mapping_df = pd.concat([sample_mapping_df, extractions_mapping_df])

        # Group by the mapping type
        group_by_mapping = mapping_df.groupby(
            self.MAPPING_FILE_MAPPED_TYPE_COL)

        # Create nested dictionary {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
        mapping_dict = {}
        for mapping_value, group in group_by_mapping:
            column_map_dict = {k: v for k, v in zip(
                group[self.MAPPING_FILE_FAIRE_FIELD_COL], group[self.MAPPING_FILE_METADATA_FIELD_NAME_COL]) if pd.notna(v)}
            mapping_dict[mapping_value] = column_map_dict

        return mapping_dict