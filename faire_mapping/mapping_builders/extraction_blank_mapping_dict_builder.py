import pandas as pd
from faire_mapping.mapping_builders.base_mapping_builder import BaseMappingBuilder
from faire_mapping.utils import load_google_sheet_as_df

class ExtractionBlankMappingDictBuilder(BaseMappingBuilder):
    """
    Creates the mapping dictionary for the extraction blanks. E.g. {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
    """
    FAIRE_SAMP_VOL_WE_DNA_EXT_COL_NAME = "samp_vol_we_dna_ext"
    FAIRE_POOL_NUM_COL_NAME = "pool_dna_num"
    FAIRE_NUCL_ACID_EXT_METHOD_ADDITIONAL_COL_NAME = "nucl_acid_ext_method_additional"

    def __init__(self, google_sheet_mapping_file_id, google_sheet_json_cred):
        super().__init__(google_sheet_mapping_file_id, google_sheet_json_cred)
    
        self.extraction_blanks_mapping_dict = self.create_extraction_blank_mapping_dict()

    def create_extraction_blank_mapping_dict(self) -> dict:
        # Creates a mapping dict for extraction blanks - mapping will be the same for only extractions faire attributes
        extractions_mapping_df = load_google_sheet_as_df(
            google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.EXTRACT_MAPPING_SHEET_NAME, header=1, google_sheet_json_cred=self.google_sheet_json_cred)

        group_by_mapping = extractions_mapping_df.groupby(
            self.MAPPING_FILE_MAPPED_TYPE_COL)

        # Create nested dictionary {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
        mapping_dict = {}
        for mapping_value, group in group_by_mapping:
            column_map_dict = {k: v for k, v in zip(
                group[self.MAPPING_FILE_FAIRE_FIELD_COL], group[self.MAPPING_FILE_METADATA_FIELD_NAME_COL]) if pd.notna(v)}
            mapping_dict[mapping_value] = column_map_dict

        # If there is an exact mapping for samp_vol_we_dna_ext then remove and put constant value base on config file value
        if self.FAIRE_SAMP_VOL_WE_DNA_EXT_COL_NAME in mapping_dict[self.EXACT_MAPPING]:
            mapping_col = mapping_dict[self.EXACT_MAPPING][self.FAIRE_SAMP_VOL_WE_DNA_EXT_COL_NAME]
            del mapping_dict[self.EXACT_MAPPING][self.FAIRE_SAMP_VOL_WE_DNA_EXT_COL_NAME]
            mapping_dict[self.RELATED_MAPPING][self.FAIRE_SAMP_VOL_WE_DNA_EXT_COL_NAME] = mapping_col


        # If pool_dna_num in exact mapping automatically make constant mapping of 1 (may need to change if blanks are ever pooled for some reason)
        if self.FAIRE_POOL_NUM_COL_NAME in mapping_dict[self.EXACT_MAPPING]:
            del mapping_dict[self.EXACT_MAPPING][self.FAIRE_POOL_NUM_COL_NAME]
            del mapping_dict[self.EXACT_MAPPING][self.FAIRE_NUCL_ACID_EXT_METHOD_ADDITIONAL_COL_NAME]
            mapping_dict[self.CONSTANT_MAPPING][self.FAIRE_POOL_NUM_COL_NAME] = 1
            mapping_dict[self.CONSTANT_MAPPING][self.FAIRE_NUCL_ACID_EXT_METHOD_ADDITIONAL_COL_NAME] = "missing: not provided"

        return mapping_dict