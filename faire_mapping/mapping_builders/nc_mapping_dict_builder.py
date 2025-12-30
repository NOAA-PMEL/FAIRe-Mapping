import pandas as pd
from faire_mapping.mapping_builders.base_mapping_builder import BaseMappingBuilder
from faire_mapping.mapping_builders.sample_extract_mapping_dict_builder import SampleExtractionMappingDictBuilder
from faire_mapping.constants import nc_faire_field_cols

class NcMappingDictBuilder(BaseMappingBuilder):

    SAMP_STORE_DUR_VALUE = "not applicable: control sample"
    HABITAT_ARTIFICIAL_VALUE = "1"
    SAMP_STORE_TEMP_VALUE = "ambient temperature"
    FAIRE_HABITAT_NAT_ART_COL_NAME = "habitat_natural_artificial_0_1"
    FAIRE_SAMP_MAT_PROCESS_COL_NAME = "samp_mat_process"
    FAIRE_PREPPED_SAMP_STORE_DUR_COL_NAME = "prepped_samp_store_dur"
    FAIRE_SAMP_STORE_DUR_COL_NAME = "samp_store_dur"
    FAIRE_SAMP_STORE_TEMP_COL_NAME = "samp_store_temp"
    FAIRE_SAMP_STORE_LOC_COL_NAME = "samp_store_loc"

    def __init__(self, google_sheet_mapping_file_id, google_sheet_json_cred, sample_mapping_builder: SampleExtractionMappingDictBuilder, nc_samp_mat_process: str, nc_prepped_samp_stor_dur, vessel_name: str):
        """
        sample_mapping_builder is the instantiation of the SampleMappingDictBuilder so the mapping dictionary can be accessed to reference
        nc_samp_mat_process is the negative control value for the faire field, nc_samp_mat_process, and is specified in the config.yaml file, because it may be different than the rest of the regular samples.
        nc_prepped_samp_store_dur is  which date columns the NC prepped samp_store_dur map to (will most likely be different than rest of samples because no ctd or bottle dates for NC)
        vessel_name is the vessel name used in the samp_store_loc field - will come from the config.yaml file.
        """
        super().__init__(google_sheet_mapping_file_id, google_sheet_json_cred)

        self.sample_mapping_builder = sample_mapping_builder
        self.nc_samp_mat_process = nc_samp_mat_process
        self.nc_prepped_samp_store_dur = nc_prepped_samp_stor_dur
        self.vessel_name = vessel_name
        self.nc_mapping_dict = self.create_nc_mapping_dict()

    def create_nc_mapping_dict(self) -> dict:

        nc_mapping_dict = {}
        for mapping_type, col_dict in self.sample_mapping_builder.sample_mapping_dict.items():
            if isinstance(col_dict, dict):
                filtered_nested = {
                    k: v for k, v in col_dict.items() if k in nc_faire_field_cols}
                nc_mapping_dict[mapping_type] = filtered_nested

        # change values that will be differenct for NC's
        nc_mapping_dict[self.CONSTANT_MAPPING][self.FAIRE_HABITAT_NAT_ART_COL_NAME] = self.HABITAT_ARTIFICIAL_VALUE
        nc_mapping_dict[self.CONSTANT_MAPPING][self.FAIRE_SAMP_MAT_PROCESS_COL_NAME] = self.nc_samp_mat_process
        nc_mapping_dict[self.CONSTANT_MAPPING][self.FAIRE_PREPPED_SAMP_STORE_DUR_COL_NAME] = self.nc_prepped_samp_store_dur
        nc_mapping_dict[self.CONSTANT_MAPPING][self.FAIRE_SAMP_STORE_DUR_COL_NAME] = self.SAMP_STORE_DUR_VALUE
        nc_mapping_dict[self.CONSTANT_MAPPING][self.FAIRE_SAMP_STORE_TEMP_COL_NAME] = self.SAMP_STORE_TEMP_VALUE
        nc_mapping_dict[self.CONSTANT_MAPPING][self.FAIRE_SAMP_STORE_LOC_COL_NAME] = self.vessel_name

        return nc_mapping_dict