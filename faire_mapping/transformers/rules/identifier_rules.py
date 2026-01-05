from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_material_samp_id__by_cruisecode_cast_btlnum(mapper: FaireSampleMetadataMapper):
    """
    Rule for creating the materialSampleID by the cruise_code + cast_no + bottle_no
    Requires the metadata_col to be the cruise code. (the cast no and bottle no, will be taken from the config file)
    """
    def apply_materialSampleID_by_cruise_code(df, faire_col, metadata_col):
            """
            Apply materialSampleID concatenation using the mapper's add_material_sample_id method.
            """
            cruise_code = metadata_col
            
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.add_material_sample_id(
                    metadata_row=row,
                    cruise_code=cruise_code
                ),
                axis=1
            )
    
    return (
            TransformationBuilder('materialSampleID_by_cruise_code')
            .when(lambda f, m, mt: (
                f == 'materialSampleID' and
                mt == 'related'
            ))
            .apply(
                apply_materialSampleID_by_cruise_code,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )
