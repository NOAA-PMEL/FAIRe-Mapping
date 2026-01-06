from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_material_samp_id_by_cruisecode_cast_btlnum(mapper: FaireSampleMetadataMapper):
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

def get_pps_material_samp_id_by_code_prefix_and_cast(mapper: FaireSampleMetadataMapper):
    """
    Rule for creating the PPS materialSampleID by the cruise_code prefex + cast_no.
    E.g. M2-PPS-0423_Port1
    Expects metadata_col to be 'Cast_col. (or event col) | cruise_prefix'
    """
    def apply_pps_materialSampleID_by_cast_and_cruise_prefix(df, faire_col, metadata_col):
            """
            Apply PPS materialSampleID concatenation using the mapper's add_material_samp_id_for_pps_samp method.
            """
            metadata_cols = metadata_col.split(' | ')

            if len(metadata_cols) < 2: 
                logger.error(f"Expected at 2 values related to creating the pps cruise code: 'Cast_col (or event col). | cruise_prefix' got: {metadata_col}")
                raise ValueError(f"MaterialSammpleID requires format 'Cast_col. (or event col) | cruise_prefix'")
                 
            cast_col = metadata_cols[0]
            cruise_prefix = metadata_cols[1]
            
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.add_material_samp_id_for_pps_samp(
                  metadata_row=row,
                  cast_or_event_col=cast_col,
                  prefix=cruise_prefix
                ),
                axis=1
            )
    
    return (
            TransformationBuilder('pps_materialSampleID_by_cast_and_cruise_prefix')
            .when(lambda f, m, mt: (
                f == 'materialSampleID' and
                mt == 'related'
            ))
            .apply(
               apply_pps_materialSampleID_by_cast_and_cruise_prefix,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )

