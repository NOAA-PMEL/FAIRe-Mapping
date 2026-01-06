from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_well_number_from_well_field(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the well number from a well field column that has a value like G1 -> 1
    Expects metadata_col to be 'well_col'
    """
    def apply_well_number_deduction(df, faire_col, metadata_col):
            """
            Apply well number deduction using the mapper's get_well_number_from_well_field method.
            """     
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.get_well_number_from_well_field(
                  metadata_row=row,
                  well_col=metadata_col
                ),
                axis=1
            )
    
    return (
            TransformationBuilder('well_number_from_well_field')
            .when(lambda f, m, mt: (
                f == 'extract_well_number' and
                mt == 'related'
            ))
            .apply(
                apply_well_number_deduction,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )

def get_well_position_from_well_field(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the well position from a well field column that has a value like G1 -> G
    Expects metadata_col to be 'well_col'
    """
    def apply_well_position_deduction(df, faire_col, metadata_col):
            """
            Apply well postion deduction using the mapper's get_well_position_from_well_field method.
            """     
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.get_well_position_from_well_field(
                  metadata_row=row,
                  well_col=metadata_col
                ),
                axis=1
            )
    
    return (
            TransformationBuilder('well_postion_from_well_field')
            .when(lambda f, m, mt: (
                f == 'extract_well_position' and
                mt == 'related'
            ))
            .apply(
                apply_well_position_deduction,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )