from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_line_id_from_standardized_station(mapper: FaireSampleMetadataMapper):
    """
    Rule for calculating getting the line_id from the station
    Expects metadata_col to be 'station; (the standardized station column).
    """
    def apply_line_id_from_station(df, faire_col, metadata_col):
            """
            Apply line_id deduction using the mapper's get_line_id method.
            """
            # Apply the calculation to each row
            return df[metadata_col].apply(mapper.get_line_id)
    return (
            TransformationBuilder('line_id_from_station')
            .when(lambda f, m, mt: (
                f == 'line_id' and
                mt == 'related'
            ))
            .apply(
                apply_line_id_from_station,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )