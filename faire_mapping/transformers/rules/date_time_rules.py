from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_eventDate_iso8601_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for converting eventData to ISO8601 format.
    Handles various date formats and converts them to standardized ISO8601
    """
    def apply_date_conversion(df, faire_col, metadata_col):
        """
        Apply date conversion using the mapper's convert_date_to_iso8601 method
        """
        return df[metadata_col].apply(mapper.convert_date_to_iso8601)
    
    return (
        TransformationBuilder('eventDate_to_iso8601')
        .when(lambda f, m, mt: f == 'eventDate')
        .apply(
            apply_date_conversion,
            mode='direct'
        )
        .update_source(True)
        .build()
    )