from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from faire_mapping.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_eventDate_iso8601_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for converting eventDate to ISO8601 format.
    Handles various date formats and converts them to standardized ISO8601
    """
    def apply_date_conversion(df, faire_col, metadata_col):
        """
        Apply date conversion using the mapper's convert_date_to_iso8601 method
        """
        return df[metadata_col].apply(mapper.convert_date_to_iso8601)
    
    return (
        TransformationBuilder('eventDate_to_iso8601')
        .when(lambda f, m, mt: 
            f == 'eventDate' and 
            mt == 'related')
        .apply(
            apply_date_conversion,
            mode='direct'
        )
        .for_mapping_type('related')
        .update_source(True)
        .build()
    )

def get_date_ext_iso8601_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for converting eventDate to ISO8601 format.
    Handles various date formats and converts them to standardized ISO8601
    """
    def apply_date_conversion(df, faire_col, metadata_col):
        """
        Apply date conversion using the mapper's convert_date_to_iso8601 method
        """
        return df[metadata_col].apply(mapper.convert_date_to_iso8601)
    
    return (
        TransformationBuilder('eventDate_to_iso8601')
        .when(lambda f, m, mt: 
            f == 'date_ext' and 
            mt == 'related')
        .apply(
            apply_date_conversion,
            mode='direct'
        )
        .for_mapping_type('related')
        .update_source(True)
        .build()
    )

def get_date_duration_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for calculating duration between two dates to get the prepped_samp_store_dur.
    Expects metadata_Col to be in format: 'start_date_col | end_date_col'
    """
    def apply_duration_calculation(df, faire_col, metadata_col):
        """
        Apply duration calculation using the mapper's calculate_date_duration method
        """
        date_cols = [col.strip() for col in metadata_col.split('|')]

        if len(date_cols) != 2:
            logger.error(f"Expected 2 date columns separated by '|' for duration calculation, got: {metadata_col}")
            raise ValueError(f"Duration calculatoin requires format 'start_date_col | end_date_col'")
        
        start_date_col = date_cols[0]
        end_date_col = date_cols[1]

        # Apply the calculation to each row
        return df.apply(
            lambda row: mapper.calculate_date_duration(
                metadata_row=row,
                start_date_col=start_date_col,
                end_date_col=end_date_col
            ),
            axis=1
        )
    
    date_fields = ['prepped_samp_store_dur', 'eventDurationValue']
    return (
        TransformationBuilder('date_duration_calculation')
        .when(lambda f, m, mt: (
            f in date_fields and
            '|' in m and
            mt == 'related'
        ))
        .apply(
            apply_duration_calculation,
            mode='direct'
        )
        .for_mapping_type('related')
        .build()
    )

def get_iso8601_date_from_date_col_and_time_col_separately(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the eventDate in iso8061 by combineing a date col and time col.
    Expects metadata_col to be 'date_col_name | time_col_name'
    """
    def apply_date_col_time_col_iso8061_combo(df, faire_col, metadata_col):
            """
            Apply Date and Time Iso8061 combo using the mapper's convert_date_and_time_cols_separately_to_iso8601.
            """
            metadata_cols = metadata_col.split(' | ')

            if len(metadata_cols) < 2: 
                logger.error(f"Expected 2 values related to creating the eventDate: 'Date Col | Time Col' got: {metadata_col}")
                raise ValueError(f"EventDate requires format 'date col | time_col'")
                 
            date_col = metadata_cols[0].strip()
            time_col = metadata_cols[1].strip()
            
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.convert_date_and_time_cols_separately_to_iso8601(
                  metadata_row=row,
                  date_utc_col=date_col,
                  time_utc_col=time_col
                ),
                axis=1
            )
    
    return (
            TransformationBuilder('EventDate_by_date_col_and_time_col')
            .when(lambda f, m, mt: (
                f in ['eventDate', 'eventDateEnd'] and
                mt == 'related'
            ))
            .apply(
                apply_date_col_time_col_iso8061_combo,
                mode='direct'
            )
            .for_mapping_type('related')
            .update_source(True)
            .build()
        )