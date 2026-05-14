from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from faire_mapping.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_samp_store_dur_from_samp_name(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the samp_store_dur from the sample name.
    Note: requires the sample storage reference file.
    Expects metadata_col to be 'samp_name_col'.
    """
    def apply_samp_store_dur(df, faire_col, metadata_col):
        """
        Apply the samp_store_dur mapper's get_samp_store_dur method.
        """
        # Apply the calculation to each row
        return df[metadata_col].apply(mapper.get_samp_store_dur)
    return (
            TransformationBuilder('samp_store_dur_from_samp_name')
            .when(lambda f, m, mt: (
                f == 'samp_store_dur' and
                mt == 'related'
            ))
            .apply(
                apply_samp_store_dur,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )     

def get_samp_store_loc_from_samp_name(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the samp_store_loc from the sample name.
    If samp_stor_dur > 1 hr, loc is vessel name fridge else just vessel name.
    Note: requires the sample storage reference file.
    Expects metadata_col to be 'samp_name_col'.
    """
    def apply_samp_store_loc(df, faire_col, metadata_col):
        """
        Apply the samp_store_loc mapper's get_samp_store_loc_by_samp_store_dur method.
        """
        # Apply the calculation to each row
        return df[metadata_col].apply(mapper.get_samp_store_loc_by_samp_store_dur)
    return (
            TransformationBuilder('samp_store_loc_from_samp_name')
            .when(lambda f, m, mt: (
                f == 'samp_store_loc' and
                mt == 'related'
            ))
            .apply(
                apply_samp_store_loc,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )     

def get_samp_store_temp_from_samp_name(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the samp_store_temp from the sample name.
    If samp_stor_dur > 1 hr, temp is 4 fridge else ambient temperature.
    Note: requires the sample storage reference file.
    Expects metadata_col to be 'samp_name_col'.
    """
    def apply_samp_store_temp(df, faire_col, metadata_col):
        """
        Apply the samp_store_temp mapper's get_samp_store_temp_by_samp_store_dur method.
        """
        # Apply the calculation to each row
        return df[metadata_col].apply(mapper.get_samp_store_temp_by_samp_store_dur)
    return (
            TransformationBuilder('samp_store_temp_from_samp_name')
            .when(lambda f, m, mt: (
                f == 'samp_store_temp' and
                mt == 'related'
            ))
            .apply(
                apply_samp_store_temp,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )  

def get_str_in_samp_name_dur_mapping_rule(mapper: FaireSampleMetadataMapper, faire_field_name: str):
     """
     Rule for mapping date values based on the present of a string in a samp_name.
     Expects metadata_col to be in format 'str_in_samp_name_dur: if_val_in_samp_name | start_date_col - end_date_dol (or constant value) | start_date_col - end_date_col (or constant value)'
     Used in WCOA net tow samples where some were extracted with Plankton smoothie and this was denoted by 'PE' in samp_name. Can hardcode date in 
     for start_date_col or end_date_col as well (so can look like '2/26/2025 - eventDate', for example). If hard coding date requires to be slashes /, NOT hyphens in date.
     """
     def apply_date_dur_in_samp_name_mapping_rule(df, faire_col, metadata_col):
        """
        Apply dated dur str in samp_name rule using the mapper's get_date_dur_based_on_str_in_samp_name method.
        """
        # Remove the 'fallback:' prefix
        if not metadata_col.startswith('str_in_samp_name_dur:'):
             return None
        # Parse the metadata_col to extract column names and options
        columns_part = metadata_col.replace('str_in_samp_name_dur:', '').strip()
        parts = [part.strip() for part in columns_part.split('|')]

        if len(parts) < 3:
             logger.error(f"Str in samp name dur mapping rule requires at 3 values separated by '|'")
             raise ValueError(f"Str in samp name mapping requires format str_in_samp_name_dur: if_val_in_samp_name | start_date_col - end_date_dol (or constant value) | start_date_col - end_date_col (or constant value)'")
        
        val_present_in_samp_name = parts[0]
        present_val_dates = parts[1]
        if_not_present_val_dates = parts[2]

        # Apply the fallback logic to each row
        return df.apply(
             lambda row: mapper.get_date_dur_based_on_str_in_samp_name(
                metadata_row=row,
                if_val_in_samp_name=val_present_in_samp_name,
                if_metadata_range_cols_should_be=present_val_dates,
                else_metadata_range_cols_should_be=if_not_present_val_dates
             ),
             axis=1
        )
     
     return (
        TransformationBuilder('str_dur_in_samp_name_mapping')
            .when(lambda f, m, mt: (
                m.startswith('str_in_samp_name_dur:') and
                f == faire_field_name and
                '|' in m and # contains pipe separator indicating str in samp name 
                mt == 'related'
                ))
            .apply(
                apply_date_dur_in_samp_name_mapping_rule,
                mode='direct'
            )
            .for_mapping_type('related')
            .update_source(True)
            .build()
     )



