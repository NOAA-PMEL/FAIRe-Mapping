from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
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