from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def get_geo_loc_name_by_lat_lon_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for finding geographic location based on longitude and latitude. 
    Expects metadata_col to be in format: 'longitude_col | latitude_col'
    """
    def apply_geo_loc(df, faire_col, metadata_col):
        """
        Apply geo_loc transformation to each row. 
        This function captures metadta_col in its closure
        """
        # Create lambda that has access to metadata_col
        return df.apply(
            lambda row: mapper.find_geo_loc_by_lat_lon(
            metadata_row=row,
            metadata_cols=metadata_col
            ),
            axis=1
        )
    
    return(
        TransformationBuilder('geo_loc_by_lat_lon')
        .when(lambda f, m, mt: (
        f == 'geo_loc_name' and
        mt == 'related' and
        '|' in m # Metadata column contains pipe separator
        ))
        .apply(
            apply_geo_loc,
            mode='direct'
        )
        .update_source(True)
        .for_mapping_type('related')
        .build()
    )

def get_env_medium_for_coastal_waters_by_geo_loc_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for setting env_medium based on geo_loc_name.
    Expects metadata_col to be in fromat: 'default_value | 'coastal_value'
    Sets coastal_value if geo_loc_name contains 'Coastal Waters of Southeast Alaska,
    otherwise sets default value. Very specific to something Sean asked for for the RC0083
    cruise data. Wrote the function in here instead of in sample mapper because this is 
    so specific.
    """
    def apply_env_medium(df, faire_col, metadata_col):
        """
        Apply env_medium transformation based on geo_loc_name.
        """
        # split the metadata column to get the two possible values
        env_mediums = metadata_col.split(' | ')

        if len(env_mediums) != 2:
            logger.error(f"Expected 2 values separated by ' | ' for env_medium, got: {metadata_col}")
            raise ValueError(f"env_medium mapping must be in format 'default_value | coastal_value'")
        
        default_value = env_mediums[0]
        coastal_value = env_mediums[1]

        # Check if geo_loc_name column exists in dataframe
        if 'geo_loc_name' not in df.columns:
            logger.warning("geo_loc_name column not found in dataframe. Using default env_medium value.")
            return pd.Series([default_value] * len(df), index=df.index)
        
        # Apply the conditional logic
        return np.where(
            df['geo_loc_name'].str.contains('Coastal Waters of Southeast Alaska', case=False, na=False),
            coastal_value,
            default_value
        )
    
    return (TransformationBuilder('env_medium_for_alaska_coastal_waters_by_geo_loc')
            .when(lambda f, m, mt: (
                f == 'env_medium' and
                mt == 'related' and
                '|' in m
            ))
            .apply(apply_env_medium,
                   mode='direct'
            )
            .for_mapping_type('related')
            .build()
    )