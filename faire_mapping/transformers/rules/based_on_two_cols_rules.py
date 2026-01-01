from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging

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