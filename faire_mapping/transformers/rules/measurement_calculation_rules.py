from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_depth_from_pressure(mapper: FaireSampleMetadataMapper):
   """
   Rule for calculating the maximumDepthInMeters from the pressure
   and latitude. Expects metadata_col to be in the format 'pressure | latitude'
   """
   def apply_depth_from_pressure_calculation(df, faire_col, metadata_col):
        """
        Apply depth calculation using the mapper's get_depth_from_pressure method.
        """
        metadata_cols = [col.strip() for col in metadata_col.split('|')]

        if len(metadata_cols) != 2:
            logger.error(f"Expected 2 date columns separated by '|' for depth calculation with pressure column first, followed by latitude column second, got: {metadata_col}")
            raise ValueError(f"Depth calculation requires format 'pressure_col | latitude_col'")
        
        pressure_col = metadata_cols[0]
        lat_col = metadata_cols[1]

        # Apply the calculation to each row
        return df.apply(
            lambda row: mapper.get_depth_from_pressure(
                metadata_row=row,
                press_col_name=pressure_col,
                lat_col_name=lat_col
            ),
            axis=1
        )
   return (
        TransformationBuilder('depth_from_pressure_and_lat')
        .when(lambda f, m, mt: (
            f == 'maximumDepthInMeters' and
            '|' in m and
            mt == 'related'
        ))
        .apply(
            apply_depth_from_pressure_calculation,
            mode='direct'
        )
        .for_mapping_type('related')
        .build()
    )

