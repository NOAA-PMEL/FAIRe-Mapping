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
        .update_source(True)
        .for_mapping_type('related')
        .build()
    )

def get_minimum_depth_from_max_minus_1m(mapper: FaireSampleMetadataMapper):
    """
    Rule for calculating the minimumDepthInMeters from the maximumDepthInMeters
    by subtracting 1 from the maximumDepthinMeters Expectes metadata_col to be the
    maximumDepthInMeters col name. If relying on a calculationg for maximumDepthInMeters
    then use the faire_col maximumDepthInMeters for metadata_col in mapping file.
    """
    def apply_mindepth_from_maxdepth_calculation(df, faire_col, metadata_col):
            """
            Apply depth calculation using the mapper's convert_min_depth_from_minus_one_meter method.
            """
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.convert_min_depth_from_minus_one_meter(
                    metadata_row=row,
                    max_depth_col_name=metadata_col
                ),
                axis=1
            )
    return (
            TransformationBuilder('mindepth_from_maxdepth_minus_1m')
            .when(lambda f, m, mt: (
                f == 'minimumDepthInMeters' and
                mt == 'related'
            ))
            .apply(
                apply_mindepth_from_maxdepth_calculation,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )

