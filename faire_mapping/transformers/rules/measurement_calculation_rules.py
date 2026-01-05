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
    by subtracting 1 from the maximumDepthinMeters Expects metadata_col to be the
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

def get_altitude_from_maxdepth_and_totdepthcol(mapper: FaireSampleMetadataMapper):
    """
    Rule for calculating the altitude from the maximumDepthInMeters and tot_depth_col
    Expects metadata_col to be 'maximumDepthInMeters | tot_depth_water_col.
    """
    def apply_altitude_calculation_from_maxdepth_and_totdepth(df, faire_col, metadata_col):
            """
            Apply altitude calculation using the mapper's calculate_altitude method.
            """
            metadata_cols = metadata_col.split(' | ')

            if len(metadata_cols) != 2: 
                logger.error(f"Expected 2 altitude related columns separated by '|' for altitude calculation with maximumDepthInMeters column first, followed by tot_dept_water_col column second, got: {metadata_col}")
                raise ValueError(f"Altitude calculation requires format 'maximumDepthInMeters | tot_depth_water_col'")
                 
            max_depth_col = metadata_cols[0]
            tot_depth_col = metadata_cols[1]
            
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.calculate_altitude(
                    metadata_row=row,
                    depth_col=max_depth_col,
                    tot_depth_col=tot_depth_col
                ),
                axis=1
            )
    return (
            TransformationBuilder('altitude_from_depth_and_tot_depth')
            .when(lambda f, m, mt: (
                f == 'altitude' and
                mt == 'related'
            ))
            .apply(
                apply_altitude_calculation_from_maxdepth_and_totdepth,
                mode='direct'
            )
            .for_mapping_type('related')
            .update_source(True)
            .build()
        )

def get_dna_yield_from_conc_and_vol(mapper: FaireSampleMetadataMapper):
    """
    Rule for calculating the dna_yield from the extraction_conc and samp_vol
    Expects metadata_col to be 'extraction_con | samp_vol'.
    Note that whatever col is put for the extraction_conc is not important as the 
    pipeline standardizes the extraction_conc col in the extraciton, builder, but
    this rule still asks that its placed in the metadata_col so it makes sense.
    """
    def apply_dna_yield_calculation(df, faire_col, metadata_col):
            """
            Apply dna_yield calculation using the mapper's calculate_dna_yield method.
            """
            metadata_cols = metadata_col.split(' | ')

            if len(metadata_cols) != 2: 
                logger.error(f"Expected 2 dna_yield related columns separated by '|' for dna_yield calculation with extraction_conc column first, followed by samp_vol column second, got: {metadata_col}")
                raise ValueError(f"dna_yield calculation requires format 'extraction_conc | samp_vol'")
                 
            samp_vol_col = metadata_cols[1]
            
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.calculate_dna_yield(
                    metadata_row=row,
                    sample_vol_metadata_col=samp_vol_col
                ),
                axis=1
            )
    return (
            TransformationBuilder('dna_yield_from_conc_and_vol')
            .when(lambda f, m, mt: (
                f == 'dna_yield' and
                mt == 'related'
            ))
            .apply(
                apply_dna_yield_calculation,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )

def get_tot_depth_water_col_from_lat_lon_or_exact_col(mapper: FaireSampleMetadataMapper):
    """
    Rule for calculating the tot_depth_water_col for the latitude and longitude, can also map 
    to an exact tot_depth_water_col optionally which will be prioritized to calculating it. 
    Expects metadata_col to be in the format 'latitude_col | longitude_col | [exact_col_mapping_name]'
    """
    def apply_tot_depth_water_calculation(df, faire_col, metadata_col):
            """
            Apply tot_depth_water_col calculation using the mapper's get_tot_depth_water_col_from_lat_lon method.
            """
            metadata_cols = metadata_col.split(' | ')

            if len(metadata_cols) < 2 and len(metadata_cols) > 3: 
                logger.error(f"Expected at least 2 tot_depth_water_col related columns separated by '|' for tot_depth_water_col calculation in the format 'lat_col | lon_col | [exact_col]]: got: {metadata_col}")
                raise ValueError(f"tot_depth_water_col calculation requires format latitude_col | longitude_col | [exact_col_mapping_name]'")
                 
            lat_col = metadata_cols[0]
            lon_col = metadata_cols[1]
            exact_col = metadata_cols[2] if len(metadata_cols) == 3 else None
            
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.get_tot_depth_water_col_from_lat_lon(
                    metadata_row=row,
                    lat_col=lat_col,
                    lon_col=lon_col,
                    exact_map_col=exact_col
                ),
                axis=1
            )
    return (
            TransformationBuilder('tot_depth_water_col_from_lat_lon_and_exact')
            .when(lambda f, m, mt: (
                f == 'tot_depth_water_col' and
                mt == 'related'
            ))
            .apply(
                apply_tot_depth_water_calculation,
                mode='direct'
            )
            .for_mapping_type('related')
            .update_source(True)
            .build()
        )

def get_wind_direction_from_wind_degrees(mapper: FaireSampleMetadataMapper):
    """
    Rule for calculating calculating the wind direction from degrees.
    Expects metadata_col to be 'wind_direction_in_degrees'.
    """
    def apply_wind_direction_calculation(df, faire_col, metadata_col):
        """
        Apply wind_direction calculation using the mapper's convert_wind_degrees_to_direction method.
        """
        # Apply the calculation to each row
        return df[metadata_col].apply(mapper.convert_wind_degrees_to_direction)
    return (
            TransformationBuilder('wind_direction_from_degrees')
            .when(lambda f, m, mt: (
                f == 'wind_direction' and
                mt == 'related'
            ))
            .apply(
                apply_wind_direction_calculation,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )
