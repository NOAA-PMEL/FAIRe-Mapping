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

def get_standardized_station_id_from_nonstandardized_station_name(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the standardized station id from nonstandardized station names.
    Not this rule requires that a staton reference file is created and referred to in 
    the config.yaml file. 
    Expects metadata_col to be unstandardized_station_col
    """
    
    def apply_station_id_deduction(df, faire_col, metadata_col):
            """
            Apply the station_id deduction using the mapper's get_station_id_from_unstandardized_station_name method.
            """
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.get_station_id_from_unstandardized_station_name(
                  metadata_row=row,
                  unstandardized_station_name_col=metadata_col
                ),
                axis=1
            )
    return (
            TransformationBuilder('station_id_from_nonstandard_station_name')
            .when(lambda f, m, mt: (
                f == 'station_id' and
                mt == 'related'
            ))
            .apply(
                apply_station_id_deduction,
                mode='direct'
            )
            .for_mapping_type('related')
            .update_source(True)
            .build()
        )

def get_stations_within_5km_of_lat_lon(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the all the standardized stations within 5 km of the lat/lon coordinates.
    Will throw an error if no stations were picked up. Also checks that the standardized station_id
    is in the list of within 5 km, if not will throw an error. Stations need to be standardized first.
    (Use rule get_standardized_station_id_from_nonstandardized_station_name first) 
    Expects metadata_col to be faire_standardized_station_col (station_id) | lat_col | lon_col
    """
    
    def apply_within_5km_station_deduction(df, faire_col, metadata_col):
            """
            Apply the within 5 km stations deduction using the mapper's get_stations_within_5km method.
            """
            metadata_cols = metadata_col.split(' | ')

            if len(metadata_cols) < 3: 
                logger.error(f"Expected at least 3 values related to getting stations within 5 km faire_standardized_station_col | lat_col | lon_col got: {metadata_col}")
                raise ValueError(f"stations_within_5km check requires format 'faire_standardized_station_col (station_id) | lat_col | lon_col'")
                 
            station_col = metadata_cols[0]
            lat_col = metadata_cols[1]
            lon_col = metadata_cols[2] 
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.get_stations_within_5km(
                  metadata_row=row,
                  station_name_col=station_col,
                  lat_col=lat_col,
                  lon_col=lon_col
                ),
                axis=1
            )
    return (
            TransformationBuilder('stations_within_5km')
            .when(lambda f, m, mt: (
                f == 'station_ids_within_5km_of_lat_lon' and
                mt == 'related'
            ))
            .apply(
                apply_within_5km_station_deduction,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )