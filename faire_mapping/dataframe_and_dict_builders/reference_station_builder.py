import pandas as pd
from faire_mapping.dataframe_and_dict_builders.base_df_builder import BaseDfBuilder

class ReferenceStationBuilder(BaseDfBuilder):
    """
    Uses the references station google sheet to create a bunch of reference dictionaries to check station and lat/lon coordinates.
    """

    STATION_NAME_COL_NAME = "station_name"
    LAT_COL_NAME = "LatitudeDecimalDegree"
    LON_COL_NAME = "LongitudeDemicalDegree"
    LAT_HEM_COL_NAME = "LatitudeHem"
    LON_HEM_COL_NAME = "LongitudeHem"
    OME_STATION_COL_NAME = "ome_station" # Basically synonyms for the reference stations (may once to change this column name to be "synonyms" instead of ome specific)
    LINE_ID_COL_NAME = "line_id"

    def __init__(self, header: int = 0, google_sheet_id: str = None, json_creds_path: str = None, sheet_name='Sheet1'):
        
        super().__init__(header=header, sep=',', csv_path=None, google_sheet_id=google_sheet_id, json_creds_path=json_creds_path, sheet_name=sheet_name)

        self.station_lat_lon_ref_dict = self.create_station_lat_lon_ref_dict(station_ref_df=self.df) # e.g. {'BF2': {'lat': '71.75076', 'lon': -154.4567}, 'DBO1.1': {'lat': '62.01', 'lon': -175.06}}
        self.station_standardized_name_dict = self.create_standardized_station_name_ref_dict(station_ref_df=self.df) # e.g {standard_station_name: [non_standard_station_name]}
        self.station_line_dict = self.create_station_line_id_ref_dict(station_ref_df=self.df) # e.g. {standard_station_name: line_id_name}

    def create_station_lat_lon_ref_dict(self, station_ref_df: pd.DataFrame) -> dict:
        station_lat_lon_ref_dict = {}
        for _, row in station_ref_df.iterrows():
            station_name = row[self.LAT_COL_NAME]
            lat = row[self.LAT_COL_NAME]
            lon = row[self.LON_COL_NAME]
            lat_hem = row[self.LAT_HEM_COL_NAME]
            lon_hem = row[self.LON_HEM_COL_NAME]

            # Add direction sign to lat/lon
            if 'S' == lat_hem:
                lat = float(-abs(float(lat)))
            if 'W' == lon_hem:
                lon = float(-abs(float(lon)))
            
            station_lat_lon_ref_dict[station_name] = {
                'lat': lat,
                'lon': lon
            }

        return station_lat_lon_ref_dict
    
    def create_standardized_station_name_ref_dict(self, station_ref_df: pd.DataFrame) -> dict:
        station_standardized_name_dict = {}
        for _, row in station_ref_df.iterrows():
            standardized_name = row[self.STATION_NAME_COL_NAME]
            non_standardized_names = row[self.OME_STATION_COL_NAME].split(' | ')

            station_standardized_name_dict[standardized_name] = non_standardized_names

        return station_standardized_name_dict
    
    def create_station_line_id_ref_dict(self, station_ref_df: pd.DataFrame) -> dict:
        # create reference dict where station name is key and line id is value
        station_line_dict = dict(zip(station_ref_df[self.STATION_NAME_COL_NAME], station_ref_df[self.LINE_ID_COL_NAME]))
        return station_line_dict