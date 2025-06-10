import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


class SampleMetadataValidator:
    faire_latitude = "decimalLatitude"
    faire_longitude = "decimalLongitude"
    faire_geo_loc_name = "geo_loc_name"
    faire_samp_name = "samp_name"

    def __init__(self):
        self.errors = []
        self.warnings = []

       # Coordinate boundaries
        self.min_lat = -90.0
        self.max_lat = 90.0
        self.min_lon = -180.0
        self.max_lon = 180.0 

        # expected country prefixes for our data:
        self.arctic_countries = {'USA'}

        # relevant keywords for Arctic regions
        self.artic_region_keywords = ['bering', 'chukchi', 'arctic', 'north pacific', 'alaska', 'british columbia']

        self.iho_dataset = self.load_iho_dataset()

    def load_iho_dataset(self):
        return gpd.read_file("/home/poseidon/zalmanek/FAIRe-Mapping/utils/World_Seas_IHO_v3/World_Seas_IHO_v3.shp")

    def validate_coordinates(self, df: pd.DataFrame, filename: str) -> None:
        # Validate latitude and longitude values in general

        # check for missing coordinates
        missing_lat = df[self.faire_latitude].isna().sum()
        missing_lon = df[self.faire_longitude].isna().sum()

        if missing_lat > 0:
            self.errors.append(f"{filename}: {missing_lat} rows missing latitude values")
        if missing_lon > 0:
            self.errors.append(f"{filename}: {missing_lon} rows missing longitude values")

        # Check coordinate ranges in general
        invalid_lat = df[(df[self.faire_latitude] < self.min_lat | df[self.faire_latitude] > self.max_lat)]
        invalid_lon = df[(df[self.faire_longitude] < self.min_lon | df[self.faire_longitude] > self.max_lon)]

        if len(invalid_lat) > 0:
            self.errors.append(f"{filename}: {len(invalid_lat)} rows with invalid latitude (outside -90 to 90)")
        if len(invalid_lon) > 0:
            self.errors.append(f"{filename}: {len(invalid_lon)} rows with invalid longitude (outside -180 to 180)")

    def validate_iho_regions(self, df: pd.DataFrame, filename: str) -> None:
        # Check if coordinates fall within relevant Arctic/Northern regions and double check geo_loc_name is making sense
        for idx, row in df.iterrows():
            point = Point(row[self.faire_longitude], row[self.faire_latitude])
            # Get the sea area and the country for geo_loc_name
            geo_loc_sea_area = row[self.faire_geo_loc_name].split(':')[1]
            geo_loc_country = row[self.faire_geo_loc_name].split(':')[0]
            samp_name = row[self.faire_samp_name]

            # Check country is USA
            if geo_loc_country not in self.arctic_countries:
                self.errors.append(f"{filename} with sample {samp_name} has country {geo_loc_country}, which is not acceptable.")
            
            # check sea area has key words
            for _, region in self.iho_dataset.iterrows():
                if region.geometry.contains(point):
                    supposed_sea = region.get('NAME')
                    for keyword in self.artic_region_keywords:
                        if keyword not in supposed_sea.lower():
                            self.errors.append(f"{filename} with sample {samp_name} has a lat/lon with found in the area of {supposed_sea}, which does not have keywords {self.artic_region_keywords}")
                        
                        # Check geo_loc kind of matches
                        # TODO: double check this logic makes sense. Maybe data list geo_loc as Bering but looking up lat/lon is coming up as Arctic Ocean. Not sure if this should be a warning or an error.
                        if supposed_sea != geo_loc_sea_area:
                            self.warnings.appned(f"{filename} with sample {samp_name} has have lat lon coordinates that point to {supposed_sea}, but geo_loc is listed as {geo_loc_sea_area}, double check this!")
