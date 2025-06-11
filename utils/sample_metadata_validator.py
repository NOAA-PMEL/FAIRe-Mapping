import pandas as pd
import geopandas as gpd
import argparse
import sys
from shapely.geometry import Point
from typing import Dict, Any


class SampleMetadataValidator:
    faire_latitude = "decimalLatitude"
    faire_longitude = "decimalLongitude"
    faire_geo_loc_name = "geo_loc_name"
    faire_samp_name = "samp_name"
    faire_samp_category = "samp_category"

    def __init__(self, csv_file_path: str):

        self.file = csv_file_path
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
        self.sample_metadata_df = self.load_csv_as_df(file_path=self.file)

    def load_csv_as_df(self, file_path: str, header=0) -> pd. DataFrame:
         # Load csv files as a data frame
        df = pd.read_csv(file_path, header=header)
        # only validate sample rows (remove NC rows) and remove empty rows
        df = df[df[self.faire_samp_category] == 'sample']
        return df.dropna(how='all')

    def load_iho_dataset(self):
        return gpd.read_file("/home/poseidon/zalmanek/FAIRe-Mapping/utils/World_Seas_IHO_v3/World_Seas_IHO_v3.shp")

    def run(self) -> None:

        self.validate_coordinates()
        self.validate_arctic_regions_and_geo_loc_name()
    
    def validate_coordinates(self) -> None:
        # Validate latitude and longitude values in general
        
        # Force latitude and longitude to be numeric
        self.sample_metadata_df[self.faire_latitude] = pd.to_numeric(
            self.sample_metadata_df[self.faire_latitude], errors='coerce'
        )
        self.sample_metadata_df[self.faire_longitude] = pd.to_numeric(
            self.sample_metadata_df[self.faire_longitude], errors='coerce'
        )

        # check for missing coordinates
        missing_lat = self.sample_metadata_df[self.faire_latitude].isna().sum()
        missing_lon = self.sample_metadata_df[self.faire_longitude].isna().sum()

        if missing_lat > 0:
            self.errors.append(f"{self.file}: {missing_lat} rows missing latitude values")
        if missing_lon > 0:
            self.errors.append(f"{self.file}: {missing_lon} rows missing longitude values")

        # Check coordinate ranges in general
        invalid_lat = self.sample_metadata_df[
            (self.sample_metadata_df[self.faire_latitude] < self.min_lat) | 
            (self.sample_metadata_df[self.faire_latitude] > self.max_lat)
            ]
        invalid_lon = self.sample_metadata_df[
            (self.sample_metadata_df[self.faire_longitude] < self.min_lon) | 
            (self.sample_metadata_df[self.faire_longitude] > self.max_lon)
            ]

        if len(invalid_lat) > 0:
            self.errors.append(f"{self.file}: {len(invalid_lat)} rows with invalid latitude (outside -90 to 90)")
        if len(invalid_lon) > 0:
            self.errors.append(f"{self.file}: {len(invalid_lon)} rows with invalid longitude (outside -180 to 180)")

    def validate_arctic_regions_and_geo_loc_name(self) -> None:
        # Check if coordinates fall within relevant Arctic/Northern regions and double check geo_loc_name is making sense
        for idx, row in self.sample_metadata_df.iterrows():
            point = Point(row[self.faire_longitude], row[self.faire_latitude])
            # Get the sea area and the country for geo_loc_name
            geo_loc_sea_area = row[self.faire_geo_loc_name].split(':')[1]
            geo_loc_country = row[self.faire_geo_loc_name].split(':')[0]
            samp_name = row[self.faire_samp_name]

            # Check country is USA
            if geo_loc_country not in self.arctic_countries:
                self.errors.append(f"{self.file} with sample {samp_name} has country {geo_loc_country}, which is not acceptable.")
            
            # check sea area has key words
            for _, region in self.iho_dataset.iterrows():
                if region.geometry.contains(point):
                    supposed_sea = region.get('NAME')
                    # Check if any of the arctic keywords exist in the supposed sea to make sure the lat/lon coords are indeed in the Arctic
                    if not any(keyword in supposed_sea.lower() for keyword in self.artic_region_keywords):
                        self.errors.append(f"{self.file} with sample {samp_name} has a lat/lon with found in the area of {supposed_sea}, which does not have keywords {self.artic_region_keywords}")
                    
                    # Check geo_loc kind of matches
                    # TODO: double check this logic makes sense. Maybe data list geo_loc as Bering but looking up lat/lon is coming up as Arctic Ocean. Not sure if this should be a warning or an error.
                    if supposed_sea != geo_loc_sea_area:
                        self.warnings.append(f"{self.file} with sample {samp_name} has have lat lon coordinates that point to {supposed_sea}, but geo_loc is listed as {geo_loc_sea_area}, double check this!")
                    break
    def get_validation_summary(self) -> Dict[str, Any]:
        # Get summary of validation results
        return {'errors': self.errors,
                'warnings': self.warnings,
                'has_errors': len(self.errors) > 0, 
                'has_warnings': len(self.warnings) > 0
                }

def main() -> None:

    parser = argparse.ArgumentParser(description='Validate sample metadata')
    parser.add_argument('--file', required=True, help='Path to FAIRe sample metadata csv.')
    parser.add_argument('--strict', action='store_true', help="Treat warnings as errors") 

    args = parser.parse_args()

    # Create validator
    validator = SampleMetadataValidator(csv_file_path=args.file)
    validator.run()
    summary = validator.get_validation_summary()
    if summary['errors']: 
        print("\u274C Validation Errors:")
        for error in summary['errors']:
            print(f"    - {error}")
    if summary['warnings']:
        print("\u26A0 Warnings:")
        for warning in summary['warnings']:
            print(f"    - {warning}")
    else:
        print(f"\u2705 Sucess! Validation passed for {args.file}")

    # Exit with appropriate code
    if summary['has_errors'] or (args.strict and summary['has_warnings']):
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()