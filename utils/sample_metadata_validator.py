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
    faire_neg_cont_type = "neg_cont_type"
    faire_pos_cont_type = "pos_cont_type"
    faire_event_date = "eventDate"
    faire_env_broad_scale = "env_broad_scale"
    faire_env_local_scale = "env_local_scale"
    faire_env_medium = "env_medium"
    faire_assay_name = "assay_name"

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

        # Mandatory columns
        self.mandatory_cols = [
            self.faire_samp_name,
            self.faire_samp_category,
            self.faire_neg_cont_type,
            self.faire_pos_cont_type,
            self.faire_latitude,
            self.faire_longitude,
            self.faire_geo_loc_name,
            self.faire_event_date,
            self.faire_env_broad_scale,
            self.faire_env_local_scale,
            self.faire_env_medium,
            self.faire_assay_name
        ]

        self.iho_dataset = self.load_iho_dataset()
        self.sample_metadata_df = self.load_csv_as_df(file_path=self.file)

    def load_csv_as_df(self, file_path: str, header=0) -> pd. DataFrame:
         # Load csv files as a data frame
        df = pd.read_csv(file_path, header=header)
        # only validate sample rows (remove NC rows) and remove empty rows
        df = df[df[self.faire_samp_category] == 'sample']
        return df.dropna(how='all')

    def load_iho_dataset(self):
        return gpd.read_file("utils/World_Seas_IHO_v3/World_Seas_IHO_v3.shp")

    def run(self) -> None:

        self.validate_mandatory_columns()
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
            samp_name = row[self.faire_samp_name]
            try:
                geo_loc_sea_area = row[self.faire_geo_loc_name].split(':')[1]
                geo_loc_country = row[self.faire_geo_loc_name].split(':')[0]
            # Sometimes Arctic Ocean won't have USA in front because its an INSDC acceptable region
            except:
                geo_loc_country = None
                geo_loc_sea_area = row[self.faire_geo_loc_name]
            

            # Check country is USA
            if geo_loc_country and geo_loc_country not in self.arctic_countries:
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
                    if supposed_sea.lower().strip() != geo_loc_sea_area.lower().strip():
                        self.warnings.append(f"{self.file} with sample {samp_name} has lat lon coordinates that point to {supposed_sea}, but geo_loc is listed as {geo_loc_sea_area}, double check this!")
                    break
    
    def validate_mandatory_columns(self) -> None:
        # Check to make sure all mandatory columns are present are do there are no Nan, None, or empty values
        missing_columns = [col for col in self.mandatory_cols if col not in self.sample_metadata_df.columns]
        if missing_columns:
            self.errors.append(f"{self.file}: Missing mandatory columsn: {missing_columns}")
        
        for col in self.mandatory_cols:
            if col in self.sample_metadata_df.columns:
                # Count different types of "empty" values
                null_count = self.sample_metadata_df[col].isna().sum()
                empty_string_count = (self.sample_metadata_df[col] == '').sum()
                none_count = (self.sample_metadata_df[col].astype(str) == 'None').sum()

                total_empty = null_count + empty_string_count + none_count

                if total_empty > 0:
                    empty_details = []
                    if null_count > 0:
                        empty_details.append(f"{null_count} NaN/null")
                    if empty_string_count > 0:
                        empty_details.append(f"{empty_string_count} empty strings")
                    if none_count > 0:
                        empty_details.append(f"{none_count} None")

                    detail_str = ",".join(empty_details)
                    self.errors.append(f"{self.file}: Column: {col} has {total_empty} empty values ({detail_str})")

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