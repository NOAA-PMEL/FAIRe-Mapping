from faire_mapping.faire_mapper import OmeFaireMapper
from faire_mapping.mapping_builders.sample_extract_mapping_dict_builder import SampleExtractionMappingDictBuilder
from faire_mapping.dataframe_and_dict_builders.base_df_builder import BaseDfBuilder
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from faire_mapping.custom_exception import NoInsdcGeoLocError
from faire_mapping import extract_insdc_geographic_locations, ExtractionMetadataBuilder
from pathlib import Path

import sys


def extract_replicate_sample_parent(sample_name):
        # Extracts the E number in the sample name
        if pd.notna(sample_name):
            return sample_name.split('.')[0]

def create_biological_replicates_dict(metadata_df: pd.DataFrame) -> dict:
        # Creates a dictionary of the parent E number as a key and the replicate sample names as the values
        # e.g. {'E26': ['E26.2B.DY2012', 'E26.1B.NC.DY2012']}

        # Extract the parent E number and add to column called replicate_parent
        # Uses set() to remove any technical replicates (they will have the same)
        metadata_df["replicate_parent"] = metadata_df["samp_name"].apply(
            extract_replicate_sample_parent)
        # Group by replicate parent
        replicate_dict = metadata_df.groupby("replicate_parent")[
            "samp_name"].apply(set).to_dict()
        # remove any key, value pairs where there aren't replicates and convert back to list
        replicate_dict = {replicate_parent: list(set(
            sample_name)) for replicate_parent, sample_name in replicate_dict.items() if len(sample_name) > 1}


        return replicate_dict

def add_biological_replicates_column(df: pd.DataFrame) -> pd.Series:
        """
        Add biological replicates for all samples
        """

        replicates_dict = create_biological_replicates_dict(metadata_df=df)

        # If replicates_dict is empty, return all as 'not applicable'
        if not replicates_dict:
            return pd.Series(['not applicable'] * len(df), index=df.index)
        
        parent_samples = df["replicate_parent"]

        # Map parent samples to their replicate strings
        replicate_strings = parent_samples.map(
            lambda parent: ' | '.join(replicates_dict[parent])
            if parent and parent in replicates_dict
            else 'not applicable'
        )

        return replicate_strings

def find_geo_loc_by_lat_lon(metadata_row: pd.Series, metadata_cols: str) -> str:
        # World Seas IHO downloaded from: https://marineregions.org/downloads.php
        """
        Updated for new code structure where metadata_cols is a pipe separated string 
        e.g. 'lon | lat', where lon comes first, followed by lat.
        """
        insdc_locations = extract_insdc_geographic_locations()
        
        print(
            f"Getting geo_loc_name for {metadata_row['samp_name']}")
        cols = [col.strip() for col in metadata_cols.split('|')]
        if len(cols) != 2:
            raise ValueError(f"Expected 2 columns separated by '|' with lat followed by lot, got: {metadata_cols}")
        
        lat = metadata_row.get(cols[1])
        lon = metadata_row.get(cols[0])

        # Handle missing values
        try:
            if pd.isna(lat) or pd.isna(lon) or str(lat).strip() == "" or str(lon).strip() == "":
                return "" # Return empty
            
            lat = float(lat)
            lon = float(lon)
        except (ValueError, TypeError):
            # This catches cases where the data might be a string like "Unknown" or " "
            return ""

        marine_regions = gpd.read_file(
            "/home/poseidon/zalmanek/FAIRe-Mapping/faire_mapping/World_Seas_IHO_v3/World_Seas_IHO_v3.shp")
        
        # create a point object
        point = Point(lon, lat)

        # Check for which marin region contains this point
        for idx, region in marine_regions.iterrows():
            if region.geometry.contains(point):
                sea = region.get('NAME')
            
                if sea == 'Arctic Ocean':
                    geo_loc = sea
                elif 'and British Columbia' in sea:
                    geo_loc = f"USA: {sea.replace('and British Columbia', '')}"
                else:
                    geo_loc = f"USA: {sea}"

                try:
                    geo_loc_name = geo_loc.split(':')[0]
                except:
                    geo_loc_name = [geo_loc]
                if geo_loc_name not in insdc_locations:
                    raise NoInsdcGeoLocError(
                        f'There is no geographic location in INSDC that matches {geo_loc_name}, check sea_name and try again')
                else:
                    return geo_loc
        # If no region contains the point return None
        return None

def extract_cast_number(value):
    # Check for your "keep as is" conditions
    # We use .lower() to make it case-insensitive
    val_str = str(value).lower()
    if "not applicable" in val_str or "missing" in val_str:
        return value
    
    # Split by hyphen and take the last element
    try:
        last_part = str(value).split('-')[-1]
        return int(last_part) # This converts '06' to 6
    except (ValueError, IndexError):
        # Fallback if the string format is unexpected
        return value

def figure_out_rel_cont_ids(faire_mapper: OmeFaireMapper):
     
     config = faire_mapper.load_config(config_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/config.yaml')
     extraction_info = config['extractions']

     extraction_mapping_dict_builder = ExtractionMetadataBuilder(extractions_info=extraction_info,
                                                                 unwanted_cruise_code=".Chaba22",
                                                                 desired_cruise_code='.TN409',
                                                                 google_sheet_json_cred=config['json_creds'],
                                                                 update_mapping_dict=False)
     print(extraction_mapping_dict_builder.extraction_df)
     extraction_mapping_dict_builder.extraction_df.to_csv("/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/data/extractions.csv")

def main() -> None:

    # metadata_df_builder = BaseDfBuilder(csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/Orphan_Projects_Metadata.csv')
   
    # # Get mapping dict
    # mapper_dict_builder = SampleExtractionMappingDictBuilder(google_sheet_mapping_file_id='1vfdQRReGUEl88axV-DAz1hGMhGznh17UfrYWyuSs-Mw',
    #                                                         google_sheet_json_cred='/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json')
    
    # print(mapper_dict_builder.sample_mapping_dict)
    
    # # Just going to use FAIRE-Mapper since extraction is included in metadata and will manually do the few related mappings
    # faire_mapper = OmeFaireMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/config.yaml')

    # # remove empty rows
    # metadata_df_builder.df.dropna(how='all', inplace=True)
    
    # sample_faire_metadata_results = {}
    
    # # Exact Mappings
    # for faire_col, metadata_col in mapper_dict_builder.sample_mapping_dict[faire_mapper.exact_mapping].items():
    #    sample_faire_metadata_results[faire_col] = faire_mapper.apply_exact_mappings(df=metadata_df_builder.df, faire_col=faire_col, metadata_col=metadata_col)

    # # --- RELATED MAPPINGS ----
    # for faire_col, metadata_col in mapper_dict_builder.sample_mapping_dict[faire_mapper.related_mapping].items():
        
    #     # samp_category by samp name
    #     if faire_col == 'biological_rep_relation' and metadata_col:
    #         sample_faire_metadata_results[faire_col] = add_biological_replicates_column(df=metadata_df_builder.df)

    #     elif faire_col == "geo_loc_name":
    #         # The axis=1 belongs to .apply(), not the helper function
    #         sample_faire_metadata_results[faire_col] = metadata_df_builder.df.apply(
    #             lambda row: find_geo_loc_by_lat_lon(metadata_row=row, metadata_cols=metadata_col), 
    #             axis=1
    #         )

    #     elif faire_col == "ctd_cast_number":
    #          # Create the new column
    #         sample_faire_metadata_results[faire_col] = metadata_df_builder.df[faire_col].apply(extract_cast_number)
            

    # faire_sample_df = pd.DataFrame(sample_faire_metadata_results)
    # faire_sample_df.to_csv("/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/data/orphan_faire.csv")

    # figure_out_rel_cont_ids(faire_mapper=faire_mapper)

    df = pd.read_csv("/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/Orphan_Projects_Metadata.csv")
    dfs = df.groupby("expedition_id")

    parent_path = Path("/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean")
    
    for expedition_id, df in dfs:
         dir = parent_path / expedition_id
         dir.mkdir(exist_ok=True)
         file_path = dir / f"{expedition_id}_metadata.csv"
         print(file_path)
         df.to_csv(file_path, index=False)
        
            
if __name__ == "__main__":
    main()
