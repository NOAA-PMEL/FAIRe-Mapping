from faire_mapping.faire_mapper import OmeFaireMapper
from faire_mapping.mapping_builders.sample_extract_mapping_dict_builder import SampleExtractionMappingDictBuilder
from faire_mapping.dataframe_and_dict_builders.base_df_builder import BaseDfBuilder
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from faire_mapping.custom_exception import NoInsdcGeoLocError
from faire_mapping import extract_insdc_geographic_locations, ExtractionMetadataBuilder
from pathlib import Path
from datetime import datetime
from difflib import SequenceMatcher



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
        return "missing: not collected"

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

def remove_cruise_codes_from_samp_dict(samp_dict: dict) -> dict:
    for control_samp, list_of_samps in samp_dict.items():
        updated_list_of_samps = []
        for samp in list_of_samps:
            samp_parts = samp.split('.')
            del samp_parts[-1] # remove last item in list (cruise code)
            updated_samp = '.'.join(samp_parts)
            updated_list_of_samps.append(updated_samp)
        samp_dict[control_samp] = updated_list_of_samps

    return samp_dict

def convert_date_to_iso8601(date: str) -> datetime:
        # converts strings from 2021/11/08 00:00:00 to iso8601 format  to 2021-11-08T00:00:00Z
        # also converts strings from 5/1/2024 to 2024-01-05T00:00:00Z
        # And coverts 2024-05-01 to 2024-01-05T00:00:00Z
        # Also handles years like 0022 and corrects them to 2022
        has_time_component = False

        date = str(date)
        
        if date in  ['None', 'nan', 'missing: not collected', '', 'missing: not provided']:
            return "missing: not provided"

        # 1. Handle full ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ) and return immediately
        try: 
            # Use the correct format including T and Z
            datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
            return date
        except ValueError: 
            pass # Not that format, continue to check others

        # 2. Check for other supported formats
        
        # Format 2021/11/08 00:00:00
        if "/" in date and ":" in date: 
            dt_obj = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
            has_time_component = True
            
        # Format 5/1/2024 or 5/1/24
        elif "/" in date and ":" not in date: 
            try: # 5/1/2024 format
                dt_obj = datetime.strptime(date, "%m/%d/%Y")
            except ValueError:
                try: # foramt 5/1/24
                    dt_obj = datetime.strptime(date, "%m/%d/%y")
                except ValueError:
                    raise ValueError(f"Unsupported slash-separated dae format: {date}")
        
        # --- FIX APPLIED HERE: Handle all dash-separated formats gracefully ---
        elif "-" in date:
            if ':' in date:
                # 2.1. Try ISO-like T-separated time (e.g., 2023-04-24T08:51:00)
                try:
                    dt_obj = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
                    has_time_component = True
                except ValueError:
                    # 2.2. Fallback: Try space-separated time (e.g., 2023-04-24 08:51:00)
                    try:
                        dt_obj = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                        has_time_component = True
                    except ValueError:
                        try:
                            # %z handles the +00:00 or +0000 part
                            dt_obj = datetime.strptime(date, "%Y-%m-%d %H:%M:%S%z")
                            has_time_component = True
                        except ValueError:
                            try:
                            # Try usin dateutil library
                                from dateutil import parser
                                dt_obj=parser.parse(date)
                                return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
                            except ValueError:
                                # Failed both time formats, raise error
                                raise ValueError(f"Unsupported dash-separated date/time format: {date}")
            else:
                # 2.3. Date-only format (e.g., 2024-04-10)
                dt_obj = datetime.strptime(date, "%Y-%m-%d")
        # ---------------------------------------------------------------------

        else:
            print(date)
            raise ValueError(f"Unsupported date format: {date}")
        
        # Correct years that are clearly wrong (like 0022 -> 2022)
        if dt_obj.year < 100:
            corrected_year = 2000 + dt_obj.year
            dt_obj = dt_obj.replace(year=corrected_year)

        # Only add time component if it was in the original string
        if has_time_component:
            return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            return dt_obj.strftime("%Y-%m-%d")

def get_blanks_info(concat_extract_df: pd.DataFrame, metadata_df: pd.DataFrame):
    #  1. blanks to samps dict -> {'blank_samp': ['samp1', samp2]}
    blank_to_samps = {}
    extract_dfs = concat_extract_df.groupby('extract_id')
    for extract_id, df in extract_dfs:
        extract_samps = df['samp_name'].to_list()
         
        blanks = [s for s in extract_samps if 'blank' in s.lower()]
        for blank in blanks:
             other_samps = [s for s in extract_samps if s!= blank]
             blank_to_samps[blank.replace(' ', '_')] = other_samps

    # 2. Nc samps to samps dict -> {'nc_samp': ['samp1', samp2]}
    nc_to_samps = {}
    expedition_dfs = metadata_df.groupby('expedition_id')
    for exp, df in expedition_dfs:
        samps = df['samp_name'].to_list()

        nc_samps = [s for s in samps if '.nc' in s.lower()]
        for nc in nc_samps:
            other_samps = [s for s in samps if s != nc]
            nc_to_samps[nc] = other_samps

    # 3. update both blanks and NC dictionaries to remove cruise code to make matching easier
    blank_to_samps = remove_cruise_codes_from_samp_dict(samp_dict=blank_to_samps)
    nc_to_samps = remove_cruise_codes_from_samp_dict(samp_dict=nc_to_samps) 


    # 4 Next creaete dict of all samples and their rel cont ids
    blanks_to_be_added = []
    samp_rel_cont_id_dict = {}
    for i, r in metadata_df.iterrows():
        samp_name = r['samp_name']
        if '.nc' not in samp_name.lower():
            samp_parts = samp_name.split('.')
            del samp_parts[-1] # remove last item in list (cruise code)
            updated_samp = '.'.join(samp_parts)
            related_controls = []
            for blank, samps in blank_to_samps.items():
                if updated_samp in samps:
                    related_controls.append(blank)
                    if blank not in blanks_to_be_added:
                        blanks_to_be_added.append(blank)
            for nc, samps in nc_to_samps.items():
                if updated_samp in samps:
                    related_controls.append(nc)
            samp_rel_cont_id_dict[samp_name] = ' | '.join(related_controls)

    # Need to add these blanks that weren't associated with anything. Sean 3/20/2026
    blanks_to_be_added.extend(['Blank1C.QIAvac Test', 'Blank3Q.QIAvac Test'])

    return samp_rel_cont_id_dict, blanks_to_be_added
         
def figure_out_rel_cont_ids(faire_mapper: OmeFaireMapper, metadata_df: pd.DataFrame):
     
    config = faire_mapper.load_config(config_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/config.yaml')
    extraction_info = config['extractions']

    extraction_mapping_dict_builder = ExtractionMetadataBuilder(extractions_info=extraction_info,
                                                                google_sheet_json_cred=config['json_creds'],
                                                                update_mapping_dict=False)
    #  print(extraction_mapping_dict_builder.extraction_df)
    extraction_mapping_dict_builder.extraction_df.to_csv("/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/data/extractions.csv")

    blank_dict, blanks_to_be_added = get_blanks_info(concat_extract_df=extraction_mapping_dict_builder.extraction_df, metadata_df=metadata_df)
    metadata_df['rel_cont_id'] = metadata_df['samp_name'].map(blank_dict)

    return metadata_df, blanks_to_be_added, extraction_mapping_dict_builder.extraction_df, blank_dict

def add_other_ext_values_to_new_row_dict(associated_samps: list,
                                          faire_df_field: str, 
                                          faire_df: pd.DataFrame,
                                          new_row_dict: dict):
    
    filtered_faire_df = faire_df[faire_df["samp_name"].isin(associated_samps)]
            
    faire_field_values = filtered_faire_df[faire_df_field].unique()
    if len(faire_field_values) == 1:
        new_row_dict[faire_df_field] = faire_field_values[0]
        return new_row_dict
    else:
        Warning(f"Too many values for {faire_df_field}: {faire_field_values}. Don't know which one to choose.")
        new_row_dict[faire_df_field] = "not applicable"
        return new_row_dict

def calculate_dna_yield(metadata_row: pd.Series, sample_vol_metadata_col: str, extraction_blank: bool = False) -> float:
    # calculate the dna yield based on the concentration (ng/uL) and the sample_volume (mL).
    concentration = str(metadata_row['extraction_conc'])
    
    if extraction_blank: # common column created in extraction builder
        sample_vol = str(metadata_row['extraction_blank_vol_dna_ext']).replace('~','')
    else: # for all other samples will be whatever col is sepcified.
        sample_vol = str(metadata_row[sample_vol_metadata_col]).replace('~','')
    
    if concentration == '' or concentration == None or concentration == 'nan':
        return 'not applicable'

    try:
        concentration = float(concentration)
        sample_vol = float(sample_vol)
        dna_yield = (concentration * 100)/sample_vol
        return round(dna_yield, 3)
    except:
        if '.nc' in metadata_row['samp_name'].lower():
            return 'not applicable: control sample'
        return 'not applicable'
    
def add_blanks_to_metadata(blanks_to_add: list, concat_extract_df: pd.DataFrame, faire_df: pd.DataFrame, blank_dict):
    blank_dict
    
    new_rows_list = []
    for i, r in concat_extract_df.iterrows():
        if r['samp_name'] in blanks_to_add:
            new_row_dict = {
                'samp_name': r['samp_name'].replace(' ', ''),
                'neg_cont_type': 'extraction negative',
                'samp_category': "negative control",
                'date_ext': convert_date_to_iso8601(date=r['extraction_date']) ,
                'habitat_natural_artificial_0_1': 1,
                'samp_vol_we_dna_ext': r['extraction_blank_vol_dna_ext'],
                'samp_vol_we_dna_ext_unit': 'mL',
                'concentration': r['extraction_conc'],
                'concentration_unit': 'ng/µl',
                'dna_yield': calculate_dna_yield(metadata_row=r, sample_vol_metadata_col='extraction_blank_vol_dna_ext', extraction_blank=True),
                'extract_well_number': r['Well'][-1] if pd.notna(r['Well']) else "not applicable",
                'extract_well_position': r['Well'][0] if pd.notna(r['Well']) else "not applicable"
                }
            
            if r['samp_name'] not in ['Blank1C.QIAvac Test', 'Blank3Q.QIAvac Test']:
                associated_samples = [samp for samp, blanks in blank_dict.items() if r['samp_name'] in blanks.split(" | ")]
                
                # Add values by taking what exists from associated samples in df
                cols_to_get_vals = ['nucl_acid_ext_lysis', 'nucl_acid_ext_sep', 'extract_id', 'nucl_acid_ext', 'nucl_acid_ext_kit',
                                    'dna_cleanup_0_1', 'dna_cleanup_method', 'ratioOfAbsorbance260_280', 'nucl_acid_ext_method_additional',
                                    'extract_plate']
                for faire_df_field in cols_to_get_vals:
                    new_row_dict = add_other_ext_values_to_new_row_dict(associated_samps=associated_samples, 
                                                                        faire_df_field=faire_df_field,
                                                                        faire_df=faire_df,
                                                                        new_row_dict=new_row_dict)
            # ['Blank1C.QiavacTest', 'Blank3Q.QiavacTest'] need to be added even though they aren't associated with any samples (Sean 3/30/2026)
            else:
                new_row_dict['nucl_acid_ext_lysis'] = 'physical | enzymatic | thermal'
                new_row_dict['nucl_acid_ext_sep'] = 'column-based'
                new_row_dict['extract_id'] = 'QiavacTest_1'
                new_row_dict['nucl_acid_ext'] = 'https://doi.org/10.5281/zenodo.15793435'
                new_row_dict['nucl_acid_ext_kit'] = 'Qiagen DNeasy Blood and Tissue Kit'
                new_row_dict['dna_cleanup_0_1'] = '0'
                new_row_dict['dna_cleanup_method'] = 'not applicable'
                new_row_dict['ratioOfAbsorbance260_280'] = 'not applicable'
                new_row_dict['nucl_acid_ext_method_additional'] = 'missing: not provided'
                new_row_dict['extract_plate'] = 'not applicable'
                new_row_dict['concentration'] = 'BDL'

            # Add 'not applicable: control sample' to other columns
            missing_cols = set(faire_df.columns) - set(new_row_dict.keys())
            for col in missing_cols:
                new_row_dict[col] = 'not applicable: control sample'
            
            new_rows_list.append(new_row_dict)
    
    new_rows_df = pd.DataFrame(new_rows_list)
    faire_df = pd.concat([faire_df, new_rows_df], ignore_index=True)

    return faire_df
        
def main() -> None:

    metadata_df_builder = BaseDfBuilder(csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/Orphan_Projects_Metadata.csv')
   
    # Get mapping dict
    mapper_dict_builder = SampleExtractionMappingDictBuilder(google_sheet_mapping_file_id='1vfdQRReGUEl88axV-DAz1hGMhGznh17UfrYWyuSs-Mw',
                                                            google_sheet_json_cred='/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json')
    
    # Just going to use FAIRE-Mapper since extraction is included in metadata and will manually do the few related mappings
    faire_mapper = OmeFaireMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/config.yaml')

    # remove empty rows
    metadata_df_builder.df.dropna(how='all', inplace=True)
    
    sample_faire_metadata_results = {}
    
    # Exact Mappings
    for faire_col, metadata_col in mapper_dict_builder.sample_mapping_dict[faire_mapper.exact_mapping].items():
       sample_faire_metadata_results[faire_col] = faire_mapper.apply_exact_mappings(df=metadata_df_builder.df, faire_col=faire_col, metadata_col=metadata_col)

    for faire_col, metadata_col in mapper_dict_builder.sample_mapping_dict[faire_mapper.constant_mapping].items():
        sample_faire_metadata_results[faire_col] = faire_mapper.apply_static_mappings(df=metadata_df_builder.df, faire_col=faire_col, static_value=metadata_col)

    # --- RELATED MAPPINGS ----
    for faire_col, metadata_col in mapper_dict_builder.sample_mapping_dict[faire_mapper.related_mapping].items():
        
        # samp_category by samp name
        if faire_col == 'biological_rep_relation' and metadata_col:
            sample_faire_metadata_results[faire_col] = add_biological_replicates_column(df=metadata_df_builder.df)

        elif faire_col == "geo_loc_name":
            # The axis=1 belongs to .apply(), not the helper function
            sample_faire_metadata_results[faire_col] = metadata_df_builder.df.apply(
                lambda row: find_geo_loc_by_lat_lon(metadata_row=row, metadata_cols=metadata_col), 
                axis=1
            )

        elif faire_col == "ctd_cast_number":
             # Create the new column
            sample_faire_metadata_results[faire_col] = metadata_df_builder.df[faire_col].apply(extract_cast_number)       

    faire_sample_df = pd.DataFrame(sample_faire_metadata_results)
    rel_cont_id_df, blanks_to_be_added, extraction_df, blank_dict = figure_out_rel_cont_ids(faire_mapper=faire_mapper, metadata_df=faire_sample_df)
    faire_with_blanks = add_blanks_to_metadata(blanks_to_add=blanks_to_be_added, concat_extract_df=extraction_df, faire_df=rel_cont_id_df, blank_dict=blank_dict)

    # Remove positive control that was added in
    faire_with_blanks.drop(faire_with_blanks[faire_with_blanks['samp_name'] == 'E2180.OC0919'].index, inplace=True)
    faire_with_blanks['samp_type'] = 'water'

    # Add in pos_cont_type for positive controls that were included
    pos_cont_type = "ZymoBIOMICS Microbial Community Standard D6300; synthetic microbial community spike in"
    faire_with_blanks['pos_cont_type'] = faire_with_blanks.apply(lambda row: pos_cont_type if row['samp_category'] == 'positive control' else '', axis=1)

    # Fix hydrogen_ion and methane range values
    target_samples = ['H2ox_bag4', 'H2ox_bag2', 'H2ox_bag7', 'H2ox_bag11', 'H2ox_bag19']
    faire_with_blanks['verbatim_hydrogen_ion'] = 'not applicable'
    faire_with_blanks['verbatim_methane'] = 'not applicable'
    mask = faire_with_blanks['samp_name'].isin(target_samples)
    faire_with_blanks.loc[mask, 'verbatim_hydrogen_ion'] = faire_with_blanks.loc[mask, 'hydrogen_ion']
    faire_with_blanks.loc[mask, 'verbatim_methane'] = faire_with_blanks.loc[mask, 'methane']
    float_regex = r'(\d+\.\d+|\d+)'
    faire_with_blanks.loc[mask, 'hydrogen_ion'] = faire_with_blanks.loc[mask, 'hydrogen_ion'].str.extract(float_regex)[0]
    faire_with_blanks.loc[mask, 'methane'] = faire_with_blanks.loc[mask, 'methane'].str.extract(float_regex)[0]


    faire_mapper.save_final_df_as_csv(final_df=faire_with_blanks, sheet_name="sampleMetadata", header=2, csv_path="/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/data/orphan_faire.csv")
                         
if __name__ == "__main__":
    main()


