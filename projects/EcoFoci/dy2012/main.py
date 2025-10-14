import pandas as pd
import sys
sys.path.append("../../..")
from utils.sample_metadata_mapper import FaireSampleMetadataMapper

# metadata was swapped
def swap_e27_e28_sample_metadata(df: pd.DataFrame) -> pd.DataFrame:
    # According to Sean's email "Note on NCBI submission and swapped metadata" samples, E27_2B_DY20 (SAMN35688246) and E28_1B_DY20 (SAMN35688288) 
    # metadata need to be swapped for sample collection and environmental data (not extraction and downstream)
    columns_to_swap = ['samp_category', 'neg_cont_type', 'pos_cont_type', 'materialSampleID', 'sample_derived_from', 'sample_composed_of', 
                       'biological_rep_relation', 'decimalLongitude', 'decimalLatitude', 'verbatimLongitude', 'verbatimLatitude', 
                       'verbatimCoordinateSystem', 'verbatimSRS', 'eventDate', 'eventDurationValue', 'verbatimEventDate', 'verbatimEventTime', 
                       'env_broad_scale', 'env_local_scale', 'env_medium', 'geo_loc_name', 'habitat_natural_artificial_0_1', 'samp_collect_method', 'samp_collect_device', 
                       'samp_size', 'samp_size_unit', 'samp_weather', 'minimumDepthInMeters', 
                       'maximumDepthInMeters', 'tot_depth_water_col', 'elev', 'temp', 'chlorophyll', 'light_intensity', 'ph', 'ph_meth', 'salinity', 
                       'suspend_part_matter', 'tidal_stage', 'turbidity', 'water_current', 'solar_irradiance', 'wind_direction', 'wind_speed', 'diss_inorg_carb', 
                       'diss_inorg_carb_unit', 'diss_inorg_nitro', 'diss_inorg_nitro_unit', 'diss_org_carb', 'diss_org_carb_unit', 'diss_org_nitro', 
                       'diss_org_nitro_unit', 'diss_oxygen', 'diss_oxygen_unit', 'tot_diss_nitro', 'tot_diss_nitro_unit', 'tot_inorg_nitro', 'tot_inorg_nitro_unit', 
                       'tot_nitro', 'tot_nitro_unit', 'tot_part_carb', 'tot_part_carb_unit', 'tot_org_carb', 'tot_org_carb_unit', 'tot_org_c_meth', 
                       'tot_nitro_content', 'tot_nitro_content_unit', 'tot_nitro_cont_meth', 'tot_carb', 'tot_carb_unit', 'part_org_carb', 'part_org_carb_unit', 
                       'part_org_nitro', 'part_org_nitro_unit', 'nitrate', 'nitrate_unit', 'nitrite', 'nitrite_unit', 'nitro', 'nitro_unit', 'org_carb', 
                       'org_carb_unit', 'org_matter', 'org_matter_unit', 'org_nitro', 'org_nitro_unit', 'ammonium', 'ammonium_unit', 'carbonate', 'carbonate_unit', 
                       'hydrogen_ion', 'nitrate_plus_nitrite', 'nitrate_plus_nitrite_unit', 'omega_arag', 'pco2', 'pco2_unit', 'phosphate', 'phosphate_unit', 'pressure', 
                       'pressure_unit', 'silicate', 'silicate_unit', 'tot_alkalinity', 'tot_alkalinity_unit', 'transmittance', 'transmittance_unit', 'serial_number', 
                       'line_id', 'station_id', 'ctd_cast_number', 'ctd_bottle_number', 'replicate_number', 'organism', 'samp_collect_notes', 
                       'percent_oxygen_sat', 'density', 'air_temperature', 'par', 'air_pressure_at_sea_level', 'expedition_id', 'expedition_name', 'rosette_position', 
                       'density_unit', 'air_temperature_unit', 'par_unit', 'air_pressure_at_sea_level_unit', 'measurements_from', 'recordedBy']
    
    # Create temporary copies of the values
    temp_e27_1b = df.loc[df['samp_name'] == 'E27.1B.DY20-12', columns_to_swap].copy()
    temp_e28_1b = df.loc[df['samp_name'] == 'E28.1B.DY20-12', columns_to_swap].copy()
    temp_e27_2b = df.loc[df['samp_name'] == 'E27.2B.DY20-12', columns_to_swap].copy()
    temp_e28_2b = df.loc[df['samp_name'] == 'E28.2B.DY20-12', columns_to_swap].copy()

    # Perform the swaps
    df.loc[df['samp_name'] == 'E27.1B.DY20-12', columns_to_swap] = temp_e28_1b.values
    df.loc[df['samp_name'] == 'E28.1B.DY20-12', columns_to_swap] = temp_e27_1b.values
    df.loc[df['samp_name'] == 'E27.2B.DY20-12', columns_to_swap] = temp_e28_2b.values
    df.loc[df['samp_name'] == 'E28.2B.DY20-12', columns_to_swap] = temp_e27_2b.values

    return df

def fix_stations(df: pd.DataFrame)  -> pd.DataFrame:
    # swap stations list for DBO4.5 with DBO4.5N, dBO4.3 with DBO4.3N, and dBO4.1 with DBO4.1N
    # shannon says these stations were probably visited and the non-N ones were accidentally written down
    replacements = {
        'DBO4-5': 'DBO4.5N',
        'DBO4-3': 'DBO4.3N',
        'DBO4-1': 'DBO4.1N'
    }

    df['Station'] = df['Station'].replace(replacements)

    return df

def create_dy2012_sample_metadata():

    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='config.yaml')
    sample_mapper.sample_metadata_df = fix_stations(df=sample_mapper.sample_metadata_df)

    sample_metadata_results = {}

    ###### Add Sample Mappings ######
    # Step 1: Add exact mappings
    for faire_col, metadata_col in sample_mapper.mapping_dict[sample_mapper.exact_mapping].items():
        sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
            lambda row: sample_mapper.apply_exact_mappings(metadata_row=row, faire_col=faire_col))
    
    # Step 2: Add constants
    for faire_col, static_value in sample_mapper.mapping_dict[sample_mapper.constant_mapping].items():
        sample_metadata_results[faire_col] = sample_mapper.apply_static_mappings(faire_col=faire_col, static_value=static_value)

    # Step 3: Add related mappings
    for faire_col, metadata_col in sample_mapper.mapping_dict[sample_mapper.related_mapping].items():
        # Add samp_category
        if faire_col == 'samp_category' and metadata_col == sample_mapper.sample_metadata_sample_name_column:
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_samp_category_by_sample_name(metadata_row=row, faire_col=faire_col, metadata_col=metadata_col),
                axis=1
            )
        elif faire_col == 'biological_rep_relation':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_biological_replicates(metadata_row=row, faire_missing_val='not applicable'),
                axis=1
            )
        elif faire_col == 'materialSampleID' or faire_col == 'sample_derived_from':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_material_sample_id(metadata_row=row, cruise_code='DY20-12'),
                axis=1
            )
        elif faire_col == 'wind_direction':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.convert_wind_degrees_to_direction)
        
        elif faire_col == 'geo_loc_name':
            lat = metadata_col.split(' | ')[0]
            lon = metadata_col.split(' | ')[1]
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.find_geo_loc_by_lat_lon(metadata_row=row, metadata_lat_col=lat, metadata_lon_col=lon), 
                    axis = 1
                )

        elif faire_col == 'prepped_samp_store_dur':
            date_col_names = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_date_duration(metadata_row=row, start_date_col=date_col_names[0], end_date_col=date_col_names[1]),
                axis=1
            )
        
        elif faire_col == 'date_ext':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.convert_date_to_iso8601)

        elif faire_col == 'extract_id':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.create_extract_id
            )

        elif faire_col == 'extract_well_number':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_well_number_from_well_field(metadata_row=row, well_col=metadata_col),
                axis=1
            )
        
        elif faire_col == 'extract_well_position':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_well_position_from_well_field(metadata_row=row, well_col=metadata_col),
                axis = 1 
            )

        elif faire_col == 'dna_yield':
            metadata_cols = metadata_col.split(' | ')
            sample_vol_col = metadata_cols[1]
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_dna_yield(metadata_row=row, sample_vol_metadata_col=sample_vol_col),
                axis = 1
            )

        elif faire_col == 'maximumDepthInMeters':
            metadata_cols = metadata_col.split(' | ')
            max_depth = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_depth_from_pressure(metadata_row=row, press_col_name=metadata_cols[0], lat_col_name=metadata_cols[1]),
                axis = 1
            )

            sample_mapper.sample_metadata_df['finalMaxDepth'] = max_depth
            sample_metadata_results[faire_col] = max_depth

            sample_metadata_results['minimumDepthInMeters'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name='finalMaxDepth'),
                axis = 1
            ) 
        
            # Get altitude to totl_depth_water_col and maximumDepthInMeters
            sample_metadata_results['altitude'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_altitude(metadata_row=row, depth_col='finalMaxDepth', tot_depth_col='ctd_Water_Depth..dbar.'),
                axis=1
            )
            
            sample_metadata_results['env_local_scale'] = sample_mapper.sample_metadata_df['finalMaxDepth'].apply(sample_mapper.calculate_env_local_scale)
        
        elif faire_col =='recordedBy':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
                lambda row: row.replace(', ', ' | ')
            )
        
        elif faire_col == 'station_id':
            station_id = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_station_id_from_unstandardized_station_name(metadata_row=row, unstandardized_station_name_col=metadata_col), 
                axis=1
            )
            sample_metadata_results[faire_col] = station_id
            sample_mapper.sample_metadata_df['station_id'] = station_id

            # Use standardized station to get stations within 3 km
            station_metadata_cols = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('station_ids_within_5km_of_lat_lon').split(' | ')
            lat_col = station_metadata_cols[1]
            lon_col = station_metadata_cols[2]
            sample_metadata_results['station_ids_within_5km_of_lat_lon'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_stations_within_5km(metadata_row=row, station_name_col='station_id', lat_col=lat_col, lon_col=lon_col), 
                axis=1)
            
            # Get line_id from standardized station name
            sample_metadata_results['line_id'] = sample_mapper.sample_metadata_df['station_id'].apply(sample_mapper.get_line_id)


    # Step 4: fill in NA with missing not collected or not applicable because they are samples and adds NC to rel_cont_id
    sample_df = sample_mapper.fill_empty_sample_values_and_finalize_sample_df(df = pd.DataFrame(sample_metadata_results))

    swap_samps_sample_df = swap_e27_e28_sample_metadata(df=sample_df)
    
    # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    # nc_df = sample_mapper.fill_nc_metadata()
    controls_df = sample_mapper.finish_up_controls_df(final_sample_df=swap_samps_sample_df)

    # Step 6: Combine all mappings at once (add nc_df if negative controls were sequenced)
    faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, swap_samps_sample_df, controls_df])
    # Add rel_cont_id
    faire_sample_df_updated = sample_mapper.add_extraction_blanks_to_rel_cont_id(final_sample_df=faire_sample_df)
    
    # step 7: save as csv:
    sample_mapper.save_final_df_as_csv(final_df=faire_sample_df_updated, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/EcoFoci/dy2012/data/dy2012_faire.csv')
   
    # # step 8: save to excel file
    # sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.faire_template_file,
    #                                           sheet_name=sample_mapper.sample_mapping_sheet_name, 
    #                                           faire_template_df=faire_sample_df_updated)

    return sample_mapper, faire_sample_df

def main() -> None:

    faire_sample_outputs = create_dy2012_sample_metadata()


if __name__ == "__main__":
    main()

