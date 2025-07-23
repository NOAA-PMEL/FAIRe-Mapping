import sys
sys.path.append("../../..")

from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from utils.experiment_run_metadata_mapper import ExperimentRunMetadataMapper
import pandas as pd


def fix_stations(df: pd.DataFrame)  -> pd.DataFrame:
    # swap stations that were incorrectly written down (discussions with Shannon)
    replacements = {
        'AW1': 'UT5',
        'AW2': 'UT4',
        'AW3': 'UT3',
        'AW4': 'UT2',
        'AW5': 'UT1',
        'AE5': 'AW5',
        'UT5': 'AE5',
        'UT3': 'AE3',
        'UT2': 'AE2',
        'UT1': 'AE1',
        'AE3': 'AW3'
    }

    df['Station'] = df['Station'].replace(replacements)

    return df
def create_dy2306_sample_metadata():
    
    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='config.yaml')
    sample_mapper.sample_metadata_df = fix_stations(df=sample_mapper.sample_metadata_df)
    
    sample_metadata_results = {}

    ### Add Sample Mappings ######
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

        elif faire_col == 'materialSampleID' or faire_col == 'sample_derived_from':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_material_sample_id(metadata_row=row),
                axis=1
            )

        elif faire_col == 'biological_rep_relation':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_biological_replicates(metadata_row=row, faire_missing_val='not applicable'),
                axis=1
            )

        # latitude and longitude need to be processed before tot_depth_water_col
        elif faire_col == 'decimalLongitude' or faire_col == 'decimalLatitude' or faire_col == 'tot_depth_water_col':
            # Format decimalLatitude first
            latitude_cols = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('decimalLatitude')
            lat_coord_cols = latitude_cols.split(' | ')
            decimal_latitude = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=lat_coord_cols[0], use_if_na_col_name=lat_coord_cols[1]),
                axis=1
            )
            sample_metadata_results['decimalLatitude'] = decimal_latitude
            sample_mapper.sample_metadata_df['decimalLatitude'] = decimal_latitude

            # Format decimal Longitude second
            longitude_cols =  sample_mapper.mapping_dict[sample_mapper.related_mapping].get('decimalLongitude')
            long_coord_cols = longitude_cols.split(' | ')
            decimal_longitude = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=long_coord_cols[0], use_if_na_col_name=long_coord_cols[1]),
                axis=1
            )
            sample_metadata_results['decimalLongitude'] = decimal_longitude
            sample_mapper.sample_metadata_df['decimalLongitude'] = decimal_longitude

            # Now can calculate tot_depth_water_col from processed decimalLatitude and decimalLongitude
            tot_depth_water_col = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_tot_depth_water_col_from_lat_lon(metadata_row=row, lat_col='decimalLatitude', lon_col='decimalLongitude'),
                axis=1
            )
            sample_metadata_results['tot_depth_water_col'] = tot_depth_water_col
            sample_mapper.sample_metadata_df['tot_depth_water_col'] = tot_depth_water_col

            # Now can caluclate maxdepth from pressure and latitude and min depth from max depth
            depth_metadata_cols = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('maximumDepthInMeters').split(' | ')
            depth_from_pressure = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_depth_from_pressure(metadata_row=row, press_col_name=depth_metadata_cols[0], lat_col_name='decimalLatitude'),
                axis = 1
            )
            sample_mapper.sample_metadata_df['depth_from_pressure'] = depth_from_pressure
            max_depth = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name='depth_from_pressure', use_if_na_col_name=depth_metadata_cols[1]),
                axis=1
            )
            sample_mapper.sample_metadata_df['FinalDepth'] = max_depth
            sample_metadata_results['maximumDepthInMeters'] = max_depth
            sample_metadata_results['minimumDepthInMeters'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name='FinalDepth'),
                axis=1
            )

            # Add DepthInMeters_method since some were calcualted using pressure
            depth_method_info = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('DepthInMeters_method')
            sample_metadata_results['DepthInMeters_method'] = sample_mapper.sample_metadata_df['depth_from_pressure'].apply(
                lambda row: depth_method_info if pd.notna(row) else None
            )
            
            sample_metadata_results['env_local_scale'] = sample_mapper.sample_metadata_df['FinalDepth'].apply(sample_mapper.calculate_env_local_scale)

            # Get altitude to totl_depth_water_col and maximumDepthInMeters
            sample_metadata_results['altitude'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_altitude(metadata_row=row, depth_col='FinalDepth', tot_depth_col='tot_depth_water_col'),
                axis=1
            )

            # Get stations info
            station_id_metadata_col = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('station_id')
            station_id = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_station_id_from_unstandardized_station_name(metadata_row=row, unstandardized_station_name_col=station_id_metadata_col), 
                axis=1
            )

            sample_metadata_results['station_id'] = station_id
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
    
        
        elif faire_col == 'geo_loc_name':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.format_geo_loc(metadata_row=row, geo_loc_metadata_col=metadata_col),
                axis=1
            )

        elif faire_col == 'samp_store_dur':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.get_samp_store_dur)
            
            # Add samp_store_loc based of samp_store_dur, so needs to come after samp_store_dur is calculated
            sample_metadata_results['samp_store_loc'] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.get_samp_store_loc_by_samp_store_dur
            )
            sample_metadata_results['samp_store_temp'] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.get_samp_sore_temp_by_samp_store_dur
            )
        
        # eventDate needs to be proecessed before prepped_samp_store_dur
        elif faire_col == 'eventDate' or faire_col == 'prepped_samp_store_dur':
            event_dates = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('eventDate')
            date_cols = event_dates.split(' | ')
            sample_collection_date = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=date_cols[0], use_if_na_col_name=date_cols[1], transform_use_col_to_date_format=True),
                axis=1
            )
            sample_metadata_results['eventDate'] = sample_collection_date
            sample_mapper.sample_metadata_df['eventDate'] = sample_collection_date

            prepped_samp_dates = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('prepped_samp_store_dur')
            date_col_names = prepped_samp_dates.split(' | ')
            sample_metadata_results['prepped_samp_store_dur'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_date_duration(metadata_row=row, start_date_col='eventDate', end_date_col=date_col_names[1]),
                axis=1
            )
        
        elif faire_col == 'date_ext':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.convert_date_to_iso8601)

        elif faire_col == 'extract_id':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.create_extract_id
            )

        elif faire_col == 'dna_yield':
            metadata_cols = metadata_col.split(' | ')
            sample_vol_col = metadata_cols[1]
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_dna_yield(metadata_row=row, sample_vol_metadata_col=sample_vol_col),
                axis = 1
            )

        elif faire_col == 'nucl_acid_ext' or faire_col == 'nucl_acid_ext_modify':
            metadata_cols = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_constant_value_based_on_str_in_col(metadata_row=row, 
                                                                                 col_name=metadata_cols[0], 
                                                                                 str_condition='QiaVac', 
                                                                                 pos_condition_const=metadata_cols[2],
                                                                                 neg_condition_const=metadata_cols[1]),
                                                                                 axis=1)
    
    # Step 4: fill in NA with missing not collected or not applicable because they are samples and adds NC to rel_cont_id
    sample_df = sample_mapper.fill_empty_sample_values_and_finalize_sample_df(df = pd.DataFrame(sample_metadata_results))
    
    # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    # nc_df = sample_mapper.fill_nc_metadata()
    controls_df = sample_mapper.finish_up_controls_df(final_sample_df=sample_df)
    nucl_acid_ext_map_cols = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('nucl_acid_ext').split(' | ')
    nucl_acid_ext_modify_map_cols = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('nucl_acid_ext_modify').split(' | ')
    controls_df['nucl_acid_ext'] = nucl_acid_ext_map_cols[1]
    controls_df['nucl_acid_ext_modify'] = nucl_acid_ext_modify_map_cols[1]

    # Step 6: Combine all mappings at once (add nc_df if negative controls were sequenced)
    faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df,controls_df])
    # Add rel_cont_id
    faire_sample_df_updated = sample_mapper.add_extraction_blanks_to_rel_cont_id(final_sample_df=faire_sample_df)

    # step 7: save as csv:
    sample_mapper.save_final_df_as_csv(final_df=faire_sample_df_updated, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/EcoFoci/dy2306/data/dy2306_faire.csv')
   
    # # step 7: save to excel file
    # sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.faire_template_file,
    #                                           sheet_name=sample_mapper.sample_mapping_sheet_name, 
    #                                           faire_template_df=faire_sample_df_updated)

    return sample_mapper, faire_sample_df

def main() -> None:

    sample_metadata = create_dy2306_sample_metadata()
                

if __name__ == "__main__":
    main()