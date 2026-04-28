from faire_mapping.sample_metadata_mapper import FaireSampleMetadataMapper
from faire_mapping.transformers.rules import (get_exact_mappings_rule,
                                              get_constant_mappings_rule,
                                              get_samp_category_rule,
                                              get_net_tow_material_samp_id_by_short_cruise_code_and_net_num,
                                              get_decimalLatitude_Longitude_from_degree_decimal_seconds,
                                              get_pipe_separated_list_of_multiple_values,
                                              get_geo_loc_name_by_lat_lon_rule,
                                              get_iso8601_date_from_date_col_and_time_col_separately,
                                              get_date_duration_rule,
                                              get_env_local_scale_by_depth,
                                              get_str_in_samp_name_mapping_rule,
                                              get_str_in_samp_name_dur_mapping_rule,
                                              get_avg_from_list_of_cols,
                                              get_stdev_from_list_of_cols,
                                              get_standardized_station_id_from_nonstandardized_station_name,
                                              get_stations_within_5km_of_lat_lon,
                                              )
from functools import partial

def main() -> None:

    additional_rules = [
        get_exact_mappings_rule,
        get_constant_mappings_rule,
        get_samp_category_rule,
        get_net_tow_material_samp_id_by_short_cruise_code_and_net_num,
        get_decimalLatitude_Longitude_from_degree_decimal_seconds,
        partial(get_pipe_separated_list_of_multiple_values, faire_field_name='verbatimLongitude'),
        partial(get_pipe_separated_list_of_multiple_values, faire_field_name='verbatimLatitude'),
        # get_geo_loc_name_by_lat_lon_rule,
        get_iso8601_date_from_date_col_and_time_col_separately,
        get_date_duration_rule,
        partial(get_pipe_separated_list_of_multiple_values, faire_field_name='verbatimEventTime'),
        get_env_local_scale_by_depth,
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='samp_mat_process'),
        partial(get_str_in_samp_name_dur_mapping_rule, faire_field_name='samp_store_dur'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='size_frac'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='filter_diameter'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='filter_surface_area'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='filter_material'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='filter_name'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='prepped_samp_store_temp'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='prepped_samp_store_sol'),
        partial(get_avg_from_list_of_cols, faire_field_name='temp'),
        partial(get_stdev_from_list_of_cols, faire_field_name='temp_standard_deviation'),
        partial(get_avg_from_list_of_cols, faire_field_name='wind_speed'),
        partial(get_stdev_from_list_of_cols, faire_field_name='wind_speed_standard_deviation'),
        get_standardized_station_id_from_nonstandardized_station_name,
        get_stations_within_5km_of_lat_lon,
    ]
    #     get_material_samp_id_by_cruisecode_cast_btlnum,
    #     partial(get_fallback_col_mapping_rule, faire_field_name='decimalLongitude'),
    #     partial(get_fallback_col_mapping_rule, faire_field_name='decimalLatitude'),
    #     get_formatted_geo_loc_by_name,
    #     partial(get_fallback_col_mapping_rule, faire_field_name='eventDate'),
    #     partial(get_max_depth_with_pressure_fallback, pressure_cols=['btl_pressure (decibar)'], lat_col='decimalLatitude', depth_cols=['Depth_m']),
    #     partial(get_fallback_col_constant_mapping_rule, faire_field_name="DepthInMeters_method"),
    #     get_env_local_scale_by_depth,
    #     get_samp_store_dur_from_samp_name,
    #     get_samp_store_temp_from_samp_name,
    #     get_samp_store_loc_from_samp_name,
    #     get_date_duration_rule,
    #     get_minimum_depth_from_max_minus_1m,
    #     get_tot_depth_water_col_from_lat_lon_or_exact_col,
    #     get_standardized_station_id_from_nonstandardized_station_name,
    #     get_stations_within_5km_of_lat_lon,
    #     get_line_id_from_standardized_station,
    #     get_altitude_from_maxdepth_and_totdepthcol,
    #     get_nucl_acid_ext_and_nucl_acid_ext_modify_by_word_in_extract_col,
    #     get_dna_yield_from_conc_and_vol,
    #     get_date_ext_iso8601_rule,
    # ]

    sample_mapper = FaireSampleMetadataMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/WCOA/wcoa21_net_tow/config.yaml',
                                              additional_rules=additional_rules,
                                              ome_auto_setup=False,
                                              net_tow_weirdness=True)


    df = sample_mapper.finalize_samp_metadata_df()

    # Custom/didn't want to write rules for
    df['size_frac_low'] = (df['size_frac_low'].str.replace('Bongo', '').str.replace('Vertical', '').str.strip())
    
    sample_mapper.save_final_df_as_csv(final_df=df, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/WCOA/wcoa21_net_tow/data/wcoa21_nettow_faire.csv')
                

if __name__ == "__main__":
    main()