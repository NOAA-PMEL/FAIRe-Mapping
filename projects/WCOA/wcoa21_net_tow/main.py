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
                                              get_line_id_from_standardized_station,
                                              get_date_ext_iso8601_rule,
                                              get_nucl_acid_ext_and_nucl_acid_ext_modify_by_word_in_extract_col
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
        get_geo_loc_name_by_lat_lon_rule,
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
        get_line_id_from_standardized_station,
        get_date_ext_iso8601_rule,
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='nucl_acid_ext_lysis'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='nucl_acid_ext'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='nucl_acid_ext_kit'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='nucl_acid_ext_modify'),
        partial(get_str_in_samp_name_mapping_rule, faire_field_name='nucl_acid_ext_method_additional'),
        get_nucl_acid_ext_and_nucl_acid_ext_modify_by_word_in_extract_col # used for samp_collect_device - need to change rule to be broader since it can apply to any str present. In V2 of FAIRe-mapping so I don't have to go back and change all the .main() files
    ]

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