from faire_mapping.sample_metadata_mapper import FaireSampleMetadataMapper

def main() -> None:

    # # sample_metadata = create_dy2306_sample_metadata()
    # additional_rules = [
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
                                            #   additiona_rules=additional_rules,
                                              ome_auto_setup=False,
                                              net_tow_weirdness=True)


    df = sample_mapper.finalize_samp_metadata_df()
    
    sample_mapper.save_final_df_as_csv(final_df=df, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/WCOA/wcoa21_net_tow/data/wcoa21_nettow_faire.csv')
                

if __name__ == "__main__":
    main()