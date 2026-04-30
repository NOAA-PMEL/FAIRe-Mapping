from faire_mapping.sample_metadata_mapper import FaireSampleMetadataMapper
from faire_mapping.transformers.rules import (get_exact_mappings_rule,
                                              get_constant_mappings_rule,
                                              get_samp_category_rule,
                                              get_geo_loc_name_by_lat_lon_rule,
                                              get_replicte_num_from_ome_sample_name,
                                              get_iso8601_date_from_date_col_and_time_col_separately,
                                              get_date_duration_rule,
                                              get_env_local_scale_by_depth,
                                              get_standardized_station_id_from_nonstandardized_station_name,
                                              get_stations_within_5km_of_lat_lon,
                                              get_line_id_from_standardized_station,
                                              get_dna_yield_from_conc_and_vol,
                                              get_minimum_depth_from_max_minus_1m
                                              )
from functools import partial

def main() -> None:

    additional_rules = [
        get_exact_mappings_rule,
        get_constant_mappings_rule,
        get_samp_category_rule,
        get_replicte_num_from_ome_sample_name,
        get_geo_loc_name_by_lat_lon_rule,
        get_iso8601_date_from_date_col_and_time_col_separately,
        get_env_local_scale_by_depth,
        get_minimum_depth_from_max_minus_1m,
        get_dna_yield_from_conc_and_vol,
        get_date_duration_rule,
        get_standardized_station_id_from_nonstandardized_station_name,
        get_stations_within_5km_of_lat_lon,
        get_line_id_from_standardized_station,   
    ]


    sample_mapper = FaireSampleMetadataMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/DCM/config.yaml',
                                              additional_rules=additional_rules,
                                              ome_auto_setup=False,
                                              net_tow_weirdness=False)


    df = sample_mapper.finalize_samp_metadata_df()
    
    sample_mapper.save_final_df_as_csv(final_df=df, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/DCM/data/DCM_faire.csv')
                

if __name__ == "__main__":
    main()