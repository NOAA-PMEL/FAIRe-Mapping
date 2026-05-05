from faire_mapping.sample_metadata_mapper import FaireSampleMetadataMapper
import pandas as pd
import numpy as np
from faire_mapping.transformers.sample_metadata_transformer import SampleMetadataTransformer
from faire_mapping.transformers.rules import (
    get_material_samp_id_by_cruisecode_cast_btlnum,
    get_formatted_geo_loc_by_name,
    get_fallback_col_mapping_rule,
    get_max_depth_with_pressure_fallback,
    get_minimum_depth_from_max_minus_1m,
    get_env_local_scale_by_depth,
    get_date_duration_rule,
    get_tot_depth_water_col_from_lat_lon_or_exact_col,
    get_condition_constant_rule,
    get_altitude_from_maxdepth_and_totdepthcol,
    get_condition_constant_rule,
    get_wind_direction_from_wind_degrees,
    get_eventDate_iso8601_rule,
    get_dna_yield_from_conc_and_vol,
    get_nucl_acid_ext_and_nucl_acid_ext_modify_by_word_in_extract_col,
    get_standardized_station_id_from_nonstandardized_station_name,
    get_stations_within_5km_of_lat_lon,
    get_line_id_from_standardized_station,
)

from functools import partial

def fix_station_errors(df: pd.DataFrame) -> pd.DataFrame:
    # swap stations that were incorrectly written down (discussions with Shannon)
    replacements = {
        '70M6': '70M02/M2',
    }

    df['Station'] = df['Station'].replace(replacements)

    return df

def main() -> None:

    # create sample metadata - experiment metadata needed first
    # sample_metadata = create_dy2206_sample_metadata()
    additional_rules = [
        get_material_samp_id_by_cruisecode_cast_btlnum,
        get_formatted_geo_loc_by_name,
        partial(get_fallback_col_mapping_rule, faire_field_name='pressure'),
        partial(get_max_depth_with_pressure_fallback, pressure_cols=['ctd_pressure', 'btl_pressure..decibar.'], lat_col='btl_latitude..degrees_north.', depth_cols=['Depth_m_notes']),
        partial(get_condition_constant_rule, faire_col='DepthInMeters_method', ref_col='Depth_m_notes'),
        get_minimum_depth_from_max_minus_1m,
        get_env_local_scale_by_depth,
        get_date_duration_rule,
        get_tot_depth_water_col_from_lat_lon_or_exact_col,
        partial(get_condition_constant_rule, faire_col='tot_depth_water_col_method', ref_col='ctd_Water_Depth..dbar.'),
        get_altitude_from_maxdepth_and_totdepthcol,
        get_wind_direction_from_wind_degrees,
        get_eventDate_iso8601_rule,
        get_dna_yield_from_conc_and_vol,
        get_nucl_acid_ext_and_nucl_acid_ext_modify_by_word_in_extract_col,
        get_standardized_station_id_from_nonstandardized_station_name,
        get_stations_within_5km_of_lat_lon,
        get_line_id_from_standardized_station
                        ]
    
    sample_mapper = FaireSampleMetadataMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/EcoFoci/dy2206/config.yaml',
                                              additional_rules=additional_rules,
                                              ome_auto_setup=True)
    sample_mapper.sample_metadata_df_builder.sample_metadata_df = fix_station_errors(df=sample_mapper.sample_metadata_df_builder.sample_metadata_df)
    df = sample_mapper.finalize_samp_metadata_df()
    df = sample_mapper.save_final_df_as_csv(final_df=df, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/tests/sample_mapper/dy2206_test/test.csv')






    # transformer = SampleMetadataTransformer(sample_mapper=sample_mapper, ome_auto_setup=True)
    # transforer.insert_rule_before('biological_rep_relation', rule5)
    # additional_rules = [
    #     get_material_samp_id_by_cruisecode_cast_btlnum(sample_mapper),
    #     get_formatted_geo_loc_by_name(sample_mapper),
    #     get_fallback_col_mapping_rule(sample_mapper, faire_field_name='pressure'),
    #     get_max_depth_with_pressure_fallback(mapper=sample_mapper, pressure_cols=['ctd_pressure', 'btl_pressure..decibar.'], lat_col='btl_latitude..degrees_north.', depth_cols=['Depth_m_notes']),
    #     get_condition_constant_rule(mapper=sample_mapper, faire_col='DepthInMeters_method', ref_col='Depth_m_notes'),
    #     get_minimum_depth_from_max_minus_1m(sample_mapper),
    #     get_env_local_scale_by_depth(sample_mapper),
    #     get_date_duration_rule(sample_mapper),
    #     get_tot_depth_water_col_from_lat_lon_or_exact_col(sample_mapper),
    #     get_condition_constant_rule(mapper=sample_mapper, faire_col='tot_depth_water_col_method', ref_col='ctd_Water_Depth..dbar.'),
    #     get_altitude_from_maxdepth_and_totdepthcol(sample_mapper),
    #     get_wind_direction_from_wind_degrees(sample_mapper),
    #     get_eventDate_iso8601_rule(sample_mapper),
    #     get_dna_yield_from_conc_and_vol(sample_mapper),
    #     get_nucl_acid_ext_and_nucl_acid_ext_modify_by_word_in_extract_col(sample_mapper),
    #     get_standardized_station_id_from_nonstandardized_station_name(sample_mapper),
    #     get_stations_within_5km_of_lat_lon(sample_mapper),
    #     get_line_id_from_standardized_station(sample_mapper)
    #                     ]
    # transformer.add_custom_rules(additional_rules)
    # sample_metadata_df = transformer.transform()

    # # Step 4: fill in NA with missing not collected or not applicable because they are samples and adds NC to rel_cont_id
    # sample_df = sample_mapper.fill_empty_sample_values_and_finalize_sample_df(df = pd.DataFrame(sample_metadata_df))
    
    # # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    # # nc_df = sample_mapper.fill_nc_metadata()
    # controls_df = sample_mapper.finish_up_controls_df(final_sample_df=sample_df)
    
    # controls_df.to_csv("/home/poseidon/zalmanek/FAIRe-Mapping/tests/sample_mapper/dy2206_test/test.csv")
                

if __name__ == "__main__":
    main()