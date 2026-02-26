from faire_mapping.sample_metadata_mapper import FaireSampleMetadataMapper
import pandas as pd
import numpy as np
from faire_mapping.transformers.sample_metadata_transformer import SampleMetadataTransformer
from faire_mapping.transformers.rules import (
    get_pps_material_samp_id_by_code_prefix_and_cast,
    # get_formatted_geo_loc_by_name,
    get_fallback_col_mapping_rule,
    get_fallback_col_constant_mapping_rule,
    # get_max_depth_with_pressure_fallback,
    # get_minimum_depth_from_max_minus_1m,
    # get_env_local_scale_by_depth,
    get_date_duration_rule,
    get_tot_depth_water_col_from_lat_lon_or_exact_col,
    # get_condition_constant_rule,
    get_altitude_from_maxdepth_and_totdepthcol,
    get_condition_constant_rule,
    # get_wind_direction_from_wind_degrees,
    get_eventDate_iso8601_rule,
    get_dna_yield_from_conc_and_vol,
    # get_nucl_acid_ext_and_nucl_acid_ext_modify_by_word_in_extract_col,
    get_standardized_station_id_from_nonstandardized_station_name,
    get_stations_within_5km_of_lat_lon,
    # get_line_id_from_standardized_station,
)

from functools import partial

deploy_recover_dict = {
        'CE042-PPS-0821': ('2021-08-25', '2021-10-18'),
        'TH042-PPS-0821': ('2021-08-25', '2021-10-08'),
        'TH042-PPS-0622': ('2022-06-21', '2022-07-19'),
        'TH042-PPS-0822': ('2022-08-22', '2022-09-20'),
        'TH042-PPS-0623': ('2023-06-15', '2023-07-26')
    }
        
def get_stationed_sample_dur(mapper: FaireSampleMetadataMapper, df: pd.DataFrame) -> pd.DataFrame:
    """
    Gets the custom stationed_sample_dur as specified by Shannon in GH issue:
    https://github.com/NOAA-PMEL/Ocean-Molecular-Ecology/issues/29
    """
    # Add deployed date and recover date cols
    # Map the ID to the tuple, then grab the first element [0]
    df['deployed_date'] = df['expedition_id'].map(deploy_recover_dict).str[0]  
    df['recovered_date'] = df['expedition_id'].map(deploy_recover_dict).str[1] 

    df['stationed_sample_dur'] = df.apply(lambda row: mapper.calculate_date_duration(
        metadata_row=row, start_date_col='deployed_date',end_date_col='recovered_date'),
                                          axis=1)
    
    df = df.drop(['deployed_date', 'recovered_date'], axis=1)

    return df

def fill_samp_collect_notes(expedition_id) -> str:
    """
    Fills the samp_collect_notes as specified by Shannon in GH issue: 
    https://github.com/NOAA-PMEL/Ocean-Molecular-Ecology/issues/29
    """

    recovery_dates = deploy_recover_dict.get(expedition_id, None)
    if recovery_dates is not None:
        deployed_date, recovered_date = recovery_dates[0], recovery_dates[1]
        return f"Automated sampler deployed on {deployed_date} and recovered on {recovered_date}"
    else:
        return "not applicable: control sample"

def main() -> None:

    additional_rules = [
        get_pps_material_samp_id_by_code_prefix_and_cast,
        # partial(get_max_depth_with_pressure_fallback, pressure_cols=['ctd_pressure', 'btl_pressure..decibar.'], lat_col='btl_latitude..degrees_north.', depth_cols=['Depth_m_notes']),
        # partial(get_condition_constant_rule, faire_col='DepthInMeters_method', ref_col='Depth_m_notes'),
        # get_minimum_depth_from_max_minus_1m,
        # get_env_local_scale_by_depth,
        get_date_duration_rule,
        get_tot_depth_water_col_from_lat_lon_or_exact_col,
        # partial(get_condition_constant_rule, faire_col='tot_depth_water_col_method', ref_col='ctd_Water_Depth..dbar.'),
        get_altitude_from_maxdepth_and_totdepthcol,
        # get_wind_direction_from_wind_degrees,
        get_eventDate_iso8601_rule,
        get_dna_yield_from_conc_and_vol,
        # get_nucl_acid_ext_and_nucl_acid_ext_modify_by_word_in_extract_col,
        get_standardized_station_id_from_nonstandardized_station_name,
        get_stations_within_5km_of_lat_lon,
        # get_line_id_from_standardized_station
                        ]
    
    sample_mapper = FaireSampleMetadataMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/OCNMS/pps_ocnms21-23/config.yaml',
                                              additiona_rules=additional_rules,
                                              ome_auto_setup=True)
    
    df = sample_mapper.finalize_samp_metadata_df()
   
   
    # Add custom samp_collect_notes
    df['samp_collect_notes'] = df['expedition_id'].apply(fill_samp_collect_notes)
    # Add stationed_samp_dur custom
    df = get_stationed_sample_dur(mapper=sample_mapper, df=df)

    df.to_csv("/home/poseidon/zalmanek/FAIRe-Mapping/projects/OCNMS/pps_ocnms21-23/data/pps_ocnms21-23_farie.csv")
                

if __name__ == "__main__":
    main()