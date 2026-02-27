from faire_mapping.sample_metadata_mapper import FaireSampleMetadataMapper
import pandas as pd
import numpy as np
from faire_mapping.transformers.sample_metadata_transformer import SampleMetadataTransformer
from faire_mapping.transformers.rules import (
    get_material_samp_id_by_cruisecode_cast_btlnum,
    get_fallback_col_mapping_rule,
    get_date_duration_rule,
    get_tot_depth_water_col_from_lat_lon_or_exact_col,
    get_altitude_from_maxdepth_and_totdepthcol,
    get_eventDate_iso8601_rule,
    get_minimum_depth_from_max_minus_1m,
    get_dna_yield_from_conc_and_vol,
    get_standardized_station_id_from_nonstandardized_station_name,
    get_stations_within_5km_of_lat_lon,
    get_line_id_from_standardized_station,
)

from functools import partial

cruise_id_short_to_long_dict = {
    "OC0821": "OC0821 OCNMS R/V Storm Petrel",
    "OC1021": "OC1021 OCNMS R/V Storm Petrel",
    "OC0622": "OC0622 OCNMS R/V Storm Petrel",
    "OC0722": "OC0722 OCNMS R/V Storm Petrel",
    "OC0822": "OC0822 OCNMS R/V Storm Petrel",
    "OC0922": "OC0922 OCNMS R/V Storm Petrel",
    "OC0623": "OC0623 OCNMS R/V Storm Petrel",
    "OC0723": "OC0723 OCNMS R/V Storm Petrel"
}

def fix_recorded_by(df: pd.DataFrame) -> pd.DataFrame:
    """Make pipe separate"""
    df['recordedBy'] = df['recordedBy'].str.replace('and', ' | ', regex=False).str.replace('nM', 'n M').str.replace('yN', 'y N').str.replace('tG', 't G').str.replace('nB', 'n B')
    return df

def get_expedition_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map the expedition name based on short cruise codes
    """
    df['expedition_name'] = df['expedition_id'].map(cruise_id_short_to_long_dict).fillna('not_applicable: control sample')
    return df
        
def main() -> None:

    additional_rules = [
        get_material_samp_id_by_cruisecode_cast_btlnum,
        partial(get_fallback_col_mapping_rule, faire_field_name='temp'),
        partial(get_fallback_col_mapping_rule, faire_field_name='salinity'),
        partial(get_fallback_col_mapping_rule, faire_field_name='diss_oxygen'),
        get_date_duration_rule,
        get_tot_depth_water_col_from_lat_lon_or_exact_col,
        get_altitude_from_maxdepth_and_totdepthcol,
        get_eventDate_iso8601_rule,
        get_minimum_depth_from_max_minus_1m,
        get_dna_yield_from_conc_and_vol,
        get_standardized_station_id_from_nonstandardized_station_name,
        get_stations_within_5km_of_lat_lon,
        get_line_id_from_standardized_station,
                        ]
    
    sample_mapper = FaireSampleMetadataMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/OCNMS/ctd_ocnms21-23/config.yaml',
                                              additiona_rules=additional_rules,
                                              ome_auto_setup=True)
    
    df = sample_mapper.finalize_samp_metadata_df()
    
    # Add custom expedition_name
    df = get_expedition_name(df=df)
    df = fix_recorded_by(df=df)

    df = sample_mapper.save_final_df_as_csv(final_df=df, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path="/home/poseidon/zalmanek/FAIRe-Mapping/projects/OCNMS/ctd_ocnms21-23/data/ctd_ocnms21-23_farie.csv")
                

if __name__ == "__main__":
    main()