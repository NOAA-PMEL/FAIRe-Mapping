from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import pandas as pd
import numpy as np
from faire_mapping.transformers.sample_metadata_transformer import SampleMetadataTransformer
from faire_mapping.transformers.rules import (
    get_geo_loc_name_by_lat_lon_rule,
    get_material_samp_id__by_cruisecode_cast_btlnum,
)

def fix_station_errors(df: pd.DataFrame) -> pd.DataFrame:
    # swap stations that were incorrectly written down (discussions with Shannon)
    replacements = {
        '70M6': '70M02/M2',
    }

    df['Station'] = df['Station'].replace(replacements)

    return df

def create_dy2206_sample_metadata():
    
    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='config.yaml')
    sample_mapper.sample_metadata_df = fix_station_errors(df=sample_mapper.sample_metadata_df)

    sample_metadata_results = {}

    #### Add Sample Mappings ######
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
                lambda row: sample_mapper.add_material_sample_id(metadata_row=row, cruise_code='DY22-06'),
                axis=1
            )

        elif faire_col == 'biological_rep_relation':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_biological_replicates(metadata_row=row, faire_missing_val='not applicable'),
                axis=1
            )

        elif faire_col == 'geo_loc_name':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.format_geo_loc(metadata_row=row, geo_loc_metadata_col=metadata_col),
                axis=1
            )

        # Need to make sure maximumDepthInMeters is processed before MinimumDepthinMeters and env_local_scale
        elif "maximumDepthInMeters" in faire_col:
            depth_metadata_cols = metadata_col.split(' | ')

            # Add pressure first
            pressure_metadat_cols = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('pressure').split(' | ')
            pressure = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row,
                                                                                             desired_col_name=pressure_metadat_cols[0],
                                                                                             use_if_na_col_name=pressure_metadat_cols[1],
                                                                                             transform_use_col_to_date_format=False),
                axis=1
            )
            sample_mapper.sample_metadata_df['final_pressure'] = pressure
            sample_metadata_results['pressure'] = pressure

            # calulate max depth from pressure and lat for the rows that include pressure
            max_depth = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_depth_from_pressure(metadata_row=row, press_col_name='final_pressure', lat_col_name=depth_metadata_cols[3]),
                axis = 1
            )
            # save values to metadata df to access in a minute
            sample_mapper.sample_metadata_df['finalMaxDepth'] = max_depth

            # Add maximumDepthInMeters to final results since taking from two columns (some pressure values have NA, so using depth_m_notes)
            final_max_depth = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row, 
                                                                                             desired_col_name='finalMaxDepth', 
                                                                                             use_if_na_col_name=depth_metadata_cols[2],
                                                                                             transform_use_col_to_date_format=False),
                axis=1
            )

            sample_mapper.sample_metadata_df['final_max_depth'] = final_max_depth
            sample_metadata_results['maximumDepthInMeters'] = final_max_depth
            
            sample_metadata_results['minimumDepthInMeters'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name='final_max_depth'),
                axis=1
            )

            # Add DepthInMeters_method since some were calcualted using pressure
            depth_method_info = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('DepthInMeters_method')
            sample_metadata_results['DepthInMeters_method'] = sample_mapper.sample_metadata_df['finalMaxDepth'].apply(
                lambda row: depth_method_info if pd.notna(row) else None
            )

            sample_metadata_results['env_local_scale'] = sample_mapper.sample_metadata_df['final_max_depth'].apply(sample_mapper.calculate_env_local_scale)

        elif faire_col == 'prepped_samp_store_dur':
            date_col_names = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_date_duration(metadata_row=row, start_date_col=date_col_names[0], end_date_col=date_col_names[1]),
                axis=1
            )

        elif faire_col == 'tot_depth_water_col':
            cols = metadata_col.split(' | ')
            tot_depth_water_col = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_tot_depth_water_col_from_lat_lon(metadata_row=row, lat_col=cols[1], lon_col=cols[2], exact_map_col=cols[0]),
                axis=1
            )

            # specify where tot_Depth_water_col came from (some measured, some calculated)
            tot_depth_water_method_info = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('tot_depth_water_col_method')
            sample_metadata_results['tot_depth_water_col_method'] = sample_mapper.sample_metadata_df['ctd_Water_Depth..dbar.'].apply(
                lambda row: tot_depth_water_method_info if pd.notna(row) else None
            )

            sample_metadata_results['tot_depth_water_col'] = tot_depth_water_col
            sample_mapper.sample_metadata_df['tot_depth_water_col'] = tot_depth_water_col
            
            # Get altitude to totl_depth_water_col and maximumDepthInMeters
            sample_metadata_results['altitude'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_altitude(metadata_row=row, depth_col='final_max_depth', tot_depth_col='tot_depth_water_col'),
                axis=1
            )

        elif faire_col == 'wind_direction':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.convert_wind_degrees_to_direction)
    
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
            
        elif faire_col == 'station_id':
            station_id = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_station_id_from_unstandardized_station_name(metadata_row=row, unstandardized_station_name_col=metadata_col), 
                axis=1
            )
            sample_metadata_results[faire_col] = station_id
            sample_mapper.sample_metadata_df['station_id'] = station_id

            # Use standardized station to get stations within 5 km
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
    sample_mapper.save_final_df_as_csv(final_df=faire_sample_df_updated, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/EcoFoci/dy2206/data/dy2206_faire.csv')
   

    # # step 7: save to excel file
    # sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.faire_template_file,
    #                                           sheet_name=sample_mapper.sample_mapping_sheet_name, 
    #                                           faire_template_df=faire_sample_df_updated)

    return sample_mapper, faire_sample_df

def main() -> None:

    # create sample metadata - experiment metadata needed first
    # sample_metadata = create_dy2206_sample_metadata()

    sample_mapper = FaireSampleMetadataMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/EcoFoci/dy2206/config.yaml')
    transformer = SampleMetadataTransformer(sample_mapper=sample_mapper, ome_auto_setup=True)
    # transforer.insert_rule_before('biological_rep_relation', rule5)
    additional_rules = [
        # get_geo_loc_name_by_lat_lon_rule(sample_mapper),
        # get_env_medium_for_coastal_waters_by_geo_loc_rule(sample_mapper),
        get_material_samp_id__by_cruisecode_cast_btlnum(sample_mapper),
                        ]
    transformer.add_custom_rules(additional_rules)
    sample_metadata_df = transformer.transform()
    sample_metadata_df.to_csv("/home/poseidon/zalmanek/FAIRe-Mapping/tests/sample_mapper/dy2206_test/test.csv")
                

if __name__ == "__main__":
    main()