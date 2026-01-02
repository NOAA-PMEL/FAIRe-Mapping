
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import pandas as pd
import numpy as np
from faire_mapping.transformers.sample_metadata_transformer import SampleMetadataTransformer
from faire_mapping.transformers.rules import (
    get_geo_loc_name_by_lat_lon_rule,
    get_env_medium_for_coastal_waters_by_geo_loc_rule,
    get_eventDate_iso8601_rule,
    get_date_duration_rule,
    get_depth_from_pressure,
    get_minimum_depth_from_max_minus_1m,
    get_altitude_from_maxdepth_and_totdepthcol,
    get_env_local_scale_by_depth,
    get_dna_yield_from_conc_and_vol
)

def create_rc0083_sample_metadata():
    
    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/AK_Carbon/rc0083/config.yaml')

    sample_metadata_results = {}

    #### Add Sample Mappings ######
    # Step 1: Add exact mappings
    for faire_col, metadata_col in sample_mapper.sample_extract_mapping_builder.sample_mapping_dict[sample_mapper.sample_extract_mapping_builder.EXACT_MAPPING].items():
        sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df_builder.sample_metadata_df[metadata_col].apply(
            lambda row: sample_mapper.apply_exact_mappings(metadata_row=row, faire_col=faire_col))
    
    # Step 2: Add constants
    for faire_col, static_value in sample_mapper.sample_extract_mapping_builder.sample_mapping_dict[sample_mapper.sample_extract_mapping_builder.CONSTANT_MAPPING].items():
        sample_metadata_results[faire_col] = sample_mapper.apply_static_mappings(faire_col=faire_col, static_value=static_value)

    # Step 3: Add related mappings
    for faire_col, metadata_col in sample_mapper.sample_extract_mapping_builder.sample_mapping_dict[sample_mapper.sample_extract_mapping_builder.RELATED_MAPPING].items():
        # Add samp_category
        if faire_col == 'samp_category' and metadata_col == sample_mapper.sample_metadata_sample_name_column:
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df_builder.sample_metadata_df.apply(
                lambda row: sample_mapper.add_samp_category_by_sample_name(metadata_row=row, faire_col=faire_col, metadata_col=metadata_col),
                axis=1
            )
    
        elif faire_col == 'biological_rep_relation':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df_builder.sample_metadata_df.apply(
                lambda row: sample_mapper.add_biological_replicates(metadata_row=row, faire_missing_val='not applicable'),
                axis=1
            )

        # env_medium depends on what geo_loc_name is (from review with Shannon and Sean: see notes.)
        # elif faire_col == 'geo_loc_name':
        #     lat_lon_cols = metadata_col.split(' | ')
        #     geo_loc_name = sample_mapper.sample_metadata_df_builder.sample_metadata_df.apply(
        #             lambda row: sample_mapper.find_geo_loc_by_lat_lon(metadata_row=row, metadata_lat_col=lat_lon_cols[1], metadata_lon_col=lat_lon_cols[0]),
        #             axis=1
        #         )
        #     sample_metadata_results['geo_loc_name'] = geo_loc_name # save to standardized results
        #     sample_mapper.sample_metadata_df_builder.sample_metadata_df['geo_loc_name'] = geo_loc_name # save to metadata_df to relate env_medium
            
        #     #update env_medium so that ones with geo_loc_name of coastal waters of Southeast Alaska have saline_water as env_medium, everything else has ocean_water
        #     env_mediums = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('env_medium').split(' | ')
        #     sample_metadata_results['env_medium'] = np.where(
        #         sample_mapper.sample_metadata_df_builder.sample_metadata_df['geo_loc_name'].str.contains('Coastal Waters of Southeast Alaska', case=False, na=False),
        #         env_mediums[1],
        #         env_mediums[0]
        #     )

        elif faire_col == 'eventDate':
            eventDate = sample_mapper.sample_metadata_df_builder.sample_metadata_df[metadata_col].apply(
                sample_mapper.convert_date_to_iso8601
            )
            sample_metadata_results[faire_col] = eventDate
            sample_mapper.sample_metadata_df_builder.sample_metadata_df['eventDate'] = eventDate

            prep_stor_date_cols = sample_mapper.sample_extract_mapping_builder.sample_mapping_dict[sample_mapper.sample_extract_mapping_builder.RELATED_MAPPING].get('prepped_samp_store_dur')
            date_col_names = prep_stor_date_cols.split(' | ')
            sample_metadata_results['prepped_samp_store_dur'] = sample_mapper.sample_metadata_df_builder.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_date_duration(metadata_row=row, start_date_col=date_col_names[0], end_date_col=date_col_names[1]),
                axis=1
            )

        elif faire_col == 'maximumDepthInMeters':
            depth_metadata_cols = metadata_col.split(' | ')
            max_depth = sample_mapper.sample_metadata_df_builder.sample_metadata_df.apply(
                lambda row: sample_mapper.get_depth_from_pressure(metadata_row=row, press_col_name=depth_metadata_cols[0], lat_col_name=depth_metadata_cols[1]),
                axis=1
            )
            sample_metadata_results[faire_col] = max_depth
            sample_mapper.sample_metadata_df_builder.sample_metadata_df[faire_col] = max_depth

            # Calculate minimumDepth
            sample_metadata_results['minimumDepthInMeters'] = sample_mapper.sample_metadata_df_builder.sample_metadata_df.apply(
                lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name=faire_col),
                axis=1
            )

            # Get altitude to totl_depth_water_col and maximumDepthInMeters
            sample_metadata_results['altitude'] = sample_mapper.sample_metadata_df_builder.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_altitude(metadata_row=row, depth_col=faire_col, tot_depth_col='btl2_Depth_bottom'),
                axis=1
            )

            # Calculate env_local_scale
            sample_metadata_results['env_local_scale'] = sample_mapper.sample_metadata_df_builder.sample_metadata_df[faire_col].apply(sample_mapper.calculate_env_local_scale)

        elif faire_col == 'date_ext':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df_builder.sample_metadata_df[metadata_col].apply(sample_mapper.convert_date_to_iso8601)

        # elif faire_col == 'extract_id':
        #     sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
        #         sample_mapper.create_extract_id
        #     )

        elif faire_col == 'dna_yield':
            metadata_cols = metadata_col.split(' | ')
            sample_vol_col = metadata_cols[1]
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df_builder.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_dna_yield(metadata_row=row, sample_vol_metadata_col=sample_vol_col),
                axis = 1
            )

        elif faire_col == 'nucl_acid_ext' or faire_col == 'nucl_acid_ext_modify':
            metadata_cols = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df_builder.sample_metadata_df.apply(
                lambda row: sample_mapper.add_constant_value_based_on_str_in_col(metadata_row=row, 
                                                                                 col_name=metadata_cols[0], 
                                                                                 str_condition='QiaVac', 
                                                                                 pos_condition_const=metadata_cols[2],
                                                                                 neg_condition_const=metadata_cols[1]),
                                                                                 axis=1)
            
        # Get line_id from standardized station name
        elif faire_col == 'line_id':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df_builder.sample_metadata_df[metadata_col].apply(sample_mapper.get_line_id)

    df = pd.DataFrame(sample_metadata_results)
    df.to_csv("/home/poseidon/zalmanek/FAIRe-Mapping/tests/sample_mapper/test.csv")
    print(df)

    # # Step 4: fill in NA with missing not collected or not applicable because they are samples and adds NC to rel_cont_id
    # sample_df = sample_mapper.fill_empty_sample_values_and_finalize_sample_df(df = pd.DataFrame(sample_metadata_results))
    
    # # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    # # nc_df = sample_mapper.fill_nc_metadata()
    # controls_df = sample_mapper.finish_up_controls_df(final_sample_df=sample_df)
    # nucl_acid_ext_map_cols = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('nucl_acid_ext').split(' | ')
    # nucl_acid_ext_modify_map_cols = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('nucl_acid_ext_modify').split(' | ')
    # controls_df['nucl_acid_ext'] = nucl_acid_ext_map_cols[1]
    # controls_df['nucl_acid_ext_modify'] = nucl_acid_ext_modify_map_cols[1]

    # # Step 6: Combine all mappings at once (add nc_df if negative controls were sequenced)
    # faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df,controls_df])
    # # Add rel_cont_id
    # faire_sample_df_updated = sample_mapper.add_extraction_blanks_to_rel_cont_id(final_sample_df=faire_sample_df)

    # # Remove ~ from samp_size that is ~500
    # faire_sample_df_updated['samp_size'] = faire_sample_df_updated['samp_size'].str.replace('~', '', regex=False)

    # # prepend cruise code to material sample id
    # faire_sample_df_updated['materialSampleID'] = 'RC0083_' + faire_sample_df_updated['materialSampleID'].astype(str).str.replace('.0', '')
    # # Don't need to update cruise code in sample name vecause already correct

    # # step 7: save as csv:
    # sample_mapper.save_final_df_as_csv(final_df=faire_sample_df_updated, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/AK_Carbon/rc0083/data/rc0083_faire.csv')
   
    # # step 8: save to excel file
    # # sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.faire_template_file,
    # #                                           sheet_name=sample_mapper.sample_mapping_sheet_name, 
    # #                                           faire_template_df=faire_sample_df_updated)

    # return sample_mapper, faire_sample_df

def main() -> None:

    # sample_metadata = create_rc0083_sample_metadata()

    sample_mapper = FaireSampleMetadataMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/AK_Carbon/rc0083/config.yaml')
    transformer = SampleMetadataTransformer(sample_mapper=sample_mapper, ome_auto_setup=True)
    # transforer.insert_rule_before('biological_rep_relation', rule5)
    additional_rules = [
        # get_geo_loc_name_by_lat_lon_rule(sample_mapper),
        # get_env_medium_for_coastal_waters_by_geo_loc_rule(sample_mapper),
        get_eventDate_iso8601_rule(sample_mapper),
        get_date_duration_rule(sample_mapper),
        get_depth_from_pressure(sample_mapper),
        get_minimum_depth_from_max_minus_1m(sample_mapper),
        get_altitude_from_maxdepth_and_totdepthcol(sample_mapper),
        get_env_local_scale_by_depth(sample_mapper),
        get_dna_yield_from_conc_and_vol(sample_mapper)
                        ]
    transformer.add_custom_rules(additional_rules)
    sample_metadata_df = transformer.transform()
    sample_metadata_df.to_csv("/home/poseidon/zalmanek/FAIRe-Mapping/tests/sample_mapper/test.csv")


    
                
if __name__ == "__main__":
    main()