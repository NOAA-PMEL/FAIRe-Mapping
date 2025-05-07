import sys
sys.path.append("../../..")

from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from utils.experiment_run_metadata_mapper import ExperimentRunMetadataMapper
import pandas as pd

# TODO: add related mapping to tot_depth_water_col when GDBC downloads when net cdf finishes copying


def create_rc0083_sample_metadata():
    
    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='config.yaml')
    print(sample_mapper.nc_df)

    # sample_metadata_results = {}

    # #### Add Sample Mappings ######
    # # Step 1: Add exact mappings
    # for faire_col, metadata_col in sample_mapper.mapping_dict[sample_mapper.exact_mapping].items():
    #     sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
    #         lambda row: sample_mapper.apply_exact_mappings(metadata_row=row, faire_col=faire_col))
    
    # # Step 2: Add constants
    # for faire_col, static_value in sample_mapper.mapping_dict[sample_mapper.constant_mapping].items():
    #     sample_metadata_results[faire_col] = sample_mapper.apply_static_mappings(faire_col=faire_col, static_value=static_value)

    # # Step 3: Add related mappings
    # for faire_col, metadata_col in sample_mapper.mapping_dict[sample_mapper.related_mapping].items():
    #     # Add samp_category
    #     if faire_col == 'samp_category' and metadata_col == sample_mapper.sample_metadata_sample_name_column:
    #         sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
    #             lambda row: sample_mapper.add_samp_category_by_sample_name(metadata_row=row, faire_col=faire_col, metadata_col=metadata_col),
    #             axis=1
    #         )

    #     elif faire_col == 'biological_rep_relation':
    #         sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
    #             lambda row: sample_mapper.add_biological_replicates(metadata_row=row, faire_missing_val='not applicable'),
    #             axis=1
    #         )

    #     elif faire_col == 'geo_loc_name':
    #         lat_lon_cols = metadata_col.split(' | ')
    #         sample_metadata_results['geo_loc_name'] = sample_mapper.sample_metadata_df.apply(
    #                 lambda row: sample_mapper.find_geo_loc_by_lat_lon(metadata_row=row, metadata_lat_col=lat_lon_cols[1], metadata_lon_col=lat_lon_cols[0]),
    #                 axis=1
    #             )
        
    #     elif faire_col == 'eventDate':
    #         sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
    #             sample_mapper.convert_date_to_iso8601
    #         )

    #     elif faire_col == 'env_local_scale':
    #         sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.calculate_env_local_scale)

    #     elif faire_col == 'prepped_samp_store_dur':
    #         date_col_names = metadata_col.split(' | ')
    #         sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
    #             lambda row: sample_mapper.calculate_date_duration(metadata_row=row, start_date_col=date_col_names[0], end_date_col=date_col_names[1]),
    #             axis=1
    #         )

    #     elif faire_col == 'minimumDepthInMeters':
    #         sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
    #             lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name=metadata_col),
    #             axis=1
    #         )
        
    #     elif faire_col == 'date_ext':
    #         sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.convert_date_to_iso8601)
    
    # df = pd.DataFrame(sample_metadata_results)
    # print(df)
    # # Step 4: fill in NA with missing not collected or not applicable because they are samples
    # sample_df = sample_mapper.fill_empty_sample_values(df = pd.DataFrame(sample_metadata_results))
    
    # # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    # nc_df = sample_mapper.fill_nc_metadata(final_sample_df = sample_df)

    # # Step 6: Combine all mappings at once (add nc_df if negative controls were sequenced)
    # faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df, nc_df])
   
    # # step 7: save to excel file
    # sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.faire_template_file,
    #                                           sheet_name=sample_mapper.sample_mapping_sheet_name, 
    #                                           faire_template_df=faire_sample_df)

    # return sample_mapper

# def create_exp_run_metadata():

#     exp_mapper = ExperimentRunMetadataMapper(config_yaml='config.yaml')
#     faire_exp_df = exp_mapper.generate_jv_run_metadata()

#     # save to excel
#     # exp_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=exp_mapper.faire_template_file,
#     #                                        sheet_name=exp_mapper.faire_template_exp_run_sheet_name, 
#     #                                        faire_template_df=faire_exp_df)


#     return faire_exp_df, exp_mapper

def main() -> None:

    # step 1: generate exp metadata - this will inform which sample get metadata
    # exp_metadata = create_exp_run_metadata()
    # exp_df = exp_metadata[0]
    # exp_mapper = exp_metadata[1]
    
    # Get sample dictionary of associated positives
    # samp_associated_positives = exp_mapper.rel_pos_cont_id_dict

    # # create sample metadata - experiment metadata needed first
    sample_metadata = create_rc0083_sample_metadata()
                

if __name__ == "__main__":
    main()