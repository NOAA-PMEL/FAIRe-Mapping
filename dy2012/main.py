import yaml
import pandas as pd
import sys
sys.path.append("..")
# from faire_metadata_mapper.sample_metadata_mapper import FaireSampleMetadataMapper
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from utils.experiment_run_metadata_mapper import ExperimentRunMetadataMapper


# TODO: add functionality in sample_metadata_mapper to check if sample names exist in experiment_run_metdata, and
# if they don't then, drop the rows from final spreadsheet. Also need to capture list of assays if samples do exist in
# experiment_run_metadata and too sample_metadata spreadsheet
# TODO: add rel_cont_id
# TODO: Add env_local_scale logic
# TODO: need to add not applicable: control sample to user defined metadata fields for sample_metadata?
 
def main() -> None:

    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='sample_metadata_config.yaml')

    all_results = {}

    ###### Add Sample Mappings ######
    # Step 1: Add exact mappings
    for faire_col, metadata_col in sample_mapper.mapping_dict[sample_mapper.exact_mapping].items():
        all_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
            lambda row: sample_mapper.apply_exact_mappings(metadata_row=row, faire_col=faire_col))
    
    # Step 2: Add constants
    for faire_col, static_value in sample_mapper.mapping_dict[sample_mapper.constant_mapping].items():
        all_results[faire_col] = sample_mapper.apply_static_mappings(faire_col=faire_col, static_value=static_value)

    # Step 3: Add related mappings
    for faire_col, metadata_col in sample_mapper.mapping_dict[sample_mapper.related_mapping].items():
        # Add samp_category
        if faire_col == 'samp_category' and metadata_col == sample_mapper.sample_metadata_sample_name_column:
            all_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_samp_category_by_sample_name(metadata_row=row, faire_col=faire_col, metadata_col=metadata_col),
                axis=1
            )
        elif faire_col == 'biological_rep_relation':
            all_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_biological_replicates(metadata_row=row, faire_missing_val='not applicable'),
                axis=1
            )
        elif faire_col == 'materialSampleID' or faire_col == 'sample_derived_from':
            all_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_material_sample_id(metadata_row=row),
                axis=1
            )
        elif faire_col == 'minimumDepthInMeters':
            all_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name=sample_mapper.sample_metadata_depth_col_name),
                axis=1
            )
        elif faire_col == 'wind_direction':
            all_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.convert_wind_degrees_to_direction)
        
        elif faire_col == 'geo_loc_name':
            all_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.format_geo_loc(metadata_row=row, geo_loc_metadata_col=metadata_col),
                axis=1
            )
        elif faire_col == 'env_local_scale':
            all_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.calculate_env_local_scale)

        elif faire_col == 'prepped_samp_store_dur':
            date_col_names = metadata_col.split(' | ')
            all_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_date_duration(metadata_row=row, start_date_col=date_col_names[0], end_date_col=date_col_names[1]),
                axis=1
            )
            
    # Step 4: fill in NA with missing not collected or not applicable because they are samples
    sample_df = sample_mapper.fill_empty_sample_values(df = pd.DataFrame(all_results))

    # Step 5: fill NC data frame
    nc_df = sample_mapper.fill_nc_metadata()

    # Combine all mappings at once 
    faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df, nc_df])
   
    # # sample_mapper.sample_faire_template_df = sample_mapper.sample_faire_template_df.assign(**all_results)
    sample_mapper.add_final_df_to_FAIRe_excel(sheet_name=sample_mapper.sample_mapping_sheet_name, faire_template_df=faire_sample_df)
  
# ####################### JV Experiment Run3 ##########################################################
    
#     run3_mapper = ExperimentRunMetadataMapper(config_yaml='experiment_run_config.yaml',
#                                               faire_sample_metadata_df=faire_sample_df)
    
#     experiment_results = {}

#     # Step 1: Add exact mappings
#     for faire_col, metadata_col in run3_mapper.mapping_dict[run3_mapper.exact_mapping].items():
#         experiment_results[faire_col] = run3_mapper.jv_run_metadata_df[metadata_col].apply(
#             lambda row: run3_mapper.apply_exact_mappings(metadata_row=row, faire_col=faire_col))
        
#     # Step 2: Add constant maapings
#     # Step 2: Add constants
#     for faire_col, static_value in run3_mapper.mapping_dict[run3_mapper.constant_mapping].items():
#         experiment_results[faire_col] = run3_mapper.apply_static_mappings(faire_col=faire_col, static_value=static_value)
        
#     # Step 3: Add related mappings
#     for faire_col, metadata_col in run3_mapper.mapping_dict[run3_mapper.related_mapping].items():
#         # Add assay
#         if faire_col == 'assay_name':
#             experiment_results[faire_col] = run3_mapper.jv_run_metadata_df[metadata_col].apply(run3_mapper.convert_assay_to_standard)

#         # Add lib_id
#         if faire_col == 'lib_id':
#             experiment_results[faire_col] = run3_mapper.jv_run_metadata_df.apply(
#                 lambda row: run3_mapper.jv_create_seq_id(metadata_row=row),
#                 axis=1
#             )

        
#     run3_df = pd.DataFrame(experiment_results)
#     print(run3_df)
   

   



if __name__ == "__main__":
    main()

