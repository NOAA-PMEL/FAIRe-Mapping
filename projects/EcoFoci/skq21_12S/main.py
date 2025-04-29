import sys
sys.path.append("../../..")

from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from utils.experiment_run_metadata_mapper import ExperimentRunMetadataMapper
import pandas as pd

#TODO: Add cv checking for related mappings?
# TODO: change order of saving excel file and add csv saving at end

def create_sample_metadata(exp_metadata_df: pd.DataFrame, samp_associated_positives: dict):
    
    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='config.yaml',
                                              exp_metadata_df=exp_metadata_df)

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
                lambda row: sample_mapper.add_material_sample_id(metadata_row=row),
                axis=1
            )
        
        elif faire_col == 'rel_cont_id':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_rel_cont_id(metadata_row=row, samp_seq_pos_controls_dict=samp_associated_positives),
                axis=1
            )

        elif faire_col == 'biological_rep_relation':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_biological_replicates(metadata_row=row, faire_missing_val='not applicable'),
                axis=1
            )

        elif faire_col == 'decimalLongitude' or faire_col == 'decimalLatitude':
            date_cols = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=date_cols[0], use_if_na_col_name=date_cols[1]),
                axis=1
            )

        elif faire_col == 'geo_loc_name':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.format_geo_loc(metadata_row=row, geo_loc_metadata_col=metadata_col),
                axis=1
            )

        # eventDate needs to be proecessed before prepped_samp_store_dur
        elif faire_col == 'eventDate' or faire_col == 'prepped_samp_store_dur':
            event_dates = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('eventDate')
            date_cols = event_dates.split(' | ')
            sample_collection_date = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=date_cols[0], use_if_na_col_name=date_cols[1], transform_use_col_to_date_format=True),
                axis=1
            )
            sample_metadata_results['eventDate'] = sample_collection_date
            sample_mapper.sample_metadata_df['eventDate'] = sample_collection_date

            prepped_samp_dates = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('prepped_samp_store_dur')
            date_col_names = prepped_samp_dates.split(' | ')
            sample_metadata_results['prepped_samp_store_dur'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_date_duration(metadata_row=row, start_date_col='eventDate', end_date_col=date_col_names[1]),
                axis=1
            )

        elif faire_col == 'assay_name':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.add_assay_name)

        elif faire_col == 'env_local_scale':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.calculate_env_local_scale)

        # Need to make sure maximumDepthInMeters is processed before MinimumDepthinMeters
        elif "DepthInMeters" in faire_col:
            max_depth_faire_col = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('maximumDepthInMeters')
            depth_col_names = max_depth_faire_col.split(' | ')
            max_depth = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=depth_col_names[0], use_if_na_col_name=depth_col_names[1]),
                axis=1
            )
            sample_mapper.sample_metadata_df['FinalDepth'] = max_depth
            sample_metadata_results['maximumDepthInMeters'] = max_depth

            sample_metadata_results['minimumDepthInMeters'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name='FinalDepth'),
                axis=1
            )
    # Step 4: fill in NA with missing not collected or not applicable because they are samples
    sample_df = sample_mapper.fill_empty_sample_values(df = pd.DataFrame(sample_metadata_results))
    
    # # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    # nc_df = sample_mapper.fill_nc_metadata(final_sample_df = sample_df)

    # Step 5 Add positive controls
    pos_df = sample_mapper.fill_seq_pos_control_metadata(final_sample_df = sample_df)

    # Step 6: Combine all mappings at once (add nc_df if negative controls were sequenced)
    faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df, pos_df])
   
    # step 7: save to excel file
    sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.final_faire_template_path,
                                              sheet_name=sample_mapper.sample_mapping_sheet_name, 
                                              faire_template_df=faire_sample_df)

    return faire_sample_df, sample_mapper

def create_exp_run_metadata():

    exp_mapper = ExperimentRunMetadataMapper(config_yaml='config.yaml')
    faire_exp_df = exp_mapper.generate_jv_run_metadata()

    # save to excel
    exp_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=exp_mapper.faire_template_file,
                                           sheet_name=exp_mapper.faire_template_exp_run_sheet_name, 
                                           faire_template_df=faire_exp_df)


    return faire_exp_df, exp_mapper

def main() -> None:

    # step 1: generate exp metadata - this will inform which sample get metadata
    exp_metadata = create_exp_run_metadata()
    exp_df = exp_metadata[0]
    exp_mapper = exp_metadata[1]
    
    # Get sample dictionary of associated positives
    samp_associated_positives = exp_mapper.rel_pos_cont_id_dict

    # create sample metadata - experiment metadata needed first
    sample_metadata = create_sample_metadata(exp_metadata_df=exp_df, samp_associated_positives=samp_associated_positives)
                

if __name__ == "__main__":
    main()