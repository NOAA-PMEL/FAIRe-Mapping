import sys
sys.path.append("../../..")

from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from utils.experiment_run_metadata_mapper import ExperimentRunMetadataMapper
import pandas as pd

# TODO: add related mapping to tot_depth_water_col when GDBC downloads when net cdf finishes copying


def create_sample_metadata():
    
    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='config.yaml')

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
        
        elif faire_col == 'eventDate':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.convert_date_to_iso8601
            )

        elif faire_col == 'env_local_scale':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.calculate_env_local_scale)

        elif faire_col == 'prepped_samp_store_dur':
            date_col_names = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_date_duration(metadata_row=row, start_date_col=date_col_names[0], end_date_col=date_col_names[1]),
                axis=1
            )

        elif faire_col == 'minimumDepthInMeters':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name=sample_mapper.sample_metadata_depth_col_name),
                axis=1
            )
        elif faire_col == 'wind_direction':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.convert_wind_degrees_to_direction)

        # elif faire_col == 'tot_depth_water_col':
        #     cols = metadata_col.split(' | ')
        #     sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
        #         lambda row: sample_mapper.get_tot_depth_water_col_from_lat_lon(metadata_row=row, lat_col=cols[1], lon_col=cols[2], exact_map_col=cols[0]),
        #         axis=1
        #     )
    
    # Step 4: fill in NA with missing not collected or not applicable because they are samples
    sample_df = sample_mapper.fill_empty_sample_values(df = pd.DataFrame(sample_metadata_results))
    
    # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    nc_df = sample_mapper.fill_nc_metadata(final_sample_df = sample_df)

    # Step 6: Combine all mappings at once (add nc_df if negative controls were sequenced)
    faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df, nc_df])
   
    # step 7: save to excel file
    sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.faire_template_file,
                                              sheet_name=sample_mapper.sample_mapping_sheet_name, 
                                              faire_template_df=faire_sample_df)

    return sample_mapper

def create_exp_run_metadata():

    exp_mapper = ExperimentRunMetadataMapper(config_yaml='config.yaml')
    faire_exp_df = exp_mapper.generate_jv_run_metadata()

    # save to excel
    # exp_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=exp_mapper.faire_template_file,
    #                                        sheet_name=exp_mapper.faire_template_exp_run_sheet_name, 
    #                                        faire_template_df=faire_exp_df)


    return faire_exp_df, exp_mapper

def main() -> None:

    # step 1: generate exp metadata - this will inform which sample get metadata
    # exp_metadata = create_exp_run_metadata()
    # exp_df = exp_metadata[0]
    # exp_mapper = exp_metadata[1]
    
    # Get sample dictionary of associated positives
    # samp_associated_positives = exp_mapper.rel_pos_cont_id_dict

    # # create sample metadata - experiment metadata needed first
    sample_metadata = create_sample_metadata()
                

if __name__ == "__main__":
    main()