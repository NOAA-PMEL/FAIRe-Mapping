import sys
sys.path.append("..")
# from faire_metadata_mapper.sample_metadata_mapper import FaireSampleMetadataMapper
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import pandas as pd


def main() -> None:

     # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='sample_metadata_config.yaml')

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

        elif faire_col == 'decimalLongitude' or faire_col == 'decimalLatitude':
            coord_metadata_cols = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=coord_metadata_cols[0], use_if_na_col_name=coord_metadata_cols[1]),
                axis=1
            )

        elif faire_col == 'geo_loc_name':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.format_geo_loc(metadata_row=row, geo_loc_metadata_col=metadata_col),
                axis=1
            )

        elif faire_col == 'eventDate':
            coord_metadata_cols = metadata_col.split(' | ')
            sample_collection_date = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=coord_metadata_cols[0], use_if_na_col_name=coord_metadata_cols[1], transform_use_col_to_date_format=True),
                axis=1
            )
            # add the date to the faire metadata and save in metadata_df to use for the prepped_sample_dr
            sample_metadata_results[faire_col] = sample_collection_date
            sample_mapper.sample_metadata_df['eventDate'] = sample_collection_date

        # A bit different than others because the start date = eventDate in faire, which was taken from two columns for SKQ21
        elif faire_col == 'prepped_samp_store_dur':
            date_col_names = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_date_duration(metadata_row=row, start_date_col='eventDate', end_date_col=date_col_names[1]),
                axis=1
            )

        elif faire_col == 'env_local_scale':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.calculate_env_local_scale)

        # Depth uses two colums, save in one col called Depth in metadata_df
        elif faire_col == 'maximumDepthInMeters':
            depth_col_names = metadata_col.split(' | ')
            max_depth = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=depth_col_names[0], use_if_na_col_name=depth_col_names[1]),
                axis=1
            )
            sample_metadata_results[faire_col] = max_depth
            sample_mapper.sample_metadata_df['Depth'] = max_depth
            print(sample_mapper.sample_metadata_df.columns)
        # first need to get the mix of the two depth cols, since some data is missing from the desired depth col
        # then use that col to convert_min_depth_from_minus_one_meter
        # elif faire_col == 'minimumDepthInMeters':
        #     sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
        #         lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name='Depth'),
        #         axis=1
        #     )

    

    # sample_df = pd.DataFrame(sample_metadata_results)
    # print(sample_df[['samp_name', 'minimumDepthInMeters', 'maximumDepthInMeters']])

    # # Step 4: fill in NA with missing not collected or not applicable because they are samples
    # sample_df = sample_mapper.fill_empty_sample_values(df = pd.DataFrame(sample_metadata_results))

    # # Step 5: fill NC data frame
    # nc_df = sample_mapper.fill_nc_metadata()

    # # Combine all mappings at once 
    # faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df, nc_df])
   
    # # # sample_mapper.sample_faire_template_df = sample_mapper.sample_faire_template_df.assign(**all_results)
    # sample_mapper.add_final_df_to_FAIRe_excel(sheet_name=sample_mapper.sample_mapping_sheet_name, faire_template_df=faire_sample_df)


if __name__ == "__main__":
    main()