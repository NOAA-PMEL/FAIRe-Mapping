import pandas as pd
import sys
import numpy as np
sys.path.append("../../..")
from utils.sample_metadata_mapper import FaireSampleMetadataMapper

# QC85 station check will throw warnings - ignore errors.

def create_33R020210613_sample_metadata():

    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='config.yaml')

    sample_metadata_results = {}

    ###### Add Sample Mappings ######
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

        elif faire_col == "materialSampleID":
            sample_metadata_results[faire_col] = 'WCOA21_' + sample_mapper.sample_metadata_df[metadata_col]
        
        elif faire_col == 'biological_rep_relation':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_biological_replicates(metadata_row=row, faire_missing_val='not applicable'),
                axis=1
            )

        elif faire_col == 'geo_loc_name':
            lat = metadata_col.split(' | ')[0]
            lon = metadata_col.split(' | ')[1]
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.find_geo_loc_by_lat_lon(metadata_row=row, metadata_lat_col=lat, metadata_lon_col=lon), 
                    axis = 1
                )
            
        elif faire_col == 'eventDate':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.convert_date_to_iso8601
            )

        # Just get the time part of the date and time
        elif faire_col == 'verbatimEventTime':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].str.split(' ').str.get(1)

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
                lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name=metadata_col),
                axis = 1
            )

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

        elif faire_col == 'altitude':
            metadata_cols = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                    lambda row: sample_mapper.calculate_altitude(metadata_row=row, depth_col=metadata_cols[1], tot_depth_col=metadata_cols[2], altitude_col=metadata_cols[0]),
                    axis=1
                )
            
        elif faire_col == "altitude_method":
            metadata_cols = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = pd.Categorical(
                np.where(sample_mapper.sample_metadata_df[metadata_cols[0]] > 95, metadata_cols[1], pd.NA),
                categories=[metadata_cols[1], "missing: not collected"]
            )

        elif faire_col == 'dna_yield':
            metadata_cols = metadata_col.split(' | ')
            sample_vol_col = metadata_cols[1]
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_dna_yield(metadata_row=row, sample_vol_metadata_col=sample_vol_col),
                axis = 1
            )
            
    
    # Step 4: fill in NA with missing not collected or not applicable because they are samples and adds NC to rel_cont_id
    sample_df = sample_mapper.fill_empty_sample_values_and_finalize_sample_df(df = pd.DataFrame(sample_metadata_results))
    
    # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    # nc_df = sample_mapper.fill_nc_metadata()
    controls_df = sample_mapper.finish_up_controls_df(final_sample_df=sample_df)

    # Step 6: Combine all mappings at once (add nc_df if negative controls were sequenced)
    faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df,controls_df])
    # Add rel_cont_id
    faire_sample_df_updated = sample_mapper.add_extraction_blanks_to_rel_cont_id(final_sample_df=faire_sample_df)

    print(faire_sample_df_updated)
    # # step 7: save as csv:
    sample_mapper.save_final_df_as_csv(final_df=faire_sample_df_updated, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/WCOA/wcoa21/data/wcoa21_faire.csv')
   
    # # # step 8: save to excel file
    # # sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.faire_template_file,
    # #                                           sheet_name=sample_mapper.sample_mapping_sheet_name, 
    # #                                           faire_template_df=faire_sample_df_updated)

    return sample_mapper, faire_sample_df

def main() -> None:

    faire_sample_outputs = create_33R020210613_sample_metadata()


if __name__ == "__main__":
    main()

