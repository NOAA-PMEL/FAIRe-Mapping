import sys
sys.path.append("../../..")

from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from utils.experiment_run_metadata_mapper import ExperimentRunMetadataMapper
import pandas as pd

# TODO: figure out what to do about missing samples in the sample data. Sample E1820.DY2306 - E1842.DY2306 are missing 
# (but they exist in the extraction spreadsheet)
# TODO: 5/5/2025 - look into extraction date problem for prepped_stor_dur
# TODO: geo_loc_name - write function to get geo_loc_name by lat/long
# TODO: add par as user defined field

def fix_stations(df: pd.DataFrame) -> pd.DataFrame:
    # According to email with Shaun Bell, the station names for the IC stations were incorrectly added (historically they were in a different order)
    # This function fixes station_id for these stations.

    station_id_mapping = {
        'IC11': 'IC01',
        'IC10': 'IC02',
        'IC09': 'IC03',
        'IC08': 'IC04',
        'IC07': 'IC05',
        'IC06': 'IC06',
        'IC05': 'IC07',
        'IC04': 'IC08',
        'IC03': 'IC09',
        'IC02': 'IC10',
        'IC01': 'IC11'
        }

    df['station_id'] = df['station_id'].map(station_id_mapping).where(df['station_id'].str.startswith('IC'), df['station_id'])

    # AFter solving station problems with Shannon, found errors where DBO2.09 was listed as station name for three samples, should be DBO2.0
    df.loc[df['station_id'] == 'DBO2.09', 'station_id'] = 'DBO2.0'

    return df


def create_skq23_12s_sample_metadata():
    
    # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='config.yaml')
  
    sample_metadata_results = {}

    ## Add Sample Mappings ######
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
            date_cols = metadata_col.split(' | ')
            degree_output = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=date_cols[0], use_if_na_col_name=date_cols[1]),
                axis=1
            )
            if faire_col == 'decimalLongitude':
                sample_metadata_results['decimalLongitude'] = degree_output
                sample_mapper.sample_metadata_df['decimalLongitude'] = degree_output
            elif faire_col == 'decimalLatitude':
                sample_metadata_results['decimalLatitude'] = degree_output
                sample_mapper.sample_metadata_df['decimalLatitude'] = degree_output

                # calculate depth using latitutde and pressure
                # calulate max depth from pressure and lat for the rows that include pressure
                depth_metadata_cols = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('maximumDepthInMeters').split(' | ')
            
                # Add maximumDepthInMeters to final results since taking from two columns (some pressure values have NA, so using depth_m_notes)
                final_max_depth = sample_mapper.sample_metadata_df.apply(
                    lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row, 
                                                                                                desired_col_name=depth_metadata_cols[0], 
                                                                                                use_if_na_col_name=depth_metadata_cols[1],
                                                                                                transform_use_col_to_date_format=False),
                    axis=1
                )

                sample_mapper.sample_metadata_df['final_max_depth'] = final_max_depth
                sample_metadata_results['maximumDepthInMeters'] = final_max_depth
                
                sample_metadata_results['minimumDepthInMeters'] = sample_mapper.sample_metadata_df.apply(
                    lambda row: sample_mapper.convert_min_depth_from_minus_one_meter(metadata_row=row, max_depth_col_name='final_max_depth'),
                    axis=1
                )

                sample_metadata_results['env_local_scale'] = sample_mapper.sample_metadata_df['final_max_depth'].apply(sample_mapper.calculate_env_local_scale)

                # calculate tot_depth_water_col
                sample_metadata_results['tot_depth_water_col'] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.get_tot_depth_water_col_from_lat_lon(metadata_row=row, lat_col='decimalLatitude', lon_col='decimalLongitude'),
                axis=1
            )

            
                sample_metadata_results['geo_loc_name'] = sample_mapper.sample_metadata_df.apply(
                    lambda row: sample_mapper.find_geo_loc_by_lat_lon(metadata_row=row, metadata_lat_col='decimalLatitude', metadata_lon_col='decimalLongitude'), 
                    axis = 1
                )

        # eventDate needs to be proecessed before prepped_samp_store_dur
        elif faire_col == 'eventDate' or faire_col == 'prepped_samp_store_dur':
            event_dates = sample_mapper.mapping_dict[sample_mapper.related_mapping].get('eventDate')
            date_cols = event_dates.split(' | ')
            sample_collection_date = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row, desired_col_name=date_cols[0], use_if_na_col_name=date_cols[1], transform_use_col_to_date_format=True),
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

        elif faire_col == 'samp_store_dur':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.get_samp_store_dur)
            
            # Add samp_store_loc based of samp_store_dur, so needs to come after samp_store_dur is calculated
            sample_metadata_results['samp_store_loc'] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.get_samp_store_loc_by_samp_store_dur
            )
            sample_metadata_results['samp_store_temp'] = sample_mapper.sample_metadata_df[metadata_col].apply(
                sample_mapper.get_samp_sore_temp_by_samp_store_dur
            )
        
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

        # Get the last three characters of cast no. and cast as int to remove leading zeros
        elif faire_col == 'ctd_cast_number':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(
                lambda row: int(float(str(row).replace('CTD', ''))))
            
        elif faire_col == 'nucl_acid_ext' or faire_col == 'nucl_acid_ext_modify':
            metadata_cols = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.add_constant_value_based_on_str_in_col(metadata_row=row, 
                                                                                 col_name=metadata_cols[0], 
                                                                                 str_condition='QiaVac', 
                                                                                 pos_condition_const=metadata_cols[2],
                                                                                 neg_condition_const=metadata_cols[1]),
                                                                                 axis=1)
    
    # Step 4: fill in NA with missing not collected or not applicable because they are samples and adds NC to rel_cont_id
    sample_df = sample_mapper.fill_empty_sample_values(df = pd.DataFrame(sample_metadata_results))

    sample_df_updated_stations = fix_stations(df=sample_df)
    
    # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    # nc_df = sample_mapper.fill_nc_metadata()
    controls_df = sample_mapper.finish_up_controls_df(final_sample_df=sample_df_updated_stations)

    # Step 6: Combine all mappings at once (add nc_df if negative controls were sequenced)
    faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df_updated_stations ,controls_df])
    # Add rel_cont_id
    faire_sample_df_updated = sample_mapper.add_extraction_blanks_to_rel_cont_id(final_sample_df=faire_sample_df)

    # step 7: save as csv:
    sample_mapper.save_final_df_as_csv(final_df=faire_sample_df_updated, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/EcoFoci/skq23_12S/data/skq23_12S_faire.csv')
   
    # step 7: save to excel file
    # sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.faire_template_file,
    #                                           sheet_name=sample_mapper.sample_mapping_sheet_name, 
    #                                           faire_template_df=faire_sample_df_updated)

    return sample_mapper, faire_sample_df

def main() -> None:

    sample_metadata = create_skq23_12s_sample_metadata()
                
if __name__ == "__main__":
    main()