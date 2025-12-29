import pandas as pd
import sys
import numpy as np
sys.path.append("../../..")
from utils.sample_metadata_mapper import FaireSampleMetadataMapper

# QC85 station check will throw warnings - ignore errors.

def fix_string_rosette_positions(df: pd.DataFrame):
    """
    if rosette position has a non numeric value like Bucket or Surface Underway, 
    will move to the Material Sample Id and make rosette position and ctd_bottle_number as
    not applicable
    """
    mask = df['rosette_position'].isin(['Bucket1', 'Bucket2', 'Underway System', 'Bucket3'])
    df.loc[mask, 'materialSampleID'] = 'WCOA21_' + df.loc[mask, 'rosette_position']
    df.loc[mask, 'rosette_position'] = 'not applicable'
    df.loc[mask, 'ctd_bottle_number'] = 'not applicable'

    return df

def replace_not_ctd_in_rep_chem_bottle(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace all the 'NOT CTD' intances in the rep_chem_bottle field with
    'not applicable.
    """
    df['rep_chem_bottle'] = df['rep_chem_bottle'].replace('NOT CTD', 'not applicable')
    return df

def fix_not_ctd_material_sample_id_vales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fixes the materialSampleID values for the not CTD samples to what is specified
    in email from Sean on 12/18 titled 'WCOA NOT CTD problem' 
    """
    mat_samp_updates = {
        "E869.WCOA21": "WCOA21_Bucket1",
        "E870.WCOA21": "WCOA21_Bucket2",
        "E871.WCOA21": "WCOA21_UnderwaySystem",
        "E913.WCOA21": "WCOA21_methaneOxidationRate_E913",
        "E914.WCOA21": "WCOA21_methaneOxidationRate_E914",
        "E915.WCOA21": "WCOA21_methaneOxidationRate_E915",
        "E916.WCOA21": "WCOA21_methaneOxidationRate_E916",
        "E917.WCOA21": "WCOA21_methaneOxidationRate_E917",
        "E918.WCOA21": "WCOA21_methaneOxidationRate_E918",
        "E919.WCOA21": "WCOA21_methaneOxidationRate_E919",
        "E920.WCOA21": "WCOA21_methaneOxidationRate_E920",
        "E921.WCOA21": "WCOA21_methaneOxidationRate_E921"
    }

    for samp_name, mat_samp_id in mat_samp_updates.items():
        df.loc[df['samp_name'] == samp_name, 'materialSampleID'] = mat_samp_id

    return df

def update_missing_data_for_E869_to_E971(df: pd.DataFrame) -> pd.DataFrame:
    """
    Updates the missing data for E869-E871 as specified in the email
    with Sean on 12/18 titled 'WCOA NOT CTD problem'
    """
    
    E869_updates = {
        "samp_collect_device": "sterilized 5 gallon bucket",
        "env_local_scale": "marine photic zone [ENVO:00000209]",
        "minimumDepthInMeters": 0,
        "maximumDepthInMeters": 1,
        "tot_depth_water_col": 60.46,
        "altitude": 59.46,
        "altitude_unit": 'm',
        "ph": 8,
        "methane": 3216.58,
        "methane_unit": "nmol/L"
    }

    E870_updates = {
        "samp_collect_device": "sterilized 5 gallon bucket",
        "env_local_scale": "marine photic zone [ENVO:00000209]",
        "minimumDepthInMeters": 0,
        "maximumDepthInMeters": 1,
        "tot_depth_water_col": 60.46,
        "altitude": 59.46,
        "altitude_unit": "m",
        "methane": 3216.58,
        "methane_unit": "nmol/L"
    }

    E871_updates = {
        "samp_collect_device": "underway system",
        "env_local_scale": "marine photic zone [ENVO:00000209]",
        "minimumDepthInMeters": 5.2,
        "maximumDepthInMeters": 5.2,
        "tot_depth_water_col": 60.46,
        "altitude": 55.26,
        "altitude_unit": "m",
        "temp": 16.07,
        "salinity": 33.45,
        "methane": 492.68,
        "methane_unit": "nmol/L"
    }

    E913_E921_updates = {
        "env_broad_scale":  "microcosm [ENVO_01000621]",
        "env_local_scale": "microcosm [ENVO_01000621]",
        "samp_size": 250,
        "samp_size_unit": "mL",
        "samp_store_method_additional": "Stored in a 250 mL crimped serum bottle",
        "samp_vol_we_dna_ext": 250,
        "samp_vol_we_dna_ext_unit": "mL"
    }

    update_mapping =  {"E869.WCOA21": E869_updates,
                       "E870.WCOA21": E870_updates,
                       "E871.WCOA21": E871_updates,
                       "E913.WCOA21": E913_E921_updates,
                        "E914.WCOA21": E913_E921_updates, 
                        "E915.WCOA21": E913_E921_updates,
                        "E916.WCOA21": E913_E921_updates,
                        "E917.WCOA21": E913_E921_updates,
                        "E918.WCOA21": E913_E921_updates, 
                        "E919.WCOA21": E913_E921_updates, 
                        "E920.WCOA21": E913_E921_updates, 
                        "E921.WCOA21": E913_E921_updates}

    for samp, mapping_dict in update_mapping.items():
        for col, val in mapping_dict.items():
            df.loc[df['samp_name'] == samp, col] = val

    return df

def fix_E913_to_E921_missing_vals(df: pd.DataFrame) -> pd.DataFrame:
    """
    The ctd_number is missing for these samples, but the rosette position is not. So if
    the rosette_position is missing: not collected, will fill in the value from the
    rosette_position.
    """
    df = df.reset_index(drop=True)
    df.loc[df['ctd_bottle_number'] == 'missing: not collected', 'ctd_bottle_number'] = df['rosette_position']

    return df
    
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

        elif faire_col == 'decimalLatitude' or faire_col == "decimalLongitude" or faire_col == "verbatimLongitude" or faire_col == "verbatimLatitude" :
            metadata_cols = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.map_using_two_or_three_cols_if_one_is_na_use_other(metadata_row=row,
                                                                                             desired_col_name=metadata_cols[0],
                                                                                             use_if_na_col_name=metadata_cols[1]),
                axis=1)

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
            sample_metadata_results[faire_col] = np.where(
                sample_mapper.sample_metadata_df[metadata_cols[0]] <= 95,
                 sample_mapper.sample_metadata_df[metadata_cols[0]], # use altimeter column if less than 95
                 sample_mapper.sample_metadata_df.apply(
                    lambda row: sample_mapper.calculate_altitude(metadata_row=row, depth_col=metadata_cols[1], tot_depth_col=metadata_cols[2], altitude_col=metadata_cols[0]),
                    axis=1
            ))
            
             
        elif faire_col == "altitude_method":
            metadata_cols = metadata_col.split(' | ')
            sample_metadata_results[faire_col] = np.where(
                sample_mapper.sample_metadata_df[metadata_cols[0]] <= 95,
                "not applicable", #  if altimeter columns was used directly
                 metadata_cols[1] #  method description used if over 95
            )
    

        elif faire_col == 'dna_yield':
            metadata_cols = metadata_col.split(' | ')
            sample_vol_col = metadata_cols[1]
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df.apply(
                lambda row: sample_mapper.calculate_dna_yield(metadata_row=row, sample_vol_metadata_col=sample_vol_col),
                axis = 1
            )

        elif faire_col == 'date_ext':
            sample_metadata_results[faire_col] = sample_mapper.sample_metadata_df[metadata_col].apply(sample_mapper.convert_date_to_iso8601)
            
    
    # Step 4: fill in NA with missing not collected or not applicable because they are samples and adds NC to rel_cont_id
    sample_df = sample_mapper.fill_empty_sample_values_and_finalize_sample_df(df = pd.DataFrame(sample_metadata_results))
    
    # Step 5: fill NC data frame if there is - DO THIS ONLY IF negative controls were sequenced! They were not for SKQ21
    # nc_df = sample_mapper.fill_nc_metadata()
    controls_df = sample_mapper.finish_up_controls_df(final_sample_df=sample_df)

    # Step 6: Combine all mappings at once (add nc_df if negative controls were sequenced)
    faire_sample_df = pd.concat([sample_mapper.sample_faire_template_df, sample_df,controls_df])
    # Add rel_cont_id
    faire_sample_df_updated = sample_mapper.add_extraction_blanks_to_rel_cont_id(final_sample_df=faire_sample_df)

    # Make unique fixes for wcoa
    final_faire_df = fix_string_rosette_positions(df=faire_sample_df_updated)

    # Make updates specific to WCOA
    final_final_faire_df = replace_not_ctd_in_rep_chem_bottle(df=final_faire_df)
    final_final_final_faire_df = fix_not_ctd_material_sample_id_vales(df=final_final_faire_df)
    last_df = update_missing_data_for_E869_to_E971(df=final_final_final_faire_df)
    last_last_df = fix_E913_to_E921_missing_vals(df=last_df)

    # # step 7: save as csv:
    sample_mapper.save_final_df_as_csv(final_df=last_last_df, sheet_name=sample_mapper.sample_mapping_sheet_name, header=2, csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/WCOA/wcoa21/data/wcoa21_faire.csv')
   
    # # # step 8: save to excel file
    # # sample_mapper.add_final_df_to_FAIRe_excel(excel_file_to_read_from=sample_mapper.faire_template_file,
    # #                                           sheet_name=sample_mapper.sample_mapping_sheet_name, 
    # #                                           faire_template_df=faire_sample_df_updated)

    return sample_mapper, faire_sample_df

def main() -> None:

    faire_sample_outputs = create_33R020210613_sample_metadata()


if __name__ == "__main__":
    main()

