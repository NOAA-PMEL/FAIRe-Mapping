from faire_mapping.faire_mapper import OmeFaireMapper
from faire_mapping.mapping_builders.sample_extract_mapping_dict_builder import SampleExtractionMappingDictBuilder
from faire_mapping.dataframe_and_dict_builders.base_df_builder import BaseDfBuilder

def main() -> None:

    metadata_df_builder = BaseDfBuilder(csv_path='/home/poseidon/zalmanek/FAIRe-Mapping/projects/WCOA/wcoa21_net_tow/FinalOMEMerge_WCOA_netTow.csv')
   
    # Get mapping dict
    mapper_dict_builder = SampleExtractionMappingDictBuilder(google_sheet_mapping_file_id='1P5OlbztMtBQyURpVeRmA5LQKsMtbkcyF5zI1eWP7DOY',
                                                            google_sheet_json_cred='/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json')
    
    # Just going to use FAIRE-Mapper since extraction is included in metadata and will manually do the few related mappings
    faire_mapper = OmeFaireMapper(config_yaml='/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/config.yaml')

    # remove empty rows
    metadata_df_builder.df.dropna(how='all', inplace=True)
    
    sample_faire_metadata_results = {}
    
    # Exact Mappings
    for faire_col, metadata_col in mapper_dict_builder.sample_mapping_dict[faire_mapper.exact_mapping].items():
       sample_faire_metadata_results[faire_col] = faire_mapper.apply_exact_mappings(df=metadata_df_builder.df, faire_col=faire_col, metadata_col=metadata_col)

    for faire_col, metadata_col in mapper_dict_builder.sample_mapping_dict[faire_mapper.constant_mapping].items():
        sample_faire_metadata_results[faire_col] = faire_mapper.apply_static_mappings(df=metadata_df_builder.df, faire_col=faire_col, static_value=metadata_col)

    # --- RELATED MAPPINGS ----
    for faire_col, metadata_col in mapper_dict_builder.sample_mapping_dict[faire_mapper.related_mapping].items():
        
        # samp_category by samp name
        if faire_col == 'biological_rep_relation' and metadata_col:
            sample_faire_metadata_results[faire_col] = add_biological_replicates_column(df=metadata_df_builder.df)

        elif faire_col == "geo_loc_name":
            # The axis=1 belongs to .apply(), not the helper function
            sample_faire_metadata_results[faire_col] = metadata_df_builder.df.apply(
                lambda row: find_geo_loc_by_lat_lon(metadata_row=row, metadata_cols=metadata_col), 
                axis=1
            )

        elif faire_col == "ctd_cast_number":
             # Create the new column
            sample_faire_metadata_results[faire_col] = metadata_df_builder.df[faire_col].apply(extract_cast_number)       

    faire_sample_df = pd.DataFrame(sample_faire_metadata_results)
    rel_cont_id_df, blanks_to_be_added, extraction_df, blank_dict = figure_out_rel_cont_ids(faire_mapper=faire_mapper, metadata_df=faire_sample_df)
    faire_with_blanks = add_blanks_to_metadata(blanks_to_add=blanks_to_be_added, concat_extract_df=extraction_df, faire_df=rel_cont_id_df, blank_dict=blank_dict)

    # Remove positive control that was added in
    faire_with_blanks.drop(faire_with_blanks[faire_with_blanks['samp_name'] == 'E2180.OC0919'].index, inplace=True)
    faire_with_blanks['samp_type'] = 'water'

    # Add in pos_cont_type for positive controls that were included
    pos_cont_type = "ZymoBIOMICS Microbial Community Standard D6300; synthetic microbial community spike in"
    faire_with_blanks['pos_cont_type'] = faire_with_blanks.apply(lambda row: pos_cont_type if row['samp_category'] == 'positive control' else '', axis=1)

    # Fix hydrogen_ion and methane range values
    target_samples = ['H2ox_bag4', 'H2ox_bag2', 'H2ox_bag7', 'H2ox_bag11', 'H2ox_bag19']
    faire_with_blanks['verbatim_hydrogen_ion'] = 'not applicable'
    faire_with_blanks['verbatim_methane'] = 'not applicable'
    mask = faire_with_blanks['samp_name'].isin(target_samples)
    faire_with_blanks.loc[mask, 'verbatim_hydrogen_ion'] = faire_with_blanks.loc[mask, 'hydrogen_ion']
    faire_with_blanks.loc[mask, 'verbatim_methane'] = faire_with_blanks.loc[mask, 'methane']
    float_regex = r'(\d+\.\d+|\d+)'
    faire_with_blanks.loc[mask, 'hydrogen_ion'] = faire_with_blanks.loc[mask, 'hydrogen_ion'].str.extract(float_regex)[0]
    faire_with_blanks.loc[mask, 'methane'] = faire_with_blanks.loc[mask, 'methane'].str.extract(float_regex)[0]


    faire_mapper.save_final_df_as_csv(final_df=faire_with_blanks, sheet_name="sampleMetadata", header=2, csv_path="/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/data/orphan_faire.csv")
                         
if __name__ == "__main__":
    main()


