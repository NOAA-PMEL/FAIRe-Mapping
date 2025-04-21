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