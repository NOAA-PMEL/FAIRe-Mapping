import sys
sys.path.append("..")
# from faire_metadata_mapper.sample_metadata_mapper import FaireSampleMetadataMapper
from utils.sample_metadata_mapper import FaireSampleMetadataMapper


def main() -> None:

     # initiate mapper
    sample_mapper = FaireSampleMetadataMapper(config_yaml='sample_metadata_config.yaml')

    sample_metadata_results = {}

    # ###### Add Sample Mappings ######
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

    # print(sample_metadata_results)


if __name__ == "__main__":
    main()