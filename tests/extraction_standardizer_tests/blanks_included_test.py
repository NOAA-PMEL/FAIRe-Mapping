from faire_mapping.dataframe_builders.extraction_metadata_builder import ExtractionMetadataBuilder
from faire_mapping.mapping_builders.sample_extract_mapping_dict_builder import SampleExtractionMappingDictBuilder

extractions_info = [
    {
        "extraction_metadata_google_sheet_id": "1KTs7LiITWi4DxmAjevH19HkWzKDY4CSkmd184dKmKxQ",
        "extraction_name": "AlaskaArctic22-23",
        "extraction_cruise_key": ".RC0083",  # Filter for the specific cruise
        "extraction_sample_name_col": "FINAL Sample NAME",
        "extraction_conc_col_name": "Qubit Conc.  (ng/μL)",  # Used for avg calculation
        "extraction_date_col_name": "Extraction Date (Local)",
        "extraction_metadata_sheet_name": "Extraction Sheets",
        "extraction_blank_vol_we_dna_ext": 1000,
        "extraction_set_grouping_col_name": "Extraction Set"
    }
]
google_sheet_json_cred = "/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json"
mapping_file_id = "1lnHUaAVj2ybyiruvBUY-vxtYpBM8xyAnsxHaC-y3W08"
sample_mapping_builder = SampleExtractionMappingDictBuilder(google_sheet_mapping_file_id=mapping_file_id, google_sheet_json_cred=google_sheet_json_cred)

extract_standardizer = ExtractionMetadataBuilder(extractions_info=extractions_info, 
                                              google_sheet_json_cred=google_sheet_json_cred,
                                              sample_extract_mapping_builder=sample_mapping_builder)

print(extract_standardizer.extraction_df)
print(extract_standardizer.extraction_blanks_df)
print(extract_standardizer.extraction_blank_rel_cont_dict)
print(extract_standardizer.sample_extract_mapping_builder.sample_mapping_dict)