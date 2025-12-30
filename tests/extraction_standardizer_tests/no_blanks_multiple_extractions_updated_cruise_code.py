from faire_mapping.dataframe_builders.extraction_builder import ExtractionBuilder

extractions_info = [
        {
            "extraction_name": "SKQ21",
            "extraction_metadata_google_sheet_id": "16Zg1MbMupnkXzuypTg4aF37OMyytPd-ZRqfz9QNX2oI",
            "extraction_cruise_key": ".SKQ21-15S",  # filter for specific cruise
            "extraction_sample_name_col": "FINAL Sample NAME",
            "extraction_conc_col_name": "Qubit Conc. (ng/μL)",
            "extraction_date_col_name": "Extraction Date (Local)",
            "extraction_metadata_sheet_name": "Sheet1",
            "extraction_blank_vol_we_dna_ext": 1000,
            "extraction_set_grouping_col_name": "Extraction Set"
        },
        {
            "extraction_name": "WCOA21",
            "extraction_metadata_google_sheet_id": "1aXN_HmGRDgDLvkz1Dbkje6R7rUUg9mNNc630YN8HvFE",
            "extraction_cruise_key": ".SKQ21",
            "extraction_sample_name_col": "FINAL Sample NAME",
            "extraction_conc_col_name": "Qubit Conc.  (ng/μL)",
            "extraction_date_col_name": "Extraction Date (Local)",
            "extraction_metadata_sheet_name": "Sheet1",
            "extraction_blank_vol_we_dna_ext": 1000,
            "extraction_set_grouping_col_name": "Extraction Set"
        }
    ]
google_sheet_json_cred = "/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json"
unwanted_cruise_code = ".SKQ2021"
desired_cruise_code = ".SKQ21-15S"

extract_standardizer = ExtractionBuilder(extractions_info=extractions_info, 
                                              google_sheet_json_cred=google_sheet_json_cred,
                                              unwanted_cruise_code=unwanted_cruise_code,
                                              desired_cruise_code=desired_cruise_code)

print(extract_standardizer.extraction_df)
print(extract_standardizer.extraction_blanks_df)
print(extract_standardizer.extraction_blank_rel_cont_dict)