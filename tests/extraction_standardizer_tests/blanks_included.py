from faire_mapping.sample_mapper.extraction_standardizer import ExtractionStandardizer

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

extract_standardizer = ExtractionStandardizer(extractions_info=extractions_info, 
                                              google_sheet_json_cred=google_sheet_json_cred)

print(extract_standardizer.extraction_df)
print(extract_standardizer.extraction_blanks_df)
print(extract_standardizer.extraction_blank_rel_cont_dict)