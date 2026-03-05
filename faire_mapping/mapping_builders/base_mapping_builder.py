class BaseMappingBuilder:
    
    MAPPING_FILE_MAPPED_TYPE_COL = "mapping" # the name of mapping type column (where exact, related, constant are added)
    MAPPING_FILE_FAIRE_FIELD_COL = 'faire_field'
    MAPPING_FILE_METADATA_FIELD_NAME_COL = 'source_name_or_constant'
    EXACT_MAPPING = 'exact'
    RELATED_MAPPING = 'related'
    CONSTANT_MAPPING = 'constant'
    EXTRACT_MAPPING_SHEET_NAME = "extractionMetadata" # Name of the extraction mapping google sheet in the mapping google sheet # Used in both extraction_blank_builder and sample_builder


    def __init__(self, google_sheet_mapping_file_id: str, google_sheet_json_cred: str):
        """
        google_sheet_mapping_file_id is the identifier for the google sheet that has the mapping information
        google_sheet_json_cred is the path to the credentials.json for accessing google sheet programmatically
        """
        self.google_sheet_mapping_file_id = google_sheet_mapping_file_id
        self.google_sheet_json_cred = google_sheet_json_cred