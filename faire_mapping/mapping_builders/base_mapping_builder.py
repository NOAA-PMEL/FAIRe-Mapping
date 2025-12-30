class BaseMappingBuilder:
    
    MAPPING_FILE_MAPPED_TYPE_COL = "mapping" # the name of mapping type column (where exact, related, constant are added)
    MAPPING_FILE_FAIRE_FIELD_COL = 'faire_field'
    MAPPING_FILE_METADATA_FIELD_NAME_COL = 'source_name_or_constant'
    EXACT_MAPPING = 'exact'
    RELATED_MAPPING = 'related'
    CONSTANT_MAPPING = 'constant'