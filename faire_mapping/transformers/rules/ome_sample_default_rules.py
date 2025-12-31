from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from faire_mapping.transformers.rules.general_rules import get_exact_mappings_rule, get_constant_mappings_rule
from typing import List
import logging

logger = logging.getLogger(__name__)

def get_samp_category_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for samp_category
    """
    return (
            TransformationBuilder('samp_category')
            .when(lambda f, m, mt: ( # mt is mapping type, f is faire col m is metadata col
                f == mapper.faire_sample_category_name_col and 
                m == mapper.sample_metadata_sample_name_column and
                mt == 'related'
            ))
            .apply(
                lambda row: mapper.add_samp_category_by_sample_name(
                    metadata_row=row, 
                    faire_col='samp_category', 
                    metadata_col=mapper.sample_metadata_sample_name_column
                ), 
                mode='row'
            )
            .for_mapping_type(mapping_type='related')
            .build()
        )

def get_biological_rep_relation_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for biological replicate relation
    """
    return (
            TransformationBuilder('biological_rep_relation')
            .when(lambda f, m, mt: (
                mt == 'related' and
                f == 'biological_rep_relation'
            ))
            .apply(
                lambda df, f, m: mapper.add_biological_replicates_column(df, f, m),
                mode = 'direct'
            )
            .for_mapping_type('related')
            .build()
        )

def get_all_ome_default_rules(mapper: FaireSampleMetadataMapper) -> List:
    """
    Get all OME default transformation rules in execution order
    """
    rules = [
        get_exact_mappings_rule(mapper),
        get_constant_mappings_rule(mapper),
        get_samp_category_rule(mapper),
        get_biological_rep_relation_rule(mapper)
    ]
    logger.info(f"Created {len(rules)} OME default rules")
    return rules
