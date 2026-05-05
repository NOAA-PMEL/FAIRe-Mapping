from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from faire_mapping.sample_metadata_mapper import FaireSampleMetadataMapper
from faire_mapping.transformers.rules.general_rules import get_exact_mappings_rule, get_constant_mappings_rule
from typing import List
import logging

logger = logging.getLogger(__name__)

def get_samp_category_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for samp_category
    """
    def apply_samp_category(df, faire_col, metadata_col):
        """
        Apply samp_category transformation to each row. 
        This function captures metadta_col in its closure
        """
        # Create lambda that has access to metadata_col
        return df.apply(
            lambda row: mapper.add_samp_category_by_sample_name(
            metadata_row=row,
            faire_col=faire_col,
            metadata_col=metadata_col
            ),
            axis=1
        )
    
    return(
        TransformationBuilder('samp_category')
        .when(lambda f, m, mt: (
        f == 'samp_category' and
        mt == 'related' 
        ))
        .apply(
             apply_samp_category,
            mode='direct'
        )
        .update_source(False)
        .for_mapping_type('related')
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

def get_replicte_num_from_ome_sample_name(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the replicate number from OME's sample name.
    Note: specific to OME
    Expects metadata_col to be 'sample_name_col'.
    """
    def apply_rep_num_deduction(df, faire_col, metadata_col):
        """
        Apply replicate_number deduction using the mapper's et_replicate_number_from_samp_name.
        """
        # Apply the calculation to each row
        return df[metadata_col].apply(mapper.get_replicate_number_from_samp_name)
    return (
            TransformationBuilder('replicate_number_from_ome_sampe_name')
            .when(lambda f, m, mt: (
                f == 'replicate_number' and
                mt == 'related'
            ))
            .apply(
                apply_rep_num_deduction,
                mode='direct'
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
        get_biological_rep_relation_rule(mapper),
        get_replicte_num_from_ome_sample_name(mapper)
    ]
    logger.info(f"Created {len(rules)} OME default rules")
    return rules

