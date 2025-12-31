from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from typing import List
import logging

def get_exact_mappings_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for exact column mappings
    """
    return (
        TransformationBuilder('exact_mapping')
        .when(lambda f, m, mt: mt == 'exact')
        .apply(
            lambda df, f, mt: mapper.apply_exact_mappings(df, f, mt),
            mode='direct'
        )
        .for_mapping_type('exact')
        .build())

def get_constant_mappings_rule(mapper: FaireSampleMetadataMapper):
    """
    Rule for constant/static mappings
    """
    # Rule 2: constant_mappings
    return (
        TransformationBuilder('constant_mapping')
        .when(lambda f, m, mt: mt == 'constant')
        .apply(
            lambda df, f, mt: mapper.apply_static_mappings(df, f, mt),
            mode='direct'
        )
        .for_mapping_type('constant')
        .build()
    )