from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_neg_cont_type_from_ome_sample_name(mapper: FaireSampleMetadataMapper):
    """
    Rule for getting the neg_cont_type from OME's sample name.
    Note: specific to OME
    Expects metadata_col to be 'sample_name_col'.
    """
    def apply_neg_cont_type_deduction(df, faire_col, metadata_col):
        """
        Apply neg_cont_type deduction using the mapper's add_neg_cont_type method.
        """
        # Apply the calculation to each row
        return df[metadata_col].apply(mapper.add_neg_cont_type)
    return (
            TransformationBuilder('neg_cont_type_from_ome_sampe_name')
            .when(lambda f, m, mt: (
                f == 'neg_cont_type' and
                mt == 'related'
            ))
            .apply(
                apply_neg_cont_type_deduction,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )
