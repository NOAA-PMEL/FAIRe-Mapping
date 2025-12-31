from faire_mapping.transformers.transformation_pipeline import TransformationPipeline, TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import pandas as pd
import logging

logger = logging.getLooger(__name__)

class SampleMetadataTransformer:
    """
    Transformer for sample metadata using the transformation pipeline
    """
    def __init__(self, sample_mapper: FaireSampleMetadataMapper):
        """
        Initialize the transformer
        """
        self.mapper = sample_mapper
        self.pipeline = TransformationPipeline(
            source_df=sample_mapper.sample_metadata_df_builder.sample_metadata_df,
            mapper=sample_mapper
        )
        self._setup_rules()