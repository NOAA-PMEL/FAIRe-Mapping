from faire_mapping.transformers.transformation_pipeline import TransformationPipeline, TransformationBuilder
from faire_mapping.transformers.rules import get_all_ome_default_rules
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from typing import Dict, List
import pandas as pd
import logging

# Use pipeline.register_rule(rule1).register_rule(rule2).register_rule(rule3) to customize order
# register_rules is added in teh sample_metadata_transformer _setup rules to add them all at once. Can just run sample_metadata_df = transformer.transform() if want to just run all the rules there

logger = logging.getLogger(__name__)

class SampleMetadataTransformer:
    """
    Transformer for sample metadata using the transformation pipeline
    """
    def __init__(self, sample_mapper: FaireSampleMetadataMapper, ome_auto_setup: bool = True):
        """
        Initialize the transformer
        """
        self.mapper = sample_mapper
        self.pipeline = TransformationPipeline(
            source_df=sample_mapper.sample_metadata_df_builder.sample_metadata_df,
            mapper=sample_mapper
        )

        if ome_auto_setup:
            self._ome_setup_default_rules()

    def _ome_setup_default_rules(self):
        """ 
        Setup OME defualt transformation rules.
        """
        rules=get_all_ome_default_rules(self.mapper)
        self.pipeline.register_rules(rules)
        logger.info(f"Registered {len(rules)} OME default transformation rules")

    def add_custom_rule(self, rules: List[TransformationBuilder]) -> 'SampleMetadataTransformer':
        """
        Add a custom rule to the pipeline

        Args:
            rule: A TransformationBuilder instance

        Returns:
            Self for method chaining
        """
        for rule in rules:
            self.pipeline.register_rule(rule.build())
        logger.info(f"Added {len(rules)} custom rules")
        return self
    
    def insert_rule_after(self, after_rule_name: str, new_rule: TransformationBuilder) -> 'SampleMetadataTransformer':
        """
        Insert a ruls after a specific existing rule
        """
        rule_names = self.pipeline.get_rule_names()

        if after_rule_name not in rule_names:
            raise ValueError(f"Rule '{after_rule_name}' not found in pipeline")
        
        index = rule_names.index(after_rule_name)
        self.pipeline.rules.insert(index + 1, new_rule.build())
        logger.info(f"Inserted rule after '{after_rule_name}")
        return self
    
    def insert_rule_before(self, before_rule_name: str, new_rule: TransformationBuilder) -> 'SampleMetadataTransformer':
        """
        Insert a rule before a specific existing rule
        """
        rule_names = self.pipeline.get_rule_names()

        if before_rule_name not in rule_names:
            raise ValueError(f"Rule '{before_rule_name}' not found in pipeline")
        
        index = rule_names.index(before_rule_name)
        self.pipeline.rules.insert(index + 1, new_rule.build())
        logger.info(f"Inserted rule after '{before_rule_name}")
        return self
    
    def transform(self) -> pd.DataFrame:
        """
        Execute all transformations 
        
        Returns:
            DataFrame with transformed results
        """
        logger.info("Starting sample metadata transformation")

        # Get the related mapping dictionary
        mapping = self.mapper.sample_extract_mapping_builder.sample_mapping_dict

        # Execute the pipeline
        self.pipeline.execute(mapping)

        # Get results as DataFrame
        results_df = self.pipeline.get_results_df()

        logger.info(f"Transformation complete. Result shape: {results_df.shape}")

        return results_df
    
    def get_results_dict(self) -> Dict[str, pd.Series]:
        """
        Get results as a dictionary
        """
        return self.pipeline.results
    
    def reset(self) -> 'SampleMetadataTransformer':
        """
        Reset the pipeline results
        """
        self.pipeline.clear_results()
        return self
        