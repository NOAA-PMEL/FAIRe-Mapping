from faire_mapping.transformers.transformation_pipeline import TransformationPipeline, TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
from typing import Dict
import pandas as pd
import logging

# Use pipeline.register_rule(rule1).register_rule(rule2).register_rule(rule3) to customize order
# register_rules is added in teh sample_metadata_transformer _setup rules to add them all at once. Can just run sample_metadata_df = transformer.transform() if want to just run all the rules there

logger = logging.getLogger(__name__)

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

    def _setup_rules(self):
        """ 
        Setup transformation rules specific to sample metadata.
        """
        # Rule 1: exact mappings
        rule_exact_mappings = (
            TransformationBuilder('exact_mapping')
            .when(lambda f, m, mt: mt == 'exact')
            .apply(
                lambda df, f, mt: self.mapper.apply_exact_mappings(df, f, mt),
                mode='direct'
            )
            .for_mapping_type('exact')
            .build())
        
        # Rule 2: constant_mappings
        rule_constant_mappings = (
            TransformationBuilder('constant_mapping')
            .when(lambda f, m, mt: mt == 'constant')
            .apply(
                lambda df, f, mt: self.mapper.apply_static_mappings(df, f, mt),
                mode='direct'
            )
            .for_mapping_type('constant')
            .build()
        )

        # Rule 3: Sample category (OME specific based on sample naming rules)
        rule_samp_category = (
            TransformationBuilder('samp_category')
            .when(lambda f, m, mt: ( # mt is mapping type, f is faire col m is metadata col
                f == self.mapper.faire_sample_category_name_col and 
                m == self.mapper.sample_metadata_sample_name_column and
                mt == 'related'
            ))
            .apply(
                lambda row: self.mapper.add_samp_category_by_sample_name(
                    metadata_row=row, 
                    faire_col='samp_category', 
                    metadata_col=self.mapper.sample_metadata_sample_name_column
                ), 
                mode='row'
            )
            .for_mapping_type(mapping_type='related')
            .build()
        )

        # Rule 4: biological_rep_relation (OME specific based on sample names)
        rule_bio_rep_relation = (
            TransformationBuilder('biological_rep_relation')
            .when(lambda f, m, mt: (
                mt == 'related' and
                f == 'biological_rep_relation'
            ))
            .apply(
                lambda df, f, m: self.mapper.add_biological_replicates_column(df, f, m),
                mode = 'direct'
            )
            .for_mapping_type('related')
            .build()
        )

        # Register all rules
        self.pipeline.register_rules([
            rule_exact_mappings,
            rule_constant_mappings,
            rule_samp_category,
            rule_bio_rep_relation
        ])

        logger.info(f"Registered {len(self.pipeline.rules)} transformation rules")

    def add_custom_rule(self, rule: TransformationBuilder) -> 'SampleMetadataTransformer':
        """
        Add a custom rule to the pipeline

        Args:
            rule: A TransformationBuilder instance

        Returns:
            Self for method chaining
        """
        self.pipeline.register_rule(rule.build())
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
        