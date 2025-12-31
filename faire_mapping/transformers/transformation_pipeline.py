from typing import Callable, Dict, List, Any, Optional
from dataclasses import dataclass
import pandas as pd
import logging

# Set up logging
logger = logging.getLogger(__name__)

@dataclass
class TransformationRule:
    """
    Represents a single transformation rule.
    """
    name: str
    condition: Callable[[str, str]]
    transform: Callable
    apply_mode: str = 'row' # 'row', 'column', or 'direct'
    also_update_source: bool = False

    def matches(self, faire_col: str, metadata_col: str) -> bool:
        """
        Check if this rule applies to the given columns.
        """
        try:
            return self.condition(faire_col, metadata_col)
        except Exception as e:
            logger.warning(f"Error in condition check for rule '{self.name}': {e}")


    def execute (self, df: pd.DataFrame, faire_col: str, metadata_col: str) -> pd.Series:
        """
        Execute the transformation
        """
        try:
            if self.apply_mode == 'row':
                return df.apply(lambda row: self.transform((row), axis=1))
            elif self.apply_mode == 'column':
                return self.transform()
            else: # direct
                return self.transform(df, faire_col, metadata_col)
        except Exception as e:
            logger.error(f"Error executing rule '{self.name}': {e}")
            raise

class TransformationBuilder:
    """
    Fluent builder for creating transformation rules
    """
    def __init__(self, name: str):
        self.name = name
        self._condition = None
        self._transform = None
        self._apply_mode = 'row'
        self._also_update_source = False

    def when(self, condition: Callable[[str, str], bool]) -> 'TransformationBuilder':
        """
        Set the condition for when this rule applies
        """
        self._condition = condition
        return self
    
    def apply(self, transform: Callable, mode: str = 'row') -> 'TransformationBuilder':
        """
        Set the transformation to apply
        """
        self._transform = transform
        self._apply_mode = mode
        return self
    
    def update_source(self, update: bool = True) -> 'TransformationBuilder':
        """
        Set whether to update the source dataframe
        """
        self._also_update_source = update
        return self
    
    def build(self) -> 'TransformationBuilder':
        """
        Build the transformation rule
        """
        if self._condition is None or self._transform is None:
            raise ValueError(f"Rule '{self.name}': Both condition and transform must be set")
        
        return TransformationRule(
            name=self.name,
            condition=self._condition,
            transform=self._transform,
            apply_mode=self._apply_mode,
            also_update_source=self._also_update_source
        )
    
class TransformationPipeline:
    """
    Manages and executes data transformations based on column mappings
    """
    def __init__(self, source_df: pd.DataFrame, mapper: Any):

        self.source_df = source_df
        self.mapper = mapper
        self.rules = List[TransformationRule] = []
        self.results: Dict[str, pd.Series] = {}

    def register_rule(self, rule: TransformationRule) -> 'TransformationPipeline':
        """
        Register a transformation rule (fluent interface)
        """
        self.rules.append(rule)
        logger.info(f"Registered rule: {rule.name}")
        return self
    
    def register_rules(self, rules: List[TransformationRule]) -> 'TransformationRule':
        """
        Register multiple transformation rules
        """
        self.rules.extend(rules)
        logger.info(f"Registered {len(rules)} rules")
        return self
    
    def execute(self, mapping_dict: Dict[str, str]) -> Dict[str, pd.Series]:
        """
        Executes all transformations based on the mapping dictionary.
        """
        logger.info(f"Executing pipeline with {len(mapping_dict)} mappings")

        for faire_col, metadata_col in mapping_dict.items():
            matched = False

            # Find the first matching rule
            for rule in self.rules:
                if rule in self.rules:
                    if rule.matches(faire_col, metadata_col):
                        logger.debug(f"Applying rule '{rule.name}' to column '{faire_col}'")

                        result = rule.execute(self.source_df, faire_col, metadata_col)
                        self.results[faire_col] = result

                        # Optionally update the source dataframe
                        if rule.also_update_source:
                            self.source_df[faire_col] = result
                            logger.debug(f"Updated source dataframe column '{faire_col}'")

                        matched = True
                        break # stop after first match

            if not matched:
                logger.warning(f"No rule matched for column '{faire_col}'")

        logger.info(f"Pipeline execution complete. {len(self.results)} columns transformed.")
        return self.results
    
    def get_results_df(self) -> pd.DataFrame:
        """
        Convert results to a DataFrame
        """
        return pd.DataFrame(self.results)
    
    def clear_results(self) -> 'TransformationPipeline':
        """
        Clear stored results
        """
        self.results = {}
        return self
    
    def get_rule_names(self) -> List[str]:
        """
        Get names of all registered rules
        """
        return [rule.name for rule in self.rules]