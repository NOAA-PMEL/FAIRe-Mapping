from faire_mapping.transformers.transformation_pipeline import TransformationBuilder
from utils.sample_metadata_mapper import FaireSampleMetadataMapper
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def get_nucl_acid_ext_and_nucl_acid_ext_modify_by_word_in_extract_col(mapper: FaireSampleMetadataMapper):
    """
    Rule for deducing the nucl_acid_ext and nucl_acid_ext_modify based on a word in a specified column
    Expects metadata_col to be 'column_name | word | neg_condition | pos_condition'
    Where neg_condition is that value if the word is absent, pos_condition is the value if the word is present
    """
    def apply_constant_val_based_on_str_method(df, faire_col, metadata_col):
            """
            Apply nucl_acid_ext and/or nucl_acid_ext_modify calculation using the mapper's add_constant_value_based_on_str_in_col method.
            """
            metadata_cols = metadata_col.split(' | ')

            if len(metadata_cols) != 4: 
                logger.error(f"Expected 4 nucl_acid_ext/nucl_acid_ext_modify related columns separated by '|' with extraction column column first, followed by word to be present, second, the neg_condition value, finally pos_condition_value; got: {metadata_col}")
                raise ValueError(f"nucl_acid_ext/nucli_acid_ext_modify deduction requires format 'metadata_col_name | word_present | neg_condition | pos_condition'")
                 
            col_name = metadata_cols[0]
            word = metadata_cols[1]
            neg_condition = metadata_cols[2]
            pos_condition = metadata_cols[3]
            
            # Apply the calculation to each row
            return df.apply(
                lambda row: mapper.add_constant_value_based_on_str_in_col(
                    metadata_row=row,
                    col_name=col_name,
                    str_condition=word,
                    pos_condition_const=pos_condition,
                    neg_condition_const=neg_condition
                ),
                axis=1
            )
    
    cols_applicable = ['nucl_acid_ext', 'nucl_acid_ext_modify']
    
    return (
            TransformationBuilder('nucl_acid_ext_or_modify_from_word_in_notes')
            .when(lambda f, m, mt: (
                f in cols_applicable and
                mt == 'related'
            ))
            .apply(
                apply_constant_val_based_on_str_method,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
        )


