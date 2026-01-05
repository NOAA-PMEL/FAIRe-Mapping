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

def get_fallback_col_mapping_rule(mapper: FaireSampleMetadataMapper, faire_field_name: str):
     """
     Rule for mapping with fallback columns.
     Expects metadata_col to be in format 'fallback: primary_col | fallback_col | fallback_col2 ...'
     Can optionally include transform flag for date times 'fallback: primary_col | fallbackcol | transform:true'
     Calls the rule in the main.py file be specifyinf the faire_field_name it applies to (helps with ordering)
     """
     def apply_fallback_mapping_rule(df, faire_col, metadata_col):
        """
        Apply fallback logic using the mapper's map_using_two_or_three_cols_if_one_is_na_use_other method.
        """
        # Remove the 'fallback:' prefix
        if not metadata_col.startswith('fallback:'):
             return None
        # Parse the metadata_col to extract column names and options
        columns_part = metadata_col.replace('fallback:', '').strip()
        parts = [part.strip() for part in columns_part.split('|')]

        # Check if there's a transform flag
        transform_to_datetime = False
        columns = []
        for part in parts:
            if part.startswith('transform:'):
                transform_to_datetime = part.split(':'[1].lower() == 'true')
            else:
                    columns.append(part)

        if len(columns) < 2:
             logger.error(f"Fallback mapping requires at least 2 columns separated by '|'")
             raise ValueError(f"Fallback mapping requires format 'primary_col | fallback_col1 | [fallback_col2] | [transform: ture/false]'")
        
        desired_col = columns[0]
        use_if_na_col = columns[1] if len(columns) > 1 else None
        use_if_second_na_col = columns[2] if len(columns) > 2 else None

        # Apply the fallback logic to each row
        return df.apply(
             lambda row: mapper.map_using_two_or_three_cols_if_one_is_na_use_other(
                metadata_row=row,
                desired_col_name=desired_col,
                use_if_na_col_name=use_if_na_col,
                transform_use_col_to_date_format=transform_to_datetime,
                use_if_second_col_is_na=use_if_second_na_col
             ),
             axis=1
        )
     
     return (
        TransformationBuilder('fallback_column_mapping')
            .when(lambda f, m, mt: (
                m.startswith('fallback:') and
                f == faire_field_name and
                '|' in m and # contains pipe separator indicating fallback
                mt == 'related'
                ))
            .apply(
                apply_fallback_mapping_rule,
                mode='direct'
            )
            .for_mapping_type('related')
            .update_source(True)
            .build()
     )

def get_max_depth_with_pressure_fallback(mapper: FaireSampleMetadataMapper, pressure_cols: list, lat_col: str, depth_cols: list):
    """
    Complex rule for maximumDepthInMeters with pressure fallback logic.
    Will basically use two pressure columns to fallback to create pressure, then calculate depth from pressure, and
    use the output of that, plus another depth column to create the maximumDepthInMeters (used in DY2206)
    Configuration is passed as paramters instead of through mapping file
    """
    def apply_compmlse_depth_calculated(df, faire_col, metadata_col):
        # Ignore metadata_col from mapping file, used passed configuration instead
        # Step 1: Get pressure using fallback
        pressure_series = df.apply(
            lambda row: mapper.map_using_two_or_three_cols_if_one_is_na_use_other(
            metadata_row=row,
            desired_col_name=pressure_cols[0],
            use_if_na_col_name=pressure_cols[1] if len(pressure_cols) > 1 else None,
            transform_use_col_to_date_format=False,
            use_if_second_col_is_na=pressure_cols[2] if len(pressure_cols)> 2 else None
            ),
        axis=1
        )

        # Add pressure to dataframe
        df_with_pressure = df.copy()
        df_with_pressure['_temp_pressure'] = pressure_series

        # Step 2. Calculate depth from pressure
        depth_from_pressure_series = df_with_pressure.apply(
            lambda row: mapper.get_depth_from_pressure(
            metadata_row=row,
            press_col_name='_temp_pressure',
            lat_col_name=lat_col
            ),
        axis=1
        )

        # Add calculated depth to dataframe
        df_with_pressure['_temp_depth_from_pressure'] = depth_from_pressure_series

        # Step 3 Use fallback with calculate depth and other depth columns
        all_depth_options = ['_temp_depth_from_pressure'] + depth_cols

        final_depth = df_with_pressure.apply(
            lambda row: mapper.map_using_two_or_three_cols_if_one_is_na_use_other(
                metadata_row=row,
                desired_col_name=all_depth_options[0],
                use_if_na_col_name=all_depth_options[1] if len(all_depth_options) > 1 else None,
                transform_use_col_to_date_format=False,
                use_if_second_col_is_na=all_depth_options[2] if len(all_depth_options) > 2 else None
            ),
            axis=1    
        )
        return final_depth

    return (
         TransformationBuilder('max_depth_with_pressure_fallback')
         .when(lambda f, m, mt: (
              f == 'maximumDepthInMeters' and 
              mt == 'related'
         ))
         .apply(apply_compmlse_depth_calculated, mode='direct')
         .update_source(True)
         .for_mapping_type('related')
         .build()
    )

def get_condition_constant_rule(mapper: FaireSampleMetadataMapper, faire_col: str, ref_col: str):
    """
    Rule that assigns a constant value if a reference col value is NA.
    Expects metadata_col format to be one value, the string to be applied if the ref_col value is not none
    """
    def apply_conditional_constant(df, faire_col, metadata_col):
        constant_val = metadata_col

        if ref_col not in df.columns:
            logger.warning(f"Reference column '{ref_col}' not found in data frame")
            raise ValueError(f"Reference column not found in df: {ref_col}, please check!")
        
        return df[ref_col].apply(
            lambda val: constant_val if pd.isna(val) else 'missing: not provided'
        )
     
    return (
          TransformationBuilder('conditional_consant_if_not_na')
            .when(lambda f, m, mt: (
                f == faire_col and 
                mt == 'related'
            ))
            .apply(
                apply_conditional_constant,
                mode='direct'
            )
            .for_mapping_type('related')
            .build()
     )

                    


