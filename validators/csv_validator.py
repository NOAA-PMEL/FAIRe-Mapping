import sys
sys.path.append("..") # uncomment when running locally, but comment back out for remote.
import pandas as pd
import csv
import warnings
from typing import Type
from pydantic import BaseModel, ValidationError
from models.experiment_run_metadata import ExperimentRunMetadata
from models.sample_metadata import SampleMetadata, SampleMetadataDatasetModel
import traceback
import logging

# Setup logging to a file
logging.basicConfig(
    filename='validation_results.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CSVValidator")


class CSVValidationResult:
    def __init__(self):
        self.valid_records = []
        self.invalid_records = []
        self.warnings = []
        self.total_rows = 0
        self.errors = []

    def to_dict(self):
        return {
            'valid_count': len(self.valid_records),
            'invalid_count': len(self.invalid_records),
            'warning_count': len(self.warnings),
            'total_rows': self.total_rows, 
            'success_rate': len(self.valid_records) / self.total_rows if self.total_rows > 0 else 0, 
            'errors': self.errors,
            'warnings': self.warnings
            }
    
class CSVValidator:
    def __init__(self, model_class: Type[BaseModel]):
        if isinstance(model_class, str):
            self.model_class = globals()[model_class]
        else:
            self.model_class = model_class

    def validate_file(self, csv_path: str, strict: bool = False) -> CSVValidationResult:
        """Validate CSV file against pydantic model with detailed logging"""
        result = CSVValidationResult()

        try:
            # low_memory=False prevents DtypeWarnings on large files
            df = pd.read_csv(csv_path, dtype={"materialSampleID": str, "sample_derived_from": str}, low_memory=False)
            result.total_rows = len(df)

            print(f"📊 Validating {result.total_rows} rows from '{csv_path}'")
            print(f"🔎 Using model: {self.model_class.__name__}")

            validated_rows = []

            for idx, row in df.iterrows():
                line_no = idx + 2 # Header is line 1, so first data row is 2
                row_data = row.to_dict()

                # 1. CLEAN DATA: Convert NaNs to None so Pydantic handles them as Optional fields
                row_data = {k: (None if pd.isna(v) else v) for k, v in row_data.items()}

                # 2. GET SAMPLE NAME: Grab it now to use in error/warning messages
                samp_name = row_data.get('samp_name', f"Unknown_Row_{line_no}")

                try: 
                    # Capture warnings during validation (e.g., Arctic region checks)
                    with warnings.catch_warnings(record=True) as w:
                        warnings.simplefilter("always")
                        validated_record = self.model_class(**row_data)

                        # Store and log warnings
                        for warning in w:
                            warn_msg = f"Sample: {samp_name} | {str(warning.message)}"
                            result.warnings.append({
                                'row': line_no, 
                                'message': warn_msg,
                                'category': warning.category.__name__,
                                'data': row_data
                            })
                            logger.warning(f"Line {line_no}: {warn_msg}")
                    
                    validated_rows.append(validated_record)
                    result.valid_records.append(validated_record.model_dump())

                except ValidationError as e:
                    # 3. LOG ERRORS WITH SAMPLE NAME
                    for error in e.errors():
                        field = " -> ".join(str(l) for l in error['loc'])
                        err_msg = f"Line {line_no} | Sample: {samp_name} | Field: {field} | Error: {error['msg']}"
                        
                        logger.error(err_msg)
                        result.errors.append(err_msg)

                    result.invalid_records.append({ 
                        'row': line_no, 
                        'sample': samp_name,
                        'data': row_data,
                        'errors': e.errors()
                    })

                    if strict:
                        raise 

                except Exception as e:
                    # Catch structural code crashes (like the Date offset-naive error)
                    tb_str = traceback.format_exc()
                    error_msg = f"Line {line_no} | Sample: {samp_name} | Critical Process Error:\n{tb_str}"
                    
                    logger.error(error_msg)
                    result.errors.append(error_msg)
                    
                    if strict:
                        raise 
                    
            # Cross-row validation using dataset model
            if self.model_class == SampleMetadata and validated_rows:
                try:
                    SampleMetadataDatasetModel(rows=validated_rows)
                    print("✅ Cross-row validation passed")
                except Exception as e:
                    dataset_error = f"Dataset level error: {str(e)}"
                    result.errors.append(dataset_error)
                    logger.error(dataset_error)
                    if strict:
                        raise
                    
        except Exception as e:
            file_err = f"Failed to read CSV file: {str(e)}"
            result.errors.append(file_err)
            logger.error(file_err)
            if strict:
                raise

        return result


def main():
    if len(sys.argv) < 3:
        print("Usage: python csv_validator.py <csv_file> <model_name> [--strict]")
        sys.exit(1)

    csv_path = sys.argv[1]
    model_class_name = sys.argv[2]
    strict_mode = '--strict' in sys.argv

    validator = CSVValidator(model_class=model_class_name)

    try: 
        # Run the full validation
        result = validator.validate_file(csv_path, strict=strict_mode)
        
        # 1. Print the high-level summary to console
        print(f"\n📋 Validation Results for {csv_path}:")
        print(f"    ✅ Valid rows:   {len(result.valid_records)}")
        print(f"    ❌ Invalid rows: {len(result.invalid_records)}")
        print(f"    ⚠️ Warnings:     {len(result.warnings)}")
        print(f"    📈 Success rate: {result.to_dict()['success_rate']:.1%}")

        # 2. Decide how to exit based on results
        if result.invalid_records or result.errors:
            print(f"\n❌ Validation failed!")
            print(f"📂 Full details of all errors logged to: validation_results.log")
            sys.exit(1) 
        else:
            if result.warnings:
                print(f"\n🎉 Success! All rows passed (with {len(result.warnings)} warnings).")
            else:
                print(f"\n🎉 Success! All rows passed with no errors.")
            sys.exit(0)

    except Exception as e:
        print(f"❌ Critical Script Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()


