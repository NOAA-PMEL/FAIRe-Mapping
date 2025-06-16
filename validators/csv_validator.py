import sys
sys.path.append("..") # uncomment when running locally, but comment back out for remote.
import pandas as pd
import csv
import warnings
from typing import Type
from pydantic import BaseModel, ValidationError
from models.experiment_run_metadata import ExperimentRunMetadata
from models.sample_metadata import SampleMetadata


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

    def validate_file(self, csv_path: str, strict: bool = True) -> CSVValidationResult:
        """Validate CSV file against pydantic model"""
        result = CSVValidationResult()

        try:
            df = pd.read_csv(csv_path, dtype={"materialSampleID": str, "sample_derived_from": str})
            result.total_rows = len(df)

            print(f"📊 Validating {result.total_rows} rows from '{csv_path}'")
            print(f"🔎 Using model: {self.model_class.__name__}")

            for idx, row in df.iterrows():
                try: 
                    # row_data = self._clean_row_data(row.to_dict())
                    row_data = row.to_dict()

                    # capture warnings during validation
                    with warnings.catch_warnings(record=True) as w:
                        warnings.simplefilter("always")
                        validated_record = self.model_class(**row_data)

                        # store any warnings that were raised
                        for warning in w:
                            result.warnings.append({
                                'row': idx + 1, 
                                'message': str(warning.message),
                                'category': warning.category.__name__,
                                'data': row_data
                            })

                    result.valid_records.append(validated_record.model_dump())

                except ValidationError as e:
                    error_info = { 
                        'row': idx + 1, 
                        'data': row_data,
                        'errors': e.errors()
                    }
                    result.invalid_records.append(error_info)
                    result.errors.append(f"Row {idx + 1}: {str(e)}")
                    if strict:
                        raise ValidationError(f"Validation failed at row {idx + 1}: {e}")
                except Exception as e:
                    error_msg = f"Failed to process CSV: {str(e)}"
                    result.errors.append(error_msg)
                    if strict:
                        raise Exception(error_msg)
                    
        except Exception as e:
            result.errors.append(f"Failed to read CSV file: {str(e)}")
            if strict:
                raise

        return result


def main():
    if len(sys.argv) < 3:
        print("Usage: python csv_validator.py <csv_file> [--strict]")
        print(" --strict: Stop validation on first error (default: continue)")
        sys.exit(1)

    csv_path = sys.argv[1]
    model_class_name = sys.argv[2]
    strict_mode = '--strict' in sys.argv

    # Initialize validator with your model
    validator = CSVValidator(model_class=model_class_name)

    try: 
        result = validator.validate_file(csv_path, strict=strict_mode)
        print(f"\n📋 Validation Results: ")
        print(f"    ✅  Valid rows: {len(result.valid_records)}" )
        print(f"    ❌ Invalid rows: {len(result.invalid_records)}")
        print(f"    📊 Total rows: {result.total_rows}")
        print(f"    📈 Success rate: {result.to_dict()['success_rate']:.1%}")

        if result.warnings:
            print(f"\n⚠️ Validation Warnings:")
            for warning in result.warnings:
                print(f"    Row {warning['row']}: {warning['message']}")

        if result.errors:
            print(f"\n🚨 Validation Errors:")
            for error in result.errors:
                print(f"    {error}")
                sys.exit(1)
        
        if len(result.invalid_records) > 0 or len(result.errors) > 0:
            print(f"\n💥 Validation failed!")
            sys.exit(1)
        else:
            if result.warnings:
                print(f"\n🎉 All rows passed validation (with {len(result.warnings)} warnings)!")
            else:
                print(f"\n🎉 All rows passed validation!")
            sys.exit(0)

    except Exception as e:
        print(f"❌ Validation error: {e}")

if __name__ == "__main__":
    main()


