# Find directories for different metadata types
SAMPLE_SUBDIRS := $(shell find projects/*/*/ -name "main.py" -exec dirname {} \; 2>/dev/null | sort)
EXPERIMENT_SUBDIRS := $(shell find runs/ -maxdepth 1 -type d ! -name runs 2>/dev/null | sort)
# Find config.yaml files in experiment subdirectories (relative to runs/ directory)
EXPERIMENT_CONFIG_FILES := $(shell cd runs && find . -maxdepth 2 -name "config.yaml" 2>/dev/null | sort)

# API rate limiting delay (in seconds)
API_DELAY := 10

# Optional filter parameter - can be set on command line
FILTER ?=

# Find CSV files in projects directories for validation
SAMPLE_CSV_FILES := $(shell find projects/*/*/data/ -name "*.csv" 2>/dev/null | sort) \
                   $(shell find projects/*/data/ -name "*sampleMetadata*.csv" 2>/dev/null | sort)

# Default target
.PHONY: all runSampleMetadata runExperimentMetadata validateSampleCSVs
all: runSampleMetadata runExperimentMetadata

# Run sample metadata projects
runSampleMetadata:
	@echo "Running each cruise sample metadata..."
	@if [ "$(FILTER)" != "" ]; then \
		echo "Filtering for subdirectories containing: $(FILTER)"; \
		filtered_dirs=$$(echo "$(SAMPLE_SUBDIRS)" | tr ' ' '\n' | grep "$(FILTER)"); \
		if [ -z "$$filtered_dirs" ]; then \
			echo "No directories found containing '$(FILTER)'"; \
			exit 1; \
		fi; \
		for dir in $$filtered_dirs; do \
			echo "-> Running $$dir/main.py"; \
			cd $$dir && python main.py && cd - > /dev/null; \
			echo "   Waiting $(API_DELAY) seconds to avoid API rate limits..."; \
			sleep $(API_DELAY); \
			echo ""; \
		done; \
	else \
		for dir in $(SAMPLE_SUBDIRS); do \
			echo "-> Running $$dir/main.py"; \
			cd $$dir && python main.py && cd - > /dev/null; \
			echo "   Waiting $(API_DELAY) seconds to avoid API rate limits..."; \
			sleep $(API_DELAY); \
			echo ""; \
		done; \
	fi
	@echo "Sample metadata projects completed!"

# Run experiment metadata projects
runExperimentMetadata:
	@echo "Running experiment metadata with different configs..."
	@for config_file in $(EXPERIMENT_CONFIG_FILES); do \
		echo "-> Running main.py with config $$config_file"; \
		cd runs && python main.py "$$config_file" && cd - > /dev/null; \
		echo "   Waiting $(API_DELAY) seconds to avoid API rate limits..."; \
		sleep $(API_DELAY); \
		echo ""; \
	done
	@echo "Experiment metadata projects completed!"

# Validate sample CSV files
validateSampleCSVs:
	@echo "Validating sample CSV files..."
	@if [ -z "$(SAMPLE_CSV_FILES)" ]; then \
		echo "No sample CSV files found to validate"; \
		exit 0; \
	fi
	@for csv_file in $(SAMPLE_CSV_FILES); do \
		echo "-> Validating $csv_file"; \
		python validators/csv_validator.py "$csv_file" "SampleMetadata"; \
		echo ""; \
	done
	@echo "Sample CSV validation completed!"

# Validate CSV files in projects directories with optional filtering
validateSampleMetadataCSVs:
	@echo "Validating sample metadata CSV files..."
	@if [ "$(FILTER)" != "" ]; then \
		echo "Filtering for projects containing: $(FILTER)"; \
		filtered_files=$$(echo "$(SAMPLE_CSV_FILES)" | tr ' ' '\n' | grep "$(FILTER)"); \
		if [ -z "$$filtered_files" ]; then \
			echo "No CSV files found in projects containing '$(FILTER)'"; \
			exit 0; \
		fi; \
		for csv_file in $$filtered_files; do \
			echo "-> Validating $$csv_file"; \
			python validators/csv_validator.py "$$csv_file" "SampleMetadata"; \
			echo ""; \
		done; \
	else \
		if [ -z "$(SAMPLE_CSV_FILES)" ]; then \
			echo "No CSV files found in projects directories"; \
			exit 0; \
		fi; \
		for csv_file in $(SAMPLE_CSV_FILES); do \
			echo "-> Validating $$csv_file"; \
			python validators/csv_validator.py "$$csv_file" "SampleMetadata"; \
			echo ""; \
		done; \
	fi
	@echo "Sample Metadata CSV validation completed!"
