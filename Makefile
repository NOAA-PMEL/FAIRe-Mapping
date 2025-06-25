# Find directories for different metadata types
SAMPLE_SUBDIRS := $(shell find projects/*/*/ -name "main.py" -exec dirname {} \; 2>/dev/null | sort)
EXPERIMENT_SUBDIRS := $(shell find runs/ -maxdepth 1 -type d ! -name runs 2>/dev/null | sort)

# API rate limiting delay (in seconds)
API_DELAY := 5

# Default target - .PHONY says that targets are not actual files, they're command names
.PHONY: all runSampleMetadata runExperimentMetadata

all: runSampleMetadata runExperimentMetadata

# Run sample metadata projects
runSampleMetadata:
	@echo "Running each cruise sample metadata..."
	@for dir in $(SAMPLE_SUBDIRS); do \
		echo "-> Running $$dir/main.py"; \
		cd $$dir && python main.py && cd - > /dev/null; \
		echo "   Waiting $(API_DELAY) seconds to avoid API rate limits..."; \
		sleep $(API_DELAY); \
		echo ""; \
	done
	@echo "Sample metadata projects completed!"

# Run experiment metadata projects
runExperimentMetadata:
	@echo "Running experiment metadata with different configs..."
	@for config_file in $(EXPERIMENT_CONFIG_FILES); do \
		echo "-> Running runs/main.py with config $config_file"; \
		cd runs && python main.py "$config_file" && cd - > /dev/null; \
		echo "   Waiting $(API_DELAY) seconds to avoid API rate limits..."; \
		sleep $(API_DELAY); \
		echo ""; \
	done
	@echo "Experiment metadata projects completed!"
