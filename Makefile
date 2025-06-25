# Find directories for different metadata types
SAMPLE_SUBDIRS := $(shell find projects/*/*/ -name "main.py" -exec dirname {} \; 2>/dev/null | sort)
EXPERIMENT_SUBDIRS := $(shell find runs/ -name "main.py" -exec dirname {} \; 2>/dev/null | sort)

# Default target - .PHONY says that targets are not actual files, they're command names
.PHONY: all runSampleMetadata runExperimentMetadata

all: runSampleMetadata

# Run sample metadata projects
runSampleMetadata:
	@echo "Running each cruise sample metadata..."
	@for dir in $(SAMPLE_SUBDIRS); do \
		echo "-> Running $$dir/main.py"; \
		cd $$dir && python main.py && cd - > /dev/null; \
		echo ""; \
	done
	@echo "Sample metadata projects completed!"

# Run experiment metadata projects
runExperimentMetadata:
	@echo "Running each experiment metadata..."
	@for dir in $(EXPERIMENT_SUBDIRS); do \
		echo "-> Running $$dir/main.py"; \
		cd $$dir && python main.py && cd - > /dev/null; \
		echo ""; \
	done
	@echo "Experiment metadata projects completed!"

# Run both sample and experiment metadata
runAll: runSampleMetadata runExperimentMetadata
