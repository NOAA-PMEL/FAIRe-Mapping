import argparse
from .initial_google_push import GoogleSheetConcatenator

def main() -> None:

    parser = argparse.ArgumentParser(description='Push faire standardized metadata to google sheet')
    parser.add_argument('root_dir', type: str, help='The root dir with all the *_faire.csv files to concatenate')
    parser.add_argument('metadata_type', type=str, help='The metadata type that is being pushed: sampleMetadata or experimentRunMetadata exactly.') 

    args = parser.parse_args()

    GoogleSheetConcatenator(root_dir=args.root_dir, metadata_type=args.metadata_type)

if __name__ == "__main__":
    main()