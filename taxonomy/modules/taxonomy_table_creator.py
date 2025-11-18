from pathlib import Path
from abc import ABC, abstractmethod
import pandas as pd
import hashlib
import openpyxl
from openpyxl.utils import get_column_letter

# TODO: Columns left to add :
    # scientificNameAuthorship
    # taxonID
    # taxonID_db (either NCBI, Bold, or PR2, or WORMS, or GBIF)
# TODO: Add assay name to sheet name when saving to the FAIRe excel file???
# TODO: THis only works for Revamp - will need to adjust/expand for other taxonomy methods

class TaxonomyTableCreator(ABC):
    # FAIRE Taxonomy fields (Includes ones we are adding as User Defined fields like Supergroup)
    SEQ_ID = "seq_id"
    DNA_SEQUENCE = "dna_sequence"
    KINGDOM = "kingdom"
    SUPERGROUP = "supergroup"
    DIVISION = "division"
    SUBDIVISION = "subdivision"
    PHYLUM = "phylum"
    CLASS = "class"
    ORDER = "order"
    FAMILY = "family"
    GENUS = "genus"
    SPECIES = "species"
    SPECIFIC_EPITHET = "specificEpithet"
    SCIENTIFIC_NAME = "scientificName"
    SCIENTIFIC_NAME_AUTHORSHIP = "scientificNameAuthorship"
    TAXON_RANK = "taxonRank"
    TAXON_ID_DB = "taxonID_db"
    ACCESSION_ID = "accession_id"
    ACCESSION_ID_REF_DB = "accession_id_ref_db"
    VERBATIM_IDENTFICATION = "verbatimIdentification"
    PERCENT_MATCH = "percent_match"
    PERCENT_QUERY_COVER = "percent_query_cover"
    CONFIDENCE_SCORE = "confidence_score"

    # Other
    TAXA_FAIRE_TEMPLATE_PATH = "/home/poseidon/zalmanek/FAIRe-Mapping/taxonomy/taxa_faire_template.xlsx"

    def __init__(self, taxon_tble_file: str, asv_dna_seq_file: str, final_faire_template_path: str):
        """
        taxon_table_file: The path to the taxonomy table.txt file
        asv_dna_seq_file: The path to the file with the asv/dna sequences
        """
        self.taxon_table_path = Path(taxon_tble_file)
        self.asv_dna_seq_file = Path(asv_dna_seq_file)
        self.final_faire_excel_path = final_faire_template_path
        self.hash_dna_seq_dict = self._create_asv_hash_dict()
    
    @property
    @abstractmethod
    def taxonomy_cols(self):
        """
        Sublasses must define their taxonomy columns
        """
        pass

    @property
    @abstractmethod
    def taxonomy_unknowns(self):
        """
        Subclasses must define their taxonomy unknowns
        """
        pass

    @abstractmethod
    def build_faire_df(self) -> pd.DataFrame:
        """
        Create the final FAIRe Taxonomy data frame. An abstract method
        because child classes based on bioinformatics methods should have
        this defined differently.
        """
        pass
    
    def _load_tab_text_file_as_df(self, file_path: Path) -> pd.DataFrame:
        """
        Loads a text file that is tab delimated as a pandas data frame.
        """
        df = pd.read_csv(file_path, sep='\t')

        return df
    
    def _create_asv_hash_dict(self) -> dict:
        # creates a dictionary like {'ASV1': {'seq': ACGT, 'hash': 'dafadsf'}}
        # replace this in experiment_run_mapper if end up using the same dictionary.
        asv_hash_dict = {}
        current_asv = None
        seq_lines = []

        print(f"Creating ASV hash dict for {self.asv_dna_seq_file}")
        with open(self.asv_dna_seq_file, 'r') as f:
            for line in f:
                line = line.strip()

                if not line: # Skip empty lines
                    continue
                
                # Get the first ASV
                if current_asv == None:
                    if line.startswith('>'):
                        current_asv = line.replace('>', '')
                        seq_lines = []
                    else:
                        seq_lines.append(line)

                # For other ASVs that aren't first row
                else:
                    if line.startswith('>'):
                        # Add the old current asv first
                        seq = "".join(seq_lines)
                        seq_hash = hashlib.md5(seq.encode()).hexdigest()
                        asv_hash_dict[current_asv] = {
                            'seq': seq, 
                            'hash': seq_hash
                            }

                        # Then create new current_asv and reset seq lines
                        current_asv = line.replace('>', '')
                        seq_lines = []
                    else:
                        seq_lines.append(line)
        
        # *** CRITICAL FIX: Process the last ASV after the loop finishes ***
        if current_asv is not None and seq_lines:
            seq = "".join(seq_lines)
            seq_hash = hashlib.md5(seq.encode()).hexdigest()
            asv_hash_dict[current_asv] = {
                'seq': seq, 
                'hash': seq_hash
                }

        return asv_hash_dict

    def _switch_asv_for_hashes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Switches the ASV names in column one of the taxonomy df for the 
        corresponding hashes
        """
        # Create flat mapping dictionary of {'ASV_1': 'kasdhfkajdsf'}
        hash_map = {
            seq_id: data['hash'] for seq_id, data in self.hash_dna_seq_dict.items()
        }
        # Replace ASV strings with hashes
        df[self.SEQ_ID] = df[self.SEQ_ID].map(hash_map)
        return df
    
    def _create_dna_sequence_col(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a column called dna_sequence that adds the corresponding
        DNA sequence to based on the ASV. Needs to come abefore the ASVs have
        been switched to hashes.
        """
        hash_map = {
            seq_id: data['seq'] for seq_id, data in self.hash_dna_seq_dict.items()
        }
        df[self.DNA_SEQUENCE] = df[self.SEQ_ID].map(hash_map)

        return df

    def _get_taxon_rank(self, row: pd.Series) -> str:
        """
        Gets the most specific column name for the most specific
        name in the Scientific name.
        """
        # Iterate through the taxonomic cols in reverse order (most specific first)
        for col in reversed(self.taxonomy_cols):
            if pd.notna(row[col]) and row[col] not in self.taxonomy_unknowns:
                if col == self.SPECIFIC_EPITHET:
                    return "species"
                else:
                    return col
   
        return "missing: not provided"
    
    def _get_scientific_name(self, row: pd.Series) -> str:
        """
        Gets the most scientific name for the most specific 
        value in the taxonomies.
        """
        for col in reversed(self.taxonomy_cols):
            if pd.notna(row[col]) and row[col] not in self.taxonomy_unknowns:
                return row[col]
        
        return "not applicable"

    def add_final_df_to_FAIRe_excel(self, final_faire_df: pd.DataFrame, sheet_name: str = "taxaFinal"):
        # Step 1 load the workbook to preserve formatting
        workbook = openpyxl.load_workbook(self.TAXA_FAIRE_TEMPLATE_PATH)
        sheet = workbook[sheet_name]

        # Step 2: Store original row 2 headers and original row 3 columns
        # This is done before any modifications to identify what was originally in the template.
        original_row2_headers = {}
        original_columns_in_sheet = []
        for col_idx in range(1, sheet.max_column + 1):
            col_name = sheet.cell(row=3, column=col_idx).value
            if col_name:
                original_columns_in_sheet.append(col_name)
                # Store the row 2 value for this column name
                original_row2_headers[col_name] = sheet.cell(row=2, column=col_idx).value
        
        # Step 4: Clear existing headers (rows 2 and 3) and all data from row 4 onwards
        # This ensures a clean slate before writing new, reordered headers and data.
        for row in range(1, sheet.max_row + 1): # Clear starting from row 1 to be safe
            for col in range(1, sheet.max_column + 1):
                sheet.cell(row=row, column=col).value = None

        # Step 5: Write the new headers (row 2 and row 3) based on the reordered DataFrame
        for col_idx, col_name in enumerate(final_faire_df.columns, 1):
            col_letter = get_column_letter(col_idx)
            
            # Write column name to row 3 (the main header row)
            sheet[f'{col_letter}3'] = col_name

            # Determine and write row 2 header
            if col_name in original_columns_in_sheet:
                # If the column existed in the original template, use its original row 2 header
                sheet[f'{col_letter}2'] = original_row2_headers.get(col_name)
            else:
                # If it's a new column, set row 2 to "User defined"
                sheet[f'{col_letter}2'] = 'User defined'

        # Step 6: Write the data frame data to the sheet (starting at row 4)
        # Iterate through the reordered DataFrame and write its values to the Excel sheet.
        for row_idx, row_data in enumerate(final_faire_df.values, 4):
            for col_idx, value in enumerate(row_data, 1):
                sheet.cell(row=row_idx, column=col_idx).value = value

        # Step 7: Save the workbook
        workbook.save(self.final_faire_excel_path)
        print(f"sheet {sheet_name} saved to {self.final_faire_excel_path}!")
                    
                    
                   