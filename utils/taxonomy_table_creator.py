from pathlib import Path
import pandas as pd
import hashlib

# TODO: Get VerbatimIdentification first because Species column has values like Vexillifera sp_ AKu_2020a, where its an unclassified 
# species the AKu_2020a is the NCBI identifier. Consider adding the taxonID too. The specificEpithet functionality will drop this info.
class TaxonomyTableCreator:
    # FAIRE Taxonomy fields
    SEQ_ID = "seq_id"
    DNA_SEQUENCE = "dna_sequence"
    KINGDOM = "kingdom"
    PHYLUM = "phylum"
    CLASS = "class"
    ORDER = "order"
    FAMILY = "family"
    GENUS = "genus"
    SPECIFIC_EPITHET = "specificEpithet"
    SCIENTIFIC_NAME = "scientificName"
    SCIENTIFIC_NAME_AUTHORSHIP = "scientificNameAuthorship"
    TAXON_RANK = "taxonRank"

    TAXONOMY_COLS = [KINGDOM, PHYLUM, CLASS, ORDER, FAMILY, GENUS]
    TAXONOMY_UNKNOWNS = ["Unknown"] # TODO - REvamp uses Unknown, not sure if there will be other ways of saying this for other dbs.
    
    def __init__(self, taxon_tble_file: str, asv_dna_seq_file: str):
        """
        taxon_table_file: The path to the taxonomy table.txt file
        asv_dna_seq_file: The path to the file with the asv/dna sequences
        """

        self.taxon_table_path = Path(taxon_tble_file)
        self.asv_dna_seq_file = Path(asv_dna_seq_file)
        self.hash_dna_seq_dict = self._create_asv_hash_dict()
        self.taxon_df = self.get_faire_df()
    
    def get_faire_df(self) -> pd.DataFrame:
        """
        Load the taxon_tble_file as a data frame
        """
        df = pd.read_csv(self.taxon_table_path, sep='\t')

        # Rename columns to match FAIRE
        df_cols_renamed = self._rename_kpcofgs_columns(df=df)

        # Replace ASVs with hashes
        df_with_hashes = self._switch_asv_for_hashes(df=df_cols_renamed)

        # Fix scientificEpithet column (which is currently just the species column from the original df)
        df_with_hashes[self.SPECIFIC_EPITHET] = df_with_hashes[self.SPECIFIC_EPITHET].apply(self._get_specific_epithet)

        # Get scientific name
        df_with_hashes[self.SCIENTIFIC_NAME] = df_with_hashes.apply(self._get_scientific_name, axis=1)
        
        # Get taxon_rank
        df_with_hashes[self.TAXON_RANK] = df_with_hashes.apply(self._get_taxon_rank, axis=1)

        return df_with_hashes
    
    def _create_asv_hash_dict(self) -> dict:
        # creates a dictionary like {'ASV1': 'seq': ACGT, 'hash': 'dafadsf'}
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

    def _rename_kpcofgs_columns(self, df: pd.DataFrame) -> pd.DataFrame:   
        """
        Renames the columns from the original loaded taxonomy file to match
        FAIRe's colums for Kingdom, Phylum, Class, Order, Family, Genus, Species
        """
        col_renames = {}
        for col in df.columns:
            col = col.strip()
            if col.lower() == 'asv':
                col_renames[col] = self.SEQ_ID
            elif col.lower() == self.KINGDOM:
                col_renames[col] = self.KINGDOM
            elif col.lower() == self.PHYLUM:
                col_renames[col] = self.PHYLUM
            elif col.lower() == self.CLASS:
                col_renames[col] = self.CLASS
            elif col.lower() == self.ORDER:
                col_renames[col] = self.ORDER
            elif col.lower() == self.FAMILY:
                col_renames[col] = self.FAMILY
            elif col.lower() == self.GENUS:
                col_renames[col] = self.GENUS
            elif col.lower() == "species":
                col_renames[col] = self.SPECIFIC_EPITHET

        df.rename(columns=col_renames, inplace=True)
        return df

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

    def _get_taxon_rank(self, row: pd.Series) -> str:
        """
        Gets the most specific column name for the most specific
        name in the Scientific name.
        """
        # Iterate through the taxonomic cols in reverse order (most specific first)
        for col in reversed(self.TAXONOMY_COLS):
            if pd.notna(row[col]) and row[col] not in self.TAXONOMY_UNKNOWNS:
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
        for col in reversed(self.TAXONOMY_COLS):
            if pd.notna(row[col]) and row[col] not in self.TAXONOMY_UNKNOWNS:
                return row[col]
        
        return "not applicable"

    def _get_specific_epithet(self, species_value: str) -> str:
        """
        The specific epithet column is currently just mapped to the species
        column in the origina taxonomy.txt file, need to edit to fit 
        the speicicEpithet which is the lower case value of the species, 
        everything else that is NA will become 'not applicable'
        """
        
        if pd.isna(species_value) or species_value in self.TAXONOMY_UNKNOWNS:
            return "not applicable"
        else:
            species_words = str(species_value).split()
            if len(species_words) ==2:
                return species_words[1]
            else:
                print(f"There appear to be a weird species structure with value {species_value}. Please check this and update code accordingly")
                return "not applicable" 
                    
                    
              

                    
                    
                    
                    
                    
                    
                   