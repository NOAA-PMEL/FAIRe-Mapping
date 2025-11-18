import pandas as pd

from taxonomy.modules.taxonomy_table_creator import TaxonomyTableCreator
from pathlib import Path

class RevampTaxTableCreator(TaxonomyTableCreator):

    # REVAMP specific (maybe?)
    REVAMP_ACCESSION_MATCH_COL = "accession"
    REVAMP_PERCENT_MATCH_COL = "percent"
    REVAMP_MATCH_LENGTH_COL = "length"
    REVAMP_ACCESSION_ID_REF_DB = "NCBI Nucleotide"


    def __init__(self, taxon_tble_file: str, asv_match_db_results_file: str, asv_dna_seq_file: str, final_faire_excel_path: str):
        """
        taxon_table_file: The path to the taxonomy table.txt file
        asv_match_db_results_file: The path to the accessions database results. E.g. for revamp its the blast results.
        asv_dna_seq_file: The path to the file with the asv/dna sequences
        """
        super().__init__(taxon_tble_file, asv_dna_seq_file, final_faire_excel_path)

        self.asv_match_db_results_file_path = Path(asv_match_db_results_file)
        self.asv_match_db_results_df = self._get_asv_match_db_df()
        self.seq_accession_ids_dict = self._create_accession_ids_dict()
        self.seq_percent_match_dict = self._create_percent_match_dict()
        self.seq_query_cover_dict = self._create_percent_query_cov_dict()

        self.taxon_df = self.build_faire_df()

    @property
    def taxonomy_cols(self):
        return [self.KINGDOM, self.PHYLUM, self.CLASS, self.ORDER, self.FAMILY, self.GENUS, self.SPECIES]
    
    @property
    def taxonomy_unknowns(self):
        return ["Unknown"]
    
    def build_faire_df(self) -> pd.DataFrame:
        """
        Load the taxon_tble_file as a data frame
        """
        # Load taxonomy text file as a data frame.
        df = self._load_tab_text_file_as_df(file_path=self.taxon_table_path)

        # Rename columns to match FAIRE
        df_cols_renamed = self._rename_kpcofgs_columns(df=df)
        print("cols renamed!")

        # Add dna_sequence column with corresponding sequences
        df_with_dna_seq = self._create_dna_sequence_col(df=df_cols_renamed)
        print("Added DNA sequences!")

        # Replace ASVs with hashes
        df_with_hashes = self._switch_asv_for_hashes(df=df_with_dna_seq)
        print("ASVS switched with hashes!")

        # First get VerbatimIdentification (because the Species column will be dropped and replaced 
        # with scientific epithet which changes the value)
        df_with_hashes[self.VERBATIM_IDENTFICATION] = df_with_hashes.apply(lambda row: self._get_verabtim_identification(row=row), 
                                                                           axis=1)
        print("Verbatim_Identification created!")

        # More Species column to be Scientific Epithet. Drop the species column after
        df_with_hashes[self.SPECIFIC_EPITHET] = df_with_hashes[self.SPECIES].apply(self._get_specific_epithet)
        print("specific epithet created!")

        # Get scientific name
        df_with_hashes[self.SCIENTIFIC_NAME] = df_with_hashes.apply(self._get_scientific_name, axis=1)
        print("Scientific name created!")
        
        # Get taxon_rank
        df_with_hashes[self.TAXON_RANK] = df_with_hashes.apply(self._get_taxon_rank, axis=1)
        print("Taxon rank created!")

        # Get accession ids and accession_id_ref_db
        df_with_hashes[self.ACCESSION_ID] = df_with_hashes[self.SEQ_ID].map(self.seq_accession_ids_dict).fillna("not applicable")
        df_with_hashes[self.ACCESSION_ID_REF_DB] = self._get_accession_ref_db()
        print("Accession Id and Accession ID ref db created!")

        # Get percent_match
        df_with_hashes[self.PERCENT_MATCH] = df_with_hashes[self.SEQ_ID].map(self.seq_percent_match_dict).fillna("not applicable")
        print("percent_match create!")

        # Get percent_query_cover
        df_with_hashes[self.PERCENT_QUERY_COVER] = df_with_hashes[self.SEQ_ID].map(self.seq_query_cover_dict).fillna("not applicable")
        print("percent_query_cover created!")

        # Get confidence score
        df_with_hashes[self.CONFIDENCE_SCORE] = self._get_confidence_score()
        print("confidence score created!")
        
        return df_with_hashes
    
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
            elif col.lower() == self.SPECIES:
                col_renames[col] = self.SPECIES

        df.rename(columns=col_renames, inplace=True)
        return df
    
    def _get_verabtim_identification(self, row: pd.Series) -> str:
        """
        Get the verbatim identification by string together values in 
        taxonomic columns.
        """
        kingdom = row[self.KINGDOM]
        phylum = row[self.PHYLUM]
        tax_class = row[self.CLASS]
        order = row[self.ORDER]
        family = row[self.FAMILY]
        genus = row[self.GENUS]
        species = row[self.SPECIES]

        verbatim_list = [kingdom, phylum, tax_class, order, family, genus, species]
        # Drop na values (but keep any strings the say unkown, or what not)
        verbatim_list = [val for val in verbatim_list if pd.notna(val)]
        verbatim_str = ', '.join(verbatim_list)
        return verbatim_str
    
    def _get_specific_epithet(self, species_value: str) -> str:
        """
        The specific epithet column is currently just mapped to the species
        column in the origina taxonomy.txt file, need to edit to fit 
        the speicicEpithet which is the lower case value of the species, 
        everything else that is NA will become 'not applicable'
        """
        
        if pd.isna(species_value) or species_value in self.taxonomy_unknowns:
            return "not applicable"
        else:
            species_words = str(species_value).split()
            if len(species_words) ==2:
                return species_words[1]
            elif len(species_words) > 2 and ('sp_' in species_words or 'cf_' in species_words or 'aff_' in species_words):
                return f"Unclassified {' '. join(species_words[1:])}"
            elif len(species_words) == 3: # Has subspecies
                return species_words[1]
            elif "endosymbiont":
                return "endosymbiont"
            else:
                print(f"\033[35mThere appear to be a weird species structure with value {species_value}.\033[0m")
                return "not applicable" 

    def _get_accession_ref_db(self) -> str:
        """
        Get the accession ref db based on the file names
        """
        if "revamp" in str(self.taxon_table_path).lower():
            accession_ref_db = self.REVAMP_ACCESSION_ID_REF_DB
        else:
            raise ValueError("Have not added functionality for your reference database!")

        return accession_ref_db

    def _get_confidence_score(self):
        """
        Get the confidence score, for REVAMP, just NA
        """   
        if "revamp" in str(self.taxon_table_path).lower():
            return "not applicable"  
        else: 
            raise ValueError("Do not know how to get confidence score for this method! Add functionlity!")
          
    def _get_asv_match_db_df(self) -> pd.DataFrame:
        """
        Puts the asv_match_db_results_file into a data frame. Replaces 
        ASV strings with md5 hashes.
        """
        df = self._load_tab_text_file_as_df(file_path=self.asv_match_db_results_file_path)

        df.rename(columns={'ASV': self.SEQ_ID}, inplace=True)

        # Replace ASVs with hashes
        df_with_hashes = self._switch_asv_for_hashes(df=df)

        return df_with_hashes
    
    def _create_accession_ids_dict(self) -> dict:
        """
        Creates a dictionary of seq: accession ids
        """    
        accession_dict = self.asv_match_db_results_df.set_index(self.SEQ_ID)[self.REVAMP_ACCESSION_MATCH_COL].to_dict()
        # update list of accession to use |
        for accession in accession_dict.values():
            accession.replace(',', ' |')

        return accession_dict 
    
    def _create_percent_match_dict(self) -> dict:
        """
        Creates the seq_id: percent_match dictionary
        """
        percent_match_dict = self.asv_match_db_results_df.set_index(self.SEQ_ID)[self.REVAMP_PERCENT_MATCH_COL].to_dict()

        return percent_match_dict 
    
    def _create_percent_query_cov_dict(self) -> dict:
        """
        Creates a dictionary of seq_id: percent_query_coverage by creating
        a dictionary of the seq_id: match_length. And then finding the 
        length of the DNA sequences
        """
        length_match_dict = self.asv_match_db_results_df.set_index(self.SEQ_ID)[self.REVAMP_MATCH_LENGTH_COL].to_dict()

        # Create dictionary of {seq_id: seq_length}
        seq_length_map = {
            data['hash']: len(data['seq']) for data in self.hash_dna_seq_dict.values()
        }

        percent_query_cov_dict = {}
        for seq_hash, length in length_match_dict.items():
            # Turn lengths from comma separated string into list of integers
            match_lengths = length.split(',')
            match_lengths = [int(s) for s in match_lengths]

            # Divide match lengths by sequence lengths (if more than one value only get the first and last value of the list and give a range)
            percent_query_cov_list = []
            for match_length in match_lengths:
                seq_length = seq_length_map.get(seq_hash, None)
                percent_cov = (match_length/seq_length)*100
                percent_query_cov_list.append(percent_cov)
            percent_query_cov_list = sorted(percent_query_cov_list)
            if len(percent_query_cov_list) > 1:
                percent_query_cov_str = f"{round(percent_query_cov_list[0], 1)} - {round(percent_query_cov_list[-1], 1)}"
            elif len(percent_query_cov_list) == 1:
                percent_query_cov_str = str(round(percent_query_cov_list[0], 1))
            percent_query_cov_dict[seq_hash] = percent_query_cov_str

        return percent_query_cov_dict