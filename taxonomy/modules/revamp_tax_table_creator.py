import pandas as pd

from taxonomy.modules.taxonomy_table_creator import TaxonomyTableCreator
from pathlib import Path

class RevampTaxTableCreator(TaxonomyTableCreator):

    # data
    TAXONOMY_UNKNOWNS = ["Unknown"] # TODO - REvamp uses Unknown, not sure if there will be other ways of saying this for other dbs.
    
    # REVAMP specific (maybe?)
    REVAMP_ACCESSION_MATCH_COL = "accession"
    REVAMP_PERCENT_MATCH_COL = "percent"
    REVAMP_MATCH_LENGTH_COL = "length"
    REVAMP_ACCESSION_ID_REF_DB = "NCBI Nucleotide"


    def __init__(self, taxon_tble_file: str, asv_match_db_results_file: str, asv_dna_seq_file: str, final_faire_template_path: str):
        """
        taxon_table_file: The path to the taxonomy table.txt file
        asv_match_db_results_file: The path to the accessions database results. E.g. for revamp its the blast results.
        asv_dna_seq_file: The path to the file with the asv/dna sequences
        """
        super().__init__(taxon_tble_file, asv_dna_seq_file, final_faire_template_path)

        self.asv_match_db_results_file_path = Path(asv_match_db_results_file)
        self.asv_match_db_results_df = self._get_asv_match_db_df()
        self.seq_accession_ids_dict = self._create_accession_ids_dict()
        self.seq_percent_match_dict = self._create_percent_match_dict()
        self.seq_query_cover_dict = self._create_percent_query_cov_dict()

    def get_faire_df(self) -> pd.DataFrame:
        """
        Load the taxon_tble_file as a data frame
        """
        # Load taxonomy text file as a data frame.
        df = self._load_tab_text_file_as_df(file_path=self.taxon_table_path)

        # Rename columns to match FAIRE
        df_cols_renamed = self._rename_kpcofgs_columns(df=df)

        # Replace ASVs with hashes
        df_with_hashes = self._switch_asv_for_hashes(df=df_cols_renamed)

        # First get VerbatimIdentification (because the Species column will be dropped and replaced 
        # with scientific epithet which changes the value)
        df_with_hashes[self.VERBATIM_IDENTFICATION] = df_with_hashes.apply(lambda row: self._get_verabtim_identification(row=row), 
                                                                           axis=1)

        # More Species column to be Scientific Epithet. Drop the species column after
        df_with_hashes[self.SPECIFIC_EPITHET] = df_with_hashes[self.SPECIES].apply(self._get_specific_epithet)

        # Get scientific name
        df_with_hashes[self.SCIENTIFIC_NAME] = df_with_hashes.apply(self._get_scientific_name, axis=1)
        
        # Get taxon_rank
        df_with_hashes[self.TAXON_RANK] = df_with_hashes.apply(self._get_taxon_rank, axis=1)

        # Get accession ids and accession_id_ref_db
        df_with_hashes[self.ACCESSION_ID] = df_with_hashes[self.SEQ_ID].map(self.seq_accession_ids_dict).fillna("not applicable")
        df_with_hashes[self.ACCESSION_ID_REF_DB] = self._get_accession_ref_db()

        # Get percent_match
        df_with_hashes[self.PERCENT_MATCH] = df_with_hashes[self.SEQ_ID].map(self.seq_percent_match_dict).fillna("not applicable")

        # Get percent_query_cover
        df_with_hashes[self.PERCENT_QUERY_COVER] = df_with_hashes[self.SEQ_ID].map(self.seq_query_cover_dict).fillna("not applicable")

        # Get confidence score
        df_with_hashes[self.CONFIDENCE_SCORE] = self._get_confidence_score()
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