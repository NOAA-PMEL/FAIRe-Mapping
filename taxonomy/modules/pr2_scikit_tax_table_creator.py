import pandas as pd
from taxonomy.modules.taxonomy_table_creator import TaxonomyTableCreator

from pathlib import Path

# TODO: Split out kindgom phylum class, etc. From Sam: "Levels of taxonomy for pr2:  Domain (replacing Kingdom), Supergroup, Division, Subdivision (new taxonomic rank added), Class, Order, Family, Genus, Species"

class Pr2ScikitTaxTableCreator(TaxonomyTableCreator):

    PR2_SCIKIT_SEQ_COL = "Feature ID"
    PR2_SCIKIT_TAXON_COL = "Taxon"
    PR2_SCIKIT_CONFIDENCE_COL = "Confidence"
    PR2_SCIKIT_ACCESSION_ID_REF_DB = "PR2"

    def __init__(self, taxon_tble_file: str, asv_dna_seq_file: str, final_faire_excel_path: str):
        """
        taxon_table_file: The path to the taxonomy table.txt file
        asv_dna_seq_file: The path to the file with the asv/dna sequences
        final_faire_excel_path: The path to save the final excel file to
        """
        super().__init__(taxon_tble_file, asv_dna_seq_file, final_faire_excel_path)

        self.taxon_df = self.build_faire_df()

    @property
    def taxonomy_cols(self):
        return [self.KINGDOM, self.SUPERGROUP, self.DIVISION, self.SUBDIVISION, self.CLASS, self.ORDER, self.FAMILY, self.GENUS, self.SPECIES]
    
    @property
    def taxonomy_unknowns(self):
        return ["Unassigned"]

    def build_faire_df(self) -> pd.DataFrame:
        """
        Load the taxon_tble_file as a data frame
        """
        # Load taxonomy text file as a data frame.
        df = self._load_tab_text_file_as_df(file_path=self.taxon_table_path)

        # Rename Feature ID columns with seq id and Confidence with confidence score, and Taxon to VerbatimIdentification
        df_renamed = self._rename_columns(df=df)
        print("Columns renamed!")

        # Replace ASVs with hashes
        df_with_hashes = self._switch_asv_for_hashes(df=df_renamed)
        print("ASVS switched with hashes!")

        # Add the dna_seq column
        df_with_dna_seqs = self._create_dna_sequence_col(df=df_with_hashes)
        print("DNA sequences added!")

        # Split Taxon column into separate columns as in FAIRe and make Taxon column become VerbatimIdentification
        df_taxon_split = self._split_taxons_into_columns(df=df_with_dna_seqs)
        print("Taxons separated out!")

        # Add specificEpithet
        df_taxon_split[self.SPECIFIC_EPITHET] = df_taxon_split[self.SPECIES].apply(self._get_specific_epithet)
        print("Scientific Epithet created!")

        # Add scientificName
        df_taxon_split[self.SCIENTIFIC_NAME] = df_taxon_split.apply(self._get_scientific_name, axis=1)
        print("ScientificName created!")

        # Add taxon_rank
        df_taxon_split[self.TAXON_RANK] = df_taxon_split.apply(self._get_taxon_rank, axis=1)
        print("taxon_rank added!")

        # Add cols with constant values
        df_updated = self._fill_constant_known_cols(df=df_taxon_split)

        return df_updated
    
    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames the columns present in the original PR2 Scikit Taxon
        file, including Feature ID to seq_id, Taxon to VerbatimIdentification, 
        and Confidence to confidence_score.
        """
        df.rename(columns={self.PR2_SCIKIT_SEQ_COL: self.SEQ_ID, 
                           self.PR2_SCIKIT_CONFIDENCE_COL: self.CONFIDENCE_SCORE,
                           self.PR2_SCIKIT_TAXON_COL: self.VERBATIM_IDENTFICATION}, inplace=True)
        
        return df
    
    def _split_taxons_into_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Splits out the taxons in the Taxon column into their own columns.
        Must come after the Taxon column has been changed to verbatimIdentification.
        """
        split_df = df[self.VERBATIM_IDENTFICATION].str.split(';', expand=True)
        
        # select the required number of columns from the split result
        # This automatically discards any extra split columns and keeps
        # The NaNS for shorter rows
        num_cols_to_select = len(self.taxonomy_cols)
        result_cols = split_df.iloc[:, 0:num_cols_to_select]

        # Rename the selected columns using taxonomy_cols list
        result_cols.columns = self.taxonomy_cols

        # Concatenate the new columns back to the original DataFrame
        df = pd.concat([df, result_cols], axis=1)
        return df
         
    def _get_specific_epithet(self, species_value: str) -> str:
        """
        The name of the species specifically. Uses the species column.
        If '_X' or _sp in the species will be unclassified. If not species, 
        will be not appliable.
        """
        if pd.isna(species_value):
            return "not applicable"
        elif "endosymbiont" in species_value.lower():
            return "endosymbiont"
        elif "_X" in species_value:
            return "Unclassified"
        elif "_sp." in species_value:
            return "Unclassified"
        elif "aff_" in species_value:
            return f"Unclassified {' '. join(species_value.split('_')[1:])}"
        elif "clade1-sp." in species_value:
            return "Unclassified"
        elif len(species_value.split('_')) == 2:
            return species_value.split('_')[1]
        elif len(species_value.split('_')) == 3:
            if "clade" or "bravo" in species_value:
                return "Unclassified"
            else:
                return species_value.split('_')[1]
        else:
            print(f"\033[35mThere appear to be a weird species structure with value {species_value}.\033[0m")
            return "not applicable" 
    
    def _fill_constant_known_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fills columns that will have the same value. The percent_match, 
        percent_query_cover, and accession_id will all be missing: not provided. 
        the accession_id_ref_db will be PR2
        """
        df[[self.PERCENT_MATCH, self.PERCENT_QUERY_COVER, self.ACCESSION_ID]] = "missing: not provided"
        df[self.ACCESSION_ID_REF_DB] = self.PR2_SCIKIT_ACCESSION_ID_REF_DB

        return df