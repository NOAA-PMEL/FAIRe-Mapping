from utils.taxonomy_table_creator import TaxonomyTableCreator

taxonomy_creator = TaxonomyTableCreator(taxon_tble_file='/home/poseidon/mcallister/eDNA_bioinformatics/Run1/01_REVAMP/COI/ASV2Taxonomy/COI_asvTaxonomyTable.txt',
                                        asv_dna_seq_file='/home/poseidon/mcallister/eDNA_bioinformatics/Run1/01_REVAMP/COI/dada2/ASVs.fa')

print(taxonomy_creator.taxon_df)