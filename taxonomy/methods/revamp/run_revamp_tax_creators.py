from taxonomy.modules.revamp_tax_table_creator import RevampTaxTableCreator

# Test REVAMP
taxonomy_creator = RevampTaxTableCreator(taxon_tble_file='/home/poseidon/mcallister/eDNA_bioinformatics/Run1/01_REVAMP/COI/ASV2Taxonomy/COI_asvTaxonomyTable.txt',
                                        asv_match_db_results_file='/home/poseidon/mcallister/eDNA_bioinformatics/Run1/01_REVAMP/COI/blast_results/ASV_blastn_nt_formatted.txt',
                                        asv_dna_seq_file='/home/poseidon/mcallister/eDNA_bioinformatics/Run1/01_REVAMP/COI/dada2/ASVs.fa',
                                        final_faire_excel_path='/home/poseidon/zalmanek/FAIRe-Mapping/taxonomy/data/output_faire.xlsx')

taxonomy_creator._create_percent_query_cov_dict()
taxonomy_creator.taxon_df.to_csv('/home/poseidon/zalmanek/FAIRe-Mapping/taxonomy/data/revamp_tax.csv')

