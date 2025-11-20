from taxonomy.modules.pr2_scikit_tax_table_creator import Pr2ScikitTaxTableCreator

taxonomy_creator = Pr2ScikitTaxTableCreator(taxon_tble_file="/home/poseidon/mcallister/eDNA_bioinformatics/Run1/03_pr2_v5.1_scikit/18Sv4/Run1_18Sv4_tax_pr2_sklearn.tsv",
                                            asv_dna_seq_file="/home/poseidon/mcallister/eDNA_bioinformatics/Run1/01_REVAMP/18Sv4/dada2/ASVs.fa",
                                            final_faire_excel_path="/home/poseidon/zalmanek/FAIRe-Mapping/taxonomy/data/18sv4_pr2_sckit/output_faire.xlsx"
                                            )

taxonomy_creator.taxon_df.to_csv('/home/poseidon/zalmanek/FAIRe-Mapping/taxonomy/data/18sv4_pr2_sckit/18sv4_pr2_sckit.csv')
taxonomy_creator.add_final_df_to_FAIRe_excel(data_df=taxonomy_creator.taxon_df, 
                                             template_path=taxonomy_creator.TAXA_FAIRE_TEMPLATE_PATH,
                                             output_path=taxonomy_creator.final_faire_excel_path)
