## FAIRe sample fields that will have the same mapping as the other samples (e.g. won't be not applicable)

nc_faire_field_cols = ['samp_name',
                    'samp_category',
                    'neg_cont_type',
                    'verbatimEventDate',
                    'verbatimEventTime',
                    'eventDate',
                    'eventDurationValue',
                    'habitat_natural_artificial_0_1',  # will be 1 for NC's
                    'samp_collect_method',
                    'samp_size',
                    'samp_size_unit',
                    'samp_store_sol',
                    'samp_store_method_additional',
                    'samp_mat_process', # remove niskin info from it
                    'filter_passive_active_0_1',
                    'pump_flow_rate',
                    'pump_flow_rate_unit',
                    'prefilter_material',
                    'size_frac_low',
                    'size_frac',
                    'filter_diameter',
                    'filter_surface_area',
                    'filter_material',
                    'filter_name',
                    'precip_chem_prep',
                    'precip_force_prep',
                    'precip_time_prep',
                    'precip_temp_prep',
                    'prepped_samp_store_sol',
                    'prepped_samp_store_dur',
                    'prep_method_additional',
                    'assay_name',
                    'date_ext',
                    'samp_vol_we_dna_ext',
                    'samp_vol_we_dna_ext_unit',
                    'nucl_acid_ext_lysis',
                    'nucl_acid_ext_sep',
                    'nucl_acid_ext',
                    'nucl_acid_ext_kit',
                    'nucl_acid_ext_modify',
                    'dna_cleanup_0_1',
                    'dna_cleanup_method',
                    'concentration',
                    'concentration_unit',
                    'concentration_method',
                    'ratioOfAbsorbance260_280',
                    'pool_dna_num',
                    'nucl_acid_ext_method_additional'
                    ]


# standardized assay names to use 
# TODO: mappings will probably need to be added by run. These map from run2
marker_to_assay_mapping = {
    '18S V9': 'AmaralZettler_phytoplankton_18S_V9',
    'dLoop': 'Baker_marmam_dLoop',
    '16S Kelley': 'Kelly_metazoan_mt16S',
    'Leray CO1': 'LerayFolmer_metazoan_COI', #geller and folmer is same region
    '18S Machida': 'Machida_metazoan_18S_V8',
    'MiFish 12S': 'MiFish_UniversalTeleost_12S',
    '16S Furhman': 'Parada_universal_SSU16S_V4',
    '18S V4': 'Stoeck_phytoplankton_18S_V4',
    'ITS': 'WhiteSterling_phytoplankton_ITS1',
}

# Dictionary that maps the JV metabarcoding marker in the run sample metadata to the actual directory names on Poseidon
# TODO: Not sure if these will be consistent across runs - may need to build for each run. This works for JV run2, add others from other runs if they don't work for other runs
marker_to_shorthand_mapping = {
    'MiFish 12S': 'MiFish',
    '18S V4': '18Sv4',
    '18S V9': '18Sv9',
    'Leray CO1': 'COI',
    '16S Furhman': 'Parada16S',
    '18S Machida': 'Machida18S',
    'ITS': 'ITS1',
    'dLoop': 'dLoop',
    '16S Kelley': 'Kelly16S' 
}
