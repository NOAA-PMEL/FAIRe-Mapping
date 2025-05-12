## FAIRe sample fields that will have the same mapping as the other samples (e.g. won't be not applicable) for negative field controls
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
                    'prepped_samp_store_temp',
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
    '18S V9': 'ssu18sv9_amaralzettler',
    'dLoop': 'dloopv1.5v4_baker',
    '16S Kelley': 'lsu16s_2434-2571_kelly',
    'Kelly 16S': 'lsu16s_2434-2571_kelly',
    'Leray CO1': 'COI_1835-2198_lerayfolmer', #geller and folmer is same region
    '18S Machida': 'ssu18sv8_machida',
    'MiFish 12S': 'ssu12sv5v6_mifish_u_sales',
    '16S Furhman': 'ssu16sv4v5_parada',
    '16S Fuhrman': 'ssu16sv4v5_parada',
    '16S Furhman/Parada': 'ssu16sv4v5_parada',
    '18S V4': 'ssu18sv4_stoeck',
    'ITS': 'ITS1_sterling',
}

# Dictionary that maps the JV metabarcoding marker in the run sample metadata to the actual directory names on Poseidon
# TODO: Not sure if these will be consistent across runs - may need to build for each run. This works for JV run2, add others from other runs if they don't work for other runs
marker_to_shorthand_mapping = {
    'MiFish 12S': 'MiFish',
    '18S V4': '18Sv4',
    '18S V9': '18Sv9',
    'Leray CO1': 'COI',
    '16S Furhman': 'Parada16S',
    '16S Fuhrman': 'Parada16S',
    '16S Furhman/Parada': 'Parada16S',
    '18S Machida': 'Machida18S',
    'ITS': 'ITS1',
    'dLoop': 'dLoop',
    '16S Kelley': 'Kelly16S',
    'Kelly 16S': 'Kelly16S'
}

# marker shorthand to gblock name - based on this spreadsheet: https://docs.google.com/spreadsheets/d/1sdShcSBPvAcIWXBXJpCfjirv57FNjJFOfOYFd5_I3gM/edit?gid=0#gid=0
marker_shorthand_to_pos_cont_gblcok_name = {
    'MiFish': 'Haast_pos_MiFish',
    '18Sv4': 'Luti_pos_18Sv4',
    '18Sv9': 'Halo_pos_18Sv89',
    'COI': 'Moa_pos_CO1',
    'Parada16S': 'Camel_pos_Univ16S',
    'Machida18S': 'Halo_pos_18Sv89',
    'ITS1': 'LuV_pos_ITS1',
    'dLoop': 'Obre_pos_dLoop',
    'Kelly16S': 'Dodo_pos_16SKelly'
}
