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
                    'nucl_acid_ext_method_additional', 
                    'expedition_id', 
                    'expedition_name',
                    'recordedBy', 
                    'samp_collect_notes',
                    'extract_id',
                    'extract_plate',
                    'extract_well_number',
                    'extract_well_position',
                    'dna_yield',
                    'dna_yield_unit'
                    ]


# standardized assay names to use 
# TODO: mappings will probably need to be added by run. These map from run2
marker_to_assay_mapping = {
    '18S V9': 'ssu18sv9_amaralzettler',
    '18Sv9': 'ssu18sv9_amaralzettler',
    'dLoop': 'dloopv1.5v4_baker',
    '16S Kelley': 'lsu16s_2434-2571_kelly',
    'Kelly16S': 'lsu16s_2434-2571_kelly',
    'Kelly 16S': 'lsu16s_2434-2571_kelly',
    'Leray CO1': 'COI_1835-2198_lerayfolmer', #geller and folmer is same region
    'COI': 'COI_1835-2198_lerayfolmer',
    '18S Machida': ['ssu18sv8_machida', 'ssu18sv8_machida_OSUmod'],
    'Machida18S': ['ssu18sv8_machida', 'ssu18sv8_machida_OSUmod'],
    'MiFish 12S': 'ssu12sv5v6_mifish_u_sales',
    'MiFish': 'ssu12sv5v6_mifish_u_sales',
    '16S Furhman': ['ssu16sv4v5_parada', 'ssu16sv4v5_parada_OSUmod'],
    '16S Fuhrman': ['ssu16sv4v5_parada', 'ssu16sv4v5_parada_OSUmod'],
    '16S Furhman/Parada': ['ssu16sv4v5_parada', 'ssu16sv4v5_parada_OSUmod'],
    'Parada16S': ['ssu16sv4v5_parada', 'ssu16sv4v5_parada_OSUmod'],
    '18S V4': 'ssu18sv4_stoeck',
    '18Sv4': 'ssu18sv4_stoeck',
    'ITS': 'ITS1_sterling',
    'ITS1': 'ITS1_sterling', 
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

# Mismatched samples metadata to raw data file matching
# this dictionary has correct sample names (key), and the wrong sample name strings that might exist for the names in the raw data files.
mismatch_sample_names_metadata_to_raw_data_files_dict = {
    '.DY20-12': '.DY20',
    'E2139.': 'E.2139.',
    'E687.WCOA21': 'E687',
    '.SKQ21-15S': '.SKQ2021',
    '.NO20-01': '.NO20',
    '.DY22-06': '.DY2206',
    '.DY22-09': '.DY2209',
    '.DY23-06': '.DY2306',
    '.M2-PPS-0423': '.DY2306',
    'E2084.CEO-AquaM-0923': 'E2084.SKQ23-12S',
    'E2090.CEO-AquaM-0923': 'E2090.SKQ23-12S',
    'E2097.CEO-AquaM-0923': 'E2097.SKQ23-12S',
    'E2030.NC.SKQ23-12S': 'E2030.NC'
}

# Dictionary with assay_name as keys and links to associated pcr library preparation bebops
project_pcr_library_prep_mapping_dict = {
    'ssu18sv9_amaralzettler': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'NOAA-PMEL-OME-AmaralZettler-phytoplankton-18S-V9-PCR-Protocol-BeBOP',
            'file_path': 'NOAA-PMEL-OME-AmaralZettler-phytoplankton-18S-V9-PCR-Protocol-BeBOP.md'
        },
        'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'Jonah-Ventures-Library-Preparation',
            'file_path': 'Jonah-Ventures-Library-NovaSeq-Preparation-BeBOP.md'
        }
    },
    'lsu16s_2434-2571_kelly': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'NOAA-PMEL-OME-Kelly-Metazoan-16S-PCR-Protocol-BeBOP-',
            'file_path': 'NOAA-PMEL-OME-Kelly-Metazoan-16S-PCR-Protocol-BeBOP.md'
        },
        'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'Jonah-Ventures-Library-Preparation',
            'file_path': 'Jonah-Ventures-Library-NovaSeq-Preparation-BeBOP.md'
        }
    },
    'COI_1835-2198_lerayfolmer': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'NOAA-PMEL-OME-LF-metazoan-COI-PCR-Protocol-BeBOP',
            'file_path': 'NOAA-PMEL-OME-LF-metazoan-COI-PCR-Protocol-BeBOP.md'
        },
        'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'Jonah-Ventures-Library-Preparation',
            'file_path': 'Jonah-Ventures-Library-NovaSeq-Preparation-BeBOP.md'
        }
    },
    'ssu18sv8_machida': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'NOAA-PMEL-OME-Machida-Metazoan-18S-V8-PCR-Protocol-BeBOP',
            'file_path': 'NOAA-PMEL-OME-Machida-Metazoan-18S-V8-PCR-Protocol-BeBOP.md'
        },
        'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'Jonah-Ventures-Library-Preparation',
            'file_path': 'Jonah-Ventures-Library-NovaSeq-Preparation-BeBOP.md'
        }
    },
    'ssu18sv8_machida_OSUmod': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'OSU-Machida-metazoan-18S-V8-PCR',
            'file_path': 'NOAA-PMEL-OME-OSU-Machida-Metazoan-18S-V8-PCR-Protocol-BeBOP.md'
        },
         'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'OSU-Library-Preparation-Sequencing',
            'file_path': 'OSU-Library-Preparation-Sequencing-BeBOP.md'
        }
    },
    'ssu12sv5v6_mifish_u_sales': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'NOAA-PMEL-OME-MiFish-mod-Universal-Teleost-12S-PCR-Protocol-BeBOP',
            'file_path': 'NOAA-PMEL-OME-MiFish-mod-Universal-Teleost-12S-PCR-Protocol-BeBOP.md'
        },
        'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'Jonah-Ventures-Library-Preparation',
            'file_path': 'Jonah-Ventures-Library-NovaSeq-Preparation-BeBOP.md'
        }
    },
    'ssu16sv4v5_parada': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'NOAA-PMEL-OME-Parada-universal-16S-PCR-Protocol-BeBOP',
            'file_path': 'NOAA-PMEL-OME-Parada-universal-16S-PCR-Protocol-BeBOP.md'
        },
        'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'Jonah-Ventures-Library-Preparation',
            'file_path': 'Jonah-Ventures-Library-NovaSeq-Preparation-BeBOP.md'
        }
    },
    'ssu16sv4v5_parada_OSUmod': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'OSU-Parada-universal-16S-PCR',
            'file_path': 'NOAA-PMEL-OME-OSU-Parada-universal-16S-PCR-Protocol-BeBOP copy.md'
        },
         'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'OSU-Library-Preparation-Sequencing',
            'file_path': 'OSU-Library-Preparation-Sequencing-BeBOP.md'
        }
    },
    'ssu18sv4_stoeck': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'NOAA-PMEL-OME-Stoeck-phytoplankton-18S-V4-PCR-Protocol-BeBOP',
            'file_path': 'NOAA-PMEL-OME-Stoeck-NCOG-phytoplankton-18S-V4-PCR-Protocol-BeBOP.md'
        },
        'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'Jonah-Ventures-Library-Preparation',
            'file_path': 'Jonah-Ventures-Library-NovaSeq-Preparation-BeBOP.md'
        }
    },
    'ITS1_sterling': {
        'pcr_bebop': {
            'owner': 'marinednadude',
            'repo': 'NOAA-PMEL-OME-WhiteSterling-phytoplankton-ITS1-PCR-Protocol-BeBOP',
            'file_path': 'NOAA-PMEL-OME-WhiteSterling-phytoplankton-ITS1-PCR-Protocol-BeBOP.md'
        },
        'library_bebop': {
            'owner': 'marinednadude',
            'repo': 'Jonah-Ventures-Library-Preparation',
            'file_path': 'Jonah-Ventures-Library-NovaSeq-Preparation-BeBOP.md'
        }
    }
}

# fAIRE columns that are int columns and will need to be converted before saving
faire_int_cols = [
                    'temp_WOCE_flag',
                    'ph_WOCE_flag',
                    'salinity_WOCE_flag',
                    'diss_inorg_carb_WOCE_flag',
                    'nitrite_WOCE_flag',
                    'ammonium_WOCE_flag',
                    'carbonate_WOCE_flag',
                    'phosphate_WOCE_flag',
                    'silicate_WOCE_flag',
                    'tot_alkalinity_WOCE_flag',
                    'ctd_cast_number',
                    'ctd_bottle_number',
                    'replicate_number',
                    'rosette_position',
                    'niskin_WOCE_flag',
                    'd18O_permil_WOCE_flag',
                    'nitrite_WOCE_flag',
                    'pool_dna_num',
                    'extract_well_number',
                    'habitat_natural_artificial_0_1',
                    'dna_cleanup_0_1'
                ]

# used in the experiment run metadata to update to desired cruise codes. key is old cruise code (in experiment run metadata sheet) and value is desired code
update_cruise_codes = {
    # '.DY2012': '.DY20-12',
    '.NO20': '.NO20-01',
    '.DY2206': '.DY22-06',
    '.DY2209': '.DY22-09',
    '.DY2306': '.DY23-06',
    '.SKQ2021': '.SKQ21-15S',
    '.DY20': '.DY20-12',
    '.DY2012': '.DY20-12',
    '.SKQ21': '.SKQ21-15S'   
}