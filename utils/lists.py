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
    '18S Machida': ['ssu18sv8_machida', 'ssu18sv8_machida_OSUmod'],
    'MiFish 12S': 'ssu12sv5v6_mifish_u_sales',
    '16S Furhman': ['ssu16sv4v5_parada', 'ssu16sv4v5_parada_OSUmod'],
    '16S Fuhrman': ['ssu16sv4v5_parada', 'ssu16sv4v5_parada_OSUmod'],
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

# Mismatched samples metadata to raw data file matching
# this dictionary has correct sample names (key), and the wrong sample name strings that might exist for the names in the raw data files
mismatch_sample_names_metadata_to_raw_data_files_dict = {
    '.DY2012': '.DY20',
    'E265.1B.NO20': 'E265.IB.NO20',
    'E2139.': 'E.2139.',
    'E687.WCOA21': 'E687'

}

# NCBI sample column name as key and faire columns ask nested values with units
faire_to_ncbi_units = {
    "alkalinity": {
        "faire_col": "tot_alkalinity",
        "faire_unit_col": "tot_alkalinity_unit"
    },
    "ammonium": {
        "faire_col": "ammonium",
        "faire_unit_col": "ammonium_unit"
    },
    "chlorophyll": {
        "faire_col": "chlorophyll",
        "constant_unit_val": "mg/m3"
    },
    "density": {
        "faire_col": "density",
        "faire_unit_col": "density_unit"
    },
    "diss_inorg_carb": {
        "faire_col": "diss_inorg_carb",
        "faire_unit_col": "diss_inorg_carb_unit"
    },
    "diss_inorg_nitro": {
        "faire_col": "diss_inorg_nitro",
        "faire_unit_col": "diss_inorg_nitro_unit"
    },
    "diss_org_carb": {
        "faire_col": "diss_org_carb",
        "faire_unit_col": "diss_org_carb_unit"
    },
    "diss_org_nitro": {
        "faire_col": "diss_org_nitro",
        "faire_unit_col": "diss_org_nitro_unit"
    },
    "diss_oxygen": {
        "faire_col": "diss_oxygen",
        "faire_unit_col": "diss_oxygen_unit"
    },
    "down_par": { # This is a user defined field that we added
        "faire_col": "par",
        "faire_unit_col": "par_unit"
    },
    "elev": {
        "faire_col": "elev",
        "constant_unit_val": "m"
    },
    "light_intensity": {
        "faire_col": "light_intensity",
        "constant_unit_val": "lux"
    },
    "nitrate": {
        "faire_col": "nitrate",
        "faire_unit_col": "nitrate_unit"
    },
    "nitrite": {
        "faire_col": "nitrite",
        "faire_unit_col": "nitrite_unit"
    },
    "nitro": {
        "faire_col": "nitro",
        "faire_unit_col": "nitro_unit"
    },
    "org_carb": {
        "faire_col": "org_carb",
        "faire_unit_col": "org_carb_unit"
    },
    "org_matter": {
        "faire_col": "org_matter",
        "faire_unit_col": "org_matter_unit"
    },
    "org_nitro": {
        "faire_col": "org_nitro",
        "faire_unit_col": "org_nitro_unit"
    },
    "part_org_carb": {
        "faire_col": "part_org_carb",
        "faire_unit_col": "part_org_carb_unit"
    },
    "part_org_nitro": {
        "faire_col": "part_org_nitro",
        "faire_unit_col": "part_org_nitro_unit"
    },
    "ph": {
        "faire_col": "ph",
    },
    "phosphate": { #user defined we added
        "faire_col": "phosphate",
        "faire_unit_col": "phosphate_unit"
    },
    "pressure": {
        "faire_col": "pressure",
        "faire_unit_col": "pressure_unit"
    },
    "salinity": {
        "faire_col": "salinity",
        "constant_unit_val": "psu"
    },
    "samp_size": {
        "faire_col": "samp_size",
        "faire_unit_col": "samp_size_unit"
    },
    "samp_store_temp": {
        "faire_col": "samp_store_temp",
        "constant_unit_val": "C"
    },
    "samp_vol_we_dna_ext": {
        "faire_col": "samp_vol_we_dna_ext",
        "faire_unit_col": "samp_vol_we_dna_ext_unit"
    },
    "silicate": { #user defined we added
        "faire_col": "silicate",
        "faire_unit_col": "silicate_unit"
    },
    "size_frac": {
        "faire_col": "size_frac",
        "constant_unit_val": "µm"
    },
    "size_frac_low": {
        "faire_col": "size_frac_low",
        "constant_unit_val": "µm"
    },
    "suspend_part_matter": {
        "faire_col": "suspend_part_matter",
        "constant_unit_val": "mg/L"
    },
    "temp": {
        "faire_col": "temp",
        "constant_unit_val": "C"
    },
    "tot_depth_water_col": {
        "faire_col": "tot_depth_water_col",
        "constant_unit_val": "m"
    },
    "tot_diss_nitro": {
        "faire_col": "tot_diss_nitro",
        "faire_unit_col": "tot_diss_nitro_unit"
    },
    "tot_inorg_nitro": {
        "faire_col": "tot_inorg_nitro",
        "faire_unit_col": "tot_inorg_nitro_unit"
    },
    "tot_nitro": {
        "faire_col": "tot_nitro",
        "faire_unit_col": "tot_nitro_unit"
    },
    "tot_part_carb": {
        "faire_col": "tot_part_carb",
        "faire_unit_col": "tot_part_carb_unit"
    },
    "turbidity": {
        "faire_col": "turbidity",
        "constant_unit_val": "ntu"
    },
    "water_current": {
        "faire_col": "water_current",
        "constant_unit_val": "m/s"
    }
}

 # The columns of faire as keys and the columns of ncbi as values (if they map exactly)
ncbi_faire_to_ncbi_column_mappings_exact = {
    "samp_name": "*sample_name",
    "eventDate": "*collection_date",
    "env_broad_scale": "*env_broad_scale",
    "env_local_scale": "*env_local_scale",
    "env_medium": "*env_medium", 
    "geo_loc_name": '*geo_loc_name',
    "collection_method": "samp_collect_method",
    "samp_collect_device": "samp_collect_device",
    "samp_mat_process": "samp_mat_process",
    "samp_store_dur": "samp_store_dur",
    "samp_store_loc": "samp_store_loc",
    "source_material_id": "materialSampleID",
    "tidal_stage": "tidal_stage"
}

# For faire_column as keys and ncbit sra columns as values
ncbi_faire_sra_column_mappings_exact = {
    "samp_name": "sample_name",
    "lib_id": "library_ID",
    "filename": "filename",
    "filename2": "filename2"
}
