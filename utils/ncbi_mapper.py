import pandas as pd
from .lists import faire_to_ncbi_units, ncbi_faire_to_ncbi_column_mappings_exact

# TODO: check on pos_cont_type and neg_cont_type, and sample_Title in ncbi sample mappings stuff?

class NCBIMapper:

    ncbi_organism = "marine metagenome"
    faire_missing_values = ["not applicable: control sample",
                            "not applicable: sample group",
                            "not applicable",
                            "missing: not collected: synthetic construct",
                            "missing: not collected: lab stock",
                            "missing: not collected: third party data",
                            "missing: not collected",
                            "missing: not provided",
                            "missing: restricted access: endangered species", 
                            "missing: restricted access: human-identifiable", 
                            "missing: restricted access"
                            ]

    def __init__(self, final_faire_sample_metadata_df: pd.DataFrame, 
                 final_faire_experiment_run_metadata_df: pd.DataFrame,
                 ncbi_sample_excel_template_path: str):
        # Initialize with the final faire_sample_metadata_df and final_faire_experiment_run_metadata_df
        # Can get these by initializing ProjectMapper and running process_sample_run_data method. Be sure to include config file
        # see run4/ncbi_mapping/ncbi_main.py for example

        self.faire_sample_df = self.clean_samp_df(df=final_faire_sample_metadata_df)
        self.faire_experiment_run_df = final_faire_experiment_run_metadata_df
        self.ncbi_sample_template_df = self.load_ncbi_template_as_df(file_path=ncbi_sample_excel_template_path, sheet_name='MIMARKS.survey.water.6.0', header=11)
    
    def create_ncbi_sample_submission(self):

        sample_df_updated_with_unit_cols = self.update_with_ncbi_unit_cols()
        print(sample_df_updated_with_unit_cols)
    
    def load_ncbi_template_as_df(self, file_path: str, sheet_name: str, header: int) -> pd.DataFrame:
        # Load FAIRe excel template as a data frame based on the specified template sheet name
        
        return pd.read_excel(file_path, sheet_name=sheet_name, header=header)
    
    def clean_samp_df(self, df:pd.DataFrame):
        # removes all FAIRe missing values and returns empty values
        # And removes all rows that are not 'sample' for samp_category (so removed positive and negative controls)
        
        df_clean = df.replace(self.faire_missing_values, '')
        df_filtered = df_clean[df_clean['samp_category'] == 'sample']
        return df_filtered
    
    def update_with_ncbi_unit_cols(self):
        

        updated_df = pd.DataFrame()

        # First handle unit transormations
        for ncbi_col_name, faire_mapping in faire_to_ncbi_units.items():
            # Get FAIRe column mame
            faire_col = faire_mapping['faire_col']

            if faire_col not in self.faire_sample_df:
                continue
            else:
                # Check if we have a constant unit or a unit column
                if 'constant_unit_val' in faire_mapping:
                    unit_val = faire_mapping['constant_unit_val']
                    updated_df[ncbi_col_name] = (self.faire_sample_df[faire_col].astype(str) + ' ' + unit_val).where(self.faire_sample_df[faire_col].astype(str).str.strip() != '', '')
                elif 'faire_unit_col' in faire_mapping:
                    unit_col = faire_mapping['faire_unit_col']
                    updated_df[ncbi_col_name] = self.faire_sample_df[faire_col].astype(str) + ' ' + self.faire_sample_df[unit_col].astype(str).where(self.faire_sample_df[faire_col].astype(str).str.strip() != '', '')
                else:
                    updated_df[ncbi_col_name] = self.faire_sample_df[faire_col]

        # Second handle direct column mappings
        for old_col_name, new_col_name in ncbi_faire_to_ncbi_column_mappings_exact.items():
            if old_col_name in self.faire_sample_df:
                updated_df[new_col_name] = self.faire_sample_df[old_col_name]

        # Third handle logic cases (depth and lat/lon)
        updated_df['*depth'] = self.faire_sample_df.apply(
                lambda row: self.get_ncbi_depth(metadata_row=row),
                axis=1
            )
        updated_df['*lat_lon'] = self.faire_sample_df.apply(
            lambda row: self.get_ncbi_lat_lon(metadata_row=row),
            axis=1
        )
            
        return updated_df

    def get_ncbi_depth(self, metadata_row: pd.Series) -> str:
        # Get the ncbi formatted value for depth, which is the interval of minimumDepthInMeters - maximumDepthInMeters
        min_depth = metadata_row['minimumDepthInMeters']
        max_depth = metadata_row['maximumDepthInMeters']

        # If min_depth and max_depth are not the same, report as interval
        if min_depth != max_depth and max_depth != '':
            ncbi_depth = min_depth + ' ' + 'm' + ' - ' + max_depth + ' ' + 'm'
        # if min_depth and max_depth are the same, report just one because they will be the same
        elif min_depth == max_depth and max_depth != '':
            ncbi_depth = min_depth + ' ' + 'm'
        else:
            raise ValueError(f"Something wrong with the depth. Min depth is {min_depth} and max_depth is {max_depth}. {metadata_row}")

        return ncbi_depth

    def get_ncbi_lat_lon(self, metadata_row: pd.Series) -> str:
        lat = float(metadata_row['decimalLatitude'])
        lon = float(metadata_row['decimalLongitude'])

        lat_dir = 'N' if lat >= 0 else 'S'
        lon_dir = 'E' if lon >= 0 else 'W'

        # Take absolute values
        lat_abs = abs(lat)
        lon_abs = abs (lon)

        lat_formatted = f"{lat_abs:.4f} {lat_dir}"
        lon_formatted = f"{lon_abs:.4f} {lon_dir}"

        ncbi_lat_lon = f"{lat_formatted} {lon_formatted}"

        return ncbi_lat_lon