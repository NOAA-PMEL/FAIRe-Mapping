from .faire_mapper import OmeFaireMapper
from pathlib import Path
from datetime import datetime
import isodate
import pandas as pd
import yaml
import requests
import numpy as np
import xarray as xr
# import matplotlib.pyplot as plt
# from pyproj import Transformer
from bs4 import BeautifulSoup
from .custom_exception import NoInsdcGeoLocError
from .lists import nc_faire_field_cols, marker_shorthand_to_pos_cont_gblcok_name

# TODO: Need to figure out how to incorporate blanks from extractions into data frame. If they are in the same extraction set as the samples
# with the shorthand cruise nmae, need to include as extraction blank (see Alaska extractions for examples)
# TODO: Turn nucl_acid_ext for DY20/12 into a BeBOP and change in extraction spreadsheet. Link to spreadsheet: https://docs.google.com/spreadsheets/d/1iY7Z8pNsKXHqsp6CsfjvKn2evXUPDYM2U3CVRGKUtX8/edit?gid=0#gid=0
# TODO: continue update pos_df - add not applicable: sample to user defined fields, also add pos_cont_type

class FaireSampleMetadataMapper(OmeFaireMapper):

    sample_mapping_sheet_name = "sampleMetadata"
    extraction_mapping_sheet_name = "extractionMetadata"
    replicate_parent_sample_metadata_col = "replicate_parent"
    faire_lat_col_name = "decimalLatitude"
    faire_lon_col_name = "verbatimLongitude"
    not_applicable_to_samp_faire_col_dict = {"neg_cont_type": "not applicable: sample group",
                                             "pos_cont_type": "not applicable: sample group"}
    gebco_file = "/home/poseidon/zalmanek/FAIRe-Mapping/utils/GEBCO_2024.nc"
    

    def __init__(self, config_yaml: yaml):
        #TODO: used to have exp_metadata_df: pd.Series as init, but removed because of abstracting out sequencing yaml. See all associated commented out portions
        # May need to move this part into a separate class that combines after all sample_metadata is generated for each cruise
        super().__init__(config_yaml)

        self.sample_metadata_sample_name_column = self.config_file['sample_metadata_sample_name_column']
        self.sample_metadata_file_neg_control_col_name = self.config_file['sample_metadata_file_neg_control_col_name']
        self.sample_metadata_cast_no_col_name = self.config_file['sample_metadata_cast_no_col_name']
        self.sample_metadata_bottle_no_col_name = self.config_file['sample_metadata_bottle_no_col_name']
        self.extraction_sample_name_col = self.config_file['extraction_sample_name_col']
        self.extraction_cruise_key = self.config_file['extraction_cruise_key']
        self.extraction_conc_col_name = self.config_file['extraction_conc_col_name']
        self.extraction_date_col_name = self.config_file['extraction_date_col_name']
        self.nc_samp_mat_process = self.config_file['nc_samp_mat_process']
        self.extraction_metadata_sheet_name = self.config_file['extraction_metadata_sheet_name']
        self.extraction_metadata_google_sheet_id = self.config_file['extraction_metadata_google_sheet_id']
        self.vessel_name = self.config_file['vessel_name']
        self.faire_template_file = self.config_file['faire_template_file']
        self.google_sheet_mapping_file_id = self.config_file['google_sheet_mapping_file_id']

        # self.exp_metadata_df = exp_metadata_df

        self.sample_metadata_df = self.filter_metadata_dfs()[0]
        self.nc_df = self.filter_metadata_dfs()[1]
        self.sample_faire_template_df = self.load_faire_template_as_df(file_path=self.config_file['faire_template_file'], sheet_name=self.sample_mapping_sheet_name, header=self.faire_sheet_header).dropna()
        self.replicates_dict = self.create_biological_replicates_dict()
        self.insdc_locations = self.extract_insdc_geographic_locations()
        self.mapping_dict = self.create_sample_mapping_dict()
        self.nc_mapping_dict = self.create_nc_mapping_dict()
        # self.sample_assay_dict = self.create_assay_name_dict()

    def create_sample_mapping_dict(self) -> dict:
            # creates a mapping dictionary and saves as self.mapping_dict

            # First concat sample_mapping_df with extractions_mapping_df
            sample_mapping_df = self.load_google_sheet_as_df(google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.sample_mapping_sheet_name, header=0)
            extractions_mapping_df = self.load_google_sheet_as_df(google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.extraction_mapping_sheet_name, header=1)
            mapping_df = pd.concat([sample_mapping_df, extractions_mapping_df])

            # Group by the mapping type
            group_by_mapping = mapping_df.groupby(self.mapping_file_mapped_type_column)

            # Create nested dictionary {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
            mapping_dict = {}
            for mapping_value, group in group_by_mapping:
                column_map_dict = {k: v for k, v in zip(group[self.mapping_file_FAIRe_column], group[self.mapping_file_metadata_column]) if pd.notna(v)}
                mapping_dict[mapping_value] = column_map_dict
    
            return mapping_dict
    
    def create_nc_mapping_dict(self) -> dict:
        
        nc_mapping_dict = {}
        for mapping_type, col_dict in self.mapping_dict.items():
            if isinstance(col_dict, dict):
                filtered_nested = {k: v for k, v in col_dict.items() if k in nc_faire_field_cols}
                nc_mapping_dict[mapping_type] = filtered_nested

        # change values that will be differenct for NC's
        nc_mapping_dict[self.constant_mapping]['habitat_natural_artificial_0_1'] = '1'
        nc_mapping_dict[self.constant_mapping]['samp_mat_process'] = self.nc_samp_mat_process
        nc_mapping_dict[self.related_mapping]['prepped_samp_store_dur'] = self.config_file['nc_prepped_samp_store_dur']
        nc_mapping_dict[self.constant_mapping]['samp_store_dur'] = "not applicable: control sample"
        nc_mapping_dict[self.constant_mapping]['samp_store_temp'] = 'ambient temperature'
        nc_mapping_dict[self.constant_mapping]['samp_store_loc'] = self.vessel_name

        return nc_mapping_dict

    # def create_assay_name_dict(self) -> dict:
    #     # Creates a dicitonary of the samples and their assays
    #     grouped = self.exp_metadata_df.groupby('samp_name')['assay_name'].apply(list).to_dict()
    #     return grouped

    def convert_mdy_date_to_iso8061(self, date_string: str) -> str:
        # converts from m/d/y to iso8061

        date_string = str(date_string).strip()

        # Handle both single and double digit formats
        try:
            parts = date_string.split('/')
            if len(parts) == 3:
                month, day, year = parts

                formatted_date = f"{int(month):02d}/{int(day):02}/{year}"

                # parse date string
                date_obj = datetime.strptime(formatted_date, "%m/%d/%Y")

                # convert to iso 8601 format
                return date_obj.strftime('%Y-%m-%d')

            else:
                raise ValueError(f"Date doesnt have three parts: {date_string}")
        
        except Exception as e:
            print(f"Error converting {date_string}: {str(e)}")

    def extraction_avg_aggregation(self, extractions_df: pd.DataFrame):
        # For extractions, calculates the mean if more than one concentration per sample name.

        # Keep Below Range
        if all(isinstance(conc, str) and ("below range" in conc.lower() or "br" == conc.lower()) for conc in extractions_df):
            return "BR"
        
        # For everything else, convert to numeric and calculate mean
        numeric_series = pd.to_numeric(extractions_df, errors='coerce') #Non numeric becomes NaN

        return numeric_series.mean()
    
    def filter_cruise_avg_extraction_conc(self) -> pd.DataFrame:
        # If extractions have multiple measurements for extraction concentrations, calculates the avg.
        # and creates a column called pool_num to show the number of samples pooled

        extractions_df = self.load_google_sheet_as_df(google_sheet_id=self.extraction_metadata_google_sheet_id, sheet_name=self.extraction_metadata_sheet_name, header=0)

        # Filter extractions df by cruise and calculate avg concentration
        extract_avg_df = extractions_df[extractions_df[self.extraction_sample_name_col].str.contains(self.extraction_cruise_key)].groupby(
            self.extraction_sample_name_col).agg({
                self.extraction_conc_col_name:self.extraction_avg_aggregation,
                **{col: 'first' for col in extractions_df.columns if col != self.extraction_sample_name_col and col != self.extraction_conc_col_name}
            }).reset_index()
            
        # Add pool_num column that shows how many samples were averaged for each group
        sample_counts = extractions_df[extractions_df[self.extraction_sample_name_col].str.contains(self.extraction_cruise_key)].groupby(
            self.extraction_sample_name_col).size().reset_index(name='pool_num')
        
        # merge sample_counts into dataframe
        extract_avg_df = extract_avg_df.merge(sample_counts, on=self.extraction_sample_name_col, how='left')

        # Add extraction_method_additional for samples that pooled more than one extract
        extract_avg_df['extraction_method_additional'] = extract_avg_df['pool_num'].apply(
            lambda x: "One sample, but two filters were used because sample clogged. Two extractions were pooled together and average concentration calculated." if x>1 else "missing: not provided")
        
        # update samp name for DY2012 cruises (from DY20) and remove E numbers from any NC samples
        extract_avg_df[self.extraction_sample_name_col] = extract_avg_df[self.extraction_sample_name_col].apply(self.str_replace_dy2012_cruise)
        extract_avg_df[self.extraction_sample_name_col] = extract_avg_df[self.extraction_sample_name_col].apply(self.str_replace_nc_samps_with_E)

        #update dates to iso8601 TODO: may need to adjust this for ones that are already in this format
        extract_avg_df[self.extraction_date_col_name] = extract_avg_df[self.extraction_date_col_name].apply(self.convert_mdy_date_to_iso8061)
        
        return extract_avg_df
    
    def transform_metadata_df(self):
        # Converts sample metadata to a data frame and checks to make sure NC samples have NC in the name (dy2206 had this problem)
        samp_metadata_df = self.load_csv_as_df(file_path=Path(self.config_file['sample_metadata_file']))

        # Add .NC to sample name if the Negative Control column is True and its missing (in DY2206 cruise)
        samp_metadata_df[self.sample_metadata_sample_name_column] = samp_metadata_df.apply(
            lambda row: self.check_nc_samp_name_has_nc(metadata_row=row),
            axis=1)
        
        return samp_metadata_df

    def join_sample_and_extract_df(self):
        # join extraction sheet with sample metadata sheet on Sample name - keeping only samples from extraction df

        extract_df = self.filter_cruise_avg_extraction_conc()

        samp_df = self.transform_metadata_df()

        metadata_df = pd.merge(
            left = extract_df,
            right = samp_df,
            left_on = self.extraction_sample_name_col,
            right_on = self.sample_metadata_sample_name_column,
            how='left'
        )

        return metadata_df

    def check_nc_samp_name_has_nc(self, metadata_row: pd.Series) -> str:
        # Checks to make sure sample names have .NC if the negative control column is True, if not adds the .NC
        sample_name = metadata_row[self.sample_metadata_sample_name_column]
        if metadata_row[self.sample_metadata_file_neg_control_col_name] == 'TRUE' or metadata_row[self.sample_metadata_file_neg_control_col_name] == True:
            if '.NC' not in metadata_row[self.sample_metadata_sample_name_column]:
                samp_name_bits = sample_name.split('.')
                samp_name = f'{samp_name_bits[0]}.NC.{samp_name_bits[1]}'
                return samp_name
            else:
                return sample_name
        else:
            return sample_name

    def filter_metadata_dfs(self):

        # Extract all unique sample names from experiment run metadata df
        # exp_sample_names = self.exp_metadata_df['samp_name'].unique()

        # Join sample metadata with extraction metadata to get samp_df
        samp_df = self.join_sample_and_extract_df()

        # filter samp_df to keep only rows with sample names that exist in the exp_sample_names
        # samp_df_filtered = samp_df[samp_df[self.sample_metadata_sample_name_column].isin(exp_sample_names)]
        
        try:
            nc_mask = samp_df[self.sample_metadata_sample_name_column].astype(str).str.contains('.NC', case=True)
            nc_df = samp_df[nc_mask].copy()
            samp_df_filtered = samp_df[~nc_mask].copy()
            return samp_df_filtered, nc_df
        except:
            print("Looks like there are no negatives in the sample df, returning an empty nc_df")
            nc_df = pd.DataFrame()
            return samp_df_filtered, nc_df
    
    def extract_insdc_geographic_locations(self) -> list:

        url = 'https://www.insdc.org/submitting-standards/geo_loc_name-qualifier-vocabulary/'
        
        response = requests.get(url)
        response.raise_for_status() # Raise an exception for HTTP errors
        html_content = response.text
        
        soup = BeautifulSoup(html_content, 'html.parser')

        # Get all elements that fall under the class name
        elements = soup.find_all(class_='wp-block-list has-large-font-size')

        # extract all elements with 'li' in their tag and get the text
        locations = []
        for element in elements:
            li_elements = element.find_all('li', recursive=True)
            for li in li_elements:
                locations.append(li.get_text(strip=True))

        return locations
    
    def extract_replicate_sample_parent(self, sample_name):
            # Extracts the E number in the sample name
            if pd.notna(sample_name):
                return sample_name.split('.')[0]
    
    def create_biological_replicates_dict(self) -> dict:
        # Creates a dictionary of the parent E number as a key and the replicate sample names as the values
        # e.g. {'E26': ['E26.2B.DY2012', 'E26.1B.NC.DY2012']}

        # Extract the parent E number and add to column called replicate_parent
        # Uses set() to remove any technical replicates (they will have the same)
        self.sample_metadata_df[self.replicate_parent_sample_metadata_col] = self.sample_metadata_df[self.sample_metadata_sample_name_column].apply(self.extract_replicate_sample_parent)
        # Group by replicate parent
        replicate_dict = self.sample_metadata_df.groupby(self.replicate_parent_sample_metadata_col)[self.sample_metadata_sample_name_column].apply(set).to_dict()
        # remove any key, value pairs where there aren't replicates and convert back to list
        replicate_dict = {replicate_parent: list(set(sample_name)) for replicate_parent, sample_name in replicate_dict.items() if len(sample_name) > 1}

        return replicate_dict
    
    def add_biological_replicates(self, metadata_row: pd.Series, faire_missing_val: str) -> dict:

        if self.replicates_dict.get(metadata_row.get(self.replicate_parent_sample_metadata_col)):
            replicates = ' | '.join(self.replicates_dict.get(metadata_row[self.replicate_parent_sample_metadata_col], None))
            return replicates
        else:
            return faire_missing_val
        
    def add_neg_cont_type(self, samp_name: str) -> dict:
        # Adds the negative control type to the neg_cont_type column of the FAIRe template based on strings in the sample name
        # TODO: Add error if it snot an field negative or extraction negative

        field_neg_control_str = '.NC'
        extraction_neg_control_str = 'blank'

        if field_neg_control_str in samp_name:
            neg_cont_type = self.check_cv_word(value='field negative', faire_attribute='neg_cont_type')
        elif extraction_neg_control_str in samp_name.lower():
            neg_cont_type = self.check_cv_word(value='extraction negative', faire_attribute='neg_cont_type')

        return neg_cont_type
    
    def add_pos_cont_type(self, pos_cont_sample_name:str) -> str:
        
        # Get the marker from the positive control sample name (e.g. run2.ITS1.POSITIVE - this will give you ITS1)
        shorthand_marker = pos_cont_sample_name.split('.')[1]

        g_block = marker_shorthand_to_pos_cont_gblcok_name.get(shorthand_marker)

        pos_cont_type = f"PCR positive of synthetic DNA. Gblock name: {g_block}"

        return pos_cont_type

    def add_samp_category_by_sample_name(self, metadata_row: pd.Series, faire_col: str, metadata_col: str) -> dict:
        # TODO: double check logic for negative controls: blanks = extraction negatives, .NC = field negatives. Waiting on Zack and Sean's run metadata?
        # TODO: create add_pos_con_type for positive controls (same as neg_cont_type is handled)
        # Adds the FAIRe samp_category based on strings in the Sample Name and the Negative Control column
        
        pos_control_str = 'POSITIVE'

        # If Negative Control column of the metadata is True. Try because metadata_file_neg_control_col_name will not be present if positive control
        try:
            if metadata_row[self.sample_metadata_file_neg_control_col_name]:
                return self.check_cv_word(value='negative control', faire_attribute=faire_col)
            else:
                return self.check_cv_word(value='sample', faire_attribute=faire_col)
        except:
            if pos_control_str in metadata_row[metadata_col]:
                return self.check_cv_word(value='positive control', faire_attribute=faire_col)
        
    def add_material_sample_id(self, metadata_row) -> str:
        # Formats MaterialSampleID to be numerical (discussed with Sean) - double check if this needs to be updated for three digit cast numbers 
        # can also be used for sample_derived_from if no other in between parent samples
     
        cast_int = int(metadata_row.get(self.sample_metadata_cast_no_col_name))
        btl_int = int(metadata_row.get(self.sample_metadata_bottle_no_col_name))
        
        formatted_cast = f'{cast_int:02d}'
        formatted_btl = f'{btl_int:02d}'

        material_sample_id = formatted_cast + formatted_btl

        return material_sample_id

    def add_rel_cont_id(self, metadata_row: pd.Series, samp_seq_pos_controls_dict: dict) -> str:
        # Adds the rel_cont_id
        sample_name = metadata_row[self.sample_metadata_sample_name_column]
        
        # Get positive controls
        control_samps = samp_seq_pos_controls_dict.get(sample_name)

        # Get field negative sample names in a list and add to associated_seq_pos
        if not self.nc_df.empty:
            nc_samples = self.nc_df[self.sample_metadata_sample_name_column].tolist()
            control_samps.extend(nc_samples)

        # join with | 
        rel_cont_id = ' | '.join(control_samps)

        return rel_cont_id 

    def add_assay_name(self, sample_name: str) -> str:
        # Uses the sample name to look up assays from the sample_assay_dict (generated from the exp_run_metadata_df)
        assays = self.sample_assay_dict.get(sample_name)
        assays_formatted = ' | '.join(assays)
        return assays_formatted

    def convert_wind_degrees_to_direction(self, degree_metadata_row: pd.Series) -> str:
        # converts wind direction  to cardinal directions
        
        if pd.isna(degree_metadata_row) or degree_metadata_row is None:
            return "missing: not collected"
        else:
            direction_labels = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
            ix  = np.round(degree_metadata_row / (360. / len(direction_labels))).astype('i')

            return direction_labels[ix % 16]
    
    def convert_min_depth_from_minus_one_meter(self, metadata_row: pd.Series, max_depth_col_name: str):
        # Subtracts 1 from the max depth to calculate min depth (niskin bottle is ~1 m)
        max_depth = float(metadata_row[max_depth_col_name])
        if max_depth > 0:
            min_depth = max_depth - 1
        else:
            min_depth = max_depth
        return min_depth
        
    def format_geo_loc(self, metadata_row: str, geo_loc_metadata_col: str) -> dict:
        # TODO: add if statement for Arctic OCean? SKQ21-12S?
     
        # For use cases that have the sea in the 
        if 'sea' in metadata_row[geo_loc_metadata_col].lower():
            # Typos in geo_loc for dy2206
            if 'Beiring' in metadata_row[geo_loc_metadata_col]:
                sea = metadata_row[geo_loc_metadata_col].replace('Beiring', 'Bering')
            else:
                sea = metadata_row[geo_loc_metadata_col]
            geo_loc = 'USA: ' + sea
        else: geo_loc = metadata_row[geo_loc_metadata_col]
    
        # check that geo_loc_name (first string before first :) is an acceptes insdc word
        if ':' in geo_loc:
            geo_loc_name = geo_loc.split(':')[0]
        # accounts for ones that did not have 'sea' in their name, thus no USA in front - e.g. Arctic Ocean
        else:
            geo_loc_name = metadata_row[geo_loc_metadata_col]
        if geo_loc_name not in self.insdc_locations:
            raise NoInsdcGeoLocError(f'There is no geographic location in INSDC that matches {metadata_row[geo_loc_metadata_col]}, check sea_name and try again')
    
        return geo_loc
    
    def calculate_env_local_scale(self, depth: float) -> str:
        # uses the depth to assign env_local_scale
        aphotic = "marine aphotic zone [ENVO:00000210]"
        photic = "marine photic zone [ENVO:00000209]"

        if depth <= 200:
            env_local_scale = photic
        elif depth > 200:
            env_local_scale = aphotic

        return env_local_scale
    
    def format_dates_for_duration_calculation(self, date: str) -> datetime:
        # Handle different date formats and timezone indicators (used with calculate_date_duration function)

        if 'T' in date:
            if 'Z' in date:
                # Convert 'Z' timezone to UTC offset format
                date = date.replace('Z', '+00:00')
                # Create datetime and strip timezone info
                dt = datetime.fromisoformat(date).replace(tzinfo=None)
        # To account for dates in form 9/5/20 - may need to refactor
        elif '/' in date and ' ' in date: #from like 2022/01/03 12:34:54
            dt = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
        elif '/' in date:
            date = datetime.strptime(date, "%m/%d/%y")
            date = date.strftime("%Y-%m-%d")
            dt = datetime.fromisoformat(date)
        else:
            dt = datetime.fromisoformat(date)

        return dt
    
    def calculate_date_duration(self, metadata_row: pd.Series, start_date_col: str, end_date_col: str) -> datetime:
        # takes two dates and calcualtes the difference to find the duration of time in ISO 8601 format
        # Handles both simple date format (2021-04-01) and dattime format (2020-09-05T02:50:00Z)
        # reformat date: # Handles both simple date format (2021-04-01) and dattime format (2020-09-05T02:50:00Z)
        start_date = metadata_row[start_date_col]
        end_date = metadata_row[end_date_col]

        if pd.notna(start_date) and pd.notna(end_date):
            start_date = self.format_dates_for_duration_calculation(date=metadata_row[start_date_col])
            end_date= self.format_dates_for_duration_calculation(date=metadata_row[end_date_col])
    
            #Calculate the difference
            duration = end_date - start_date

            # Convert to ISO 8061
            iso_duration = isodate.duration_isoformat(duration)

            return iso_duration
        
        else:
            # if start date or end date is NA will return missing: not collected
            return "missing: not collected "
    
    def get_tot_depth_water_col_from_lat_lon(self, metadata_row: pd.Series, lat_col: float, lon_col: float, exact_map_col: str = None) -> float:


        if pd.notna(metadata_row[exact_map_col]):
            return metadata_row[exact_map_col]
        else:
            lat = metadata_row[lat_col]
            lon = metadata_row[lon_col]

            # Check if verbatim column and use to determin negative or positive sign 
            # pandas has a bug where it removes the negative using .apply()
            if 'S' in metadata_row['verbatimLatitude']:
                lat = float(-abs(lat))
            if 'W' in metadata_row['verbatimLongitude']:
                lon = float(-abs(lon))

            # open the gebco dataset
            ds = xr.open_dataset(self.gebco_file)

            # get the closest point in the dataset to the coordinates
            elevation = ds.elevation.sel(lat=lat, lon=lon, method='nearest').values

            # close the dataset
            ds.close()

            # make positive value
            return abs(elevation)

    def fill_empty_sample_values(self, df: pd.DataFrame, default_message = "missing: not collected"):
        # fill empty values for samples after mapping over all sample data without control samples

        # check if data frame is sample data frame and if so, then adds not applicable: control sample to control columns
        if '.NC' not in df['samp_name'].iloc[0] and 'POSITIVE' not in df['samp_name'].iloc[0]:
            for col, message in self.not_applicable_to_samp_faire_col_dict.items():
                df[col] = message
        elif '.NC' in df['samp_name']:
            # for NC sample pos_cont_type is just not applicable
            df['pos_cont_type'] = "not applicable"
        
        # Use default message for all other empty values - handles None, Nan
        df = df.fillna(default_message)

        # Handles empty strings (which might not be caught by fillna) - with default message
        df = df.map(lambda x: default_message if x == "" else x)

        return df
    
    def fill_nc_metadata(self, final_sample_df: pd.DataFrame) -> pd.DataFrame:
        # Fills the negative control data frame
        
        nc_results = {}
        # add exact mappings
        for faire_col, metadata_col in self.nc_mapping_dict[self.exact_mapping].items():
            nc_results[faire_col] = self.nc_df[metadata_col].apply(
                lambda row: self.apply_exact_mappings(metadata_row=row, faire_col=faire_col))
            
        # Step 2: Add constant mappings
        for faire_col, static_value in self.nc_mapping_dict[self.constant_mapping].items():
            nc_results[faire_col] = self.apply_static_mappings(faire_col=faire_col, static_value=static_value)

        # Step 3. Add related mappings
        # Step 3: Add related mappings
        for faire_col, metadata_col in self.nc_mapping_dict[self.related_mapping].items():
            # Add samp_category
            if faire_col == 'samp_category' and metadata_col == self.sample_metadata_sample_name_column:
                nc_results[faire_col] = self.nc_df.apply(
                    lambda row: self.add_samp_category_by_sample_name(metadata_row=row, faire_col=faire_col, metadata_col=metadata_col),
                    axis=1
                )
            elif faire_col == 'prepped_samp_store_dur':
                date_col_names = metadata_col.split(' | ')
                nc_results[faire_col] = self.nc_df.apply(
                    lambda row: self.calculate_date_duration(metadata_row=row, start_date_col=date_col_names[0], end_date_col=date_col_names[1]),
                    axis=1
                )
            elif faire_col == 'neg_cont_type':
                nc_results[faire_col] = self.nc_df[metadata_col].apply(self.add_neg_cont_type)
    
        # First concat with sample_faire_template to get rest of columns, 
        # # and add user_defined columnsthen
        # Fill na and empty values with not applicable: control sample
        nc_df = pd.concat([self.sample_faire_template_df, pd.DataFrame(nc_results)])

        new_cols = [col for col in final_sample_df.columns if col not in nc_df.columns]
        for col in new_cols: 
            nc_df[col] = 'not applicable: control sample'
        nc_df = self.fill_empty_sample_values(df = nc_df, default_message='not applicable: control sample')

        return nc_df

    # def fill_seq_pos_control_metadata(self, final_sample_df: pd.DataFrame) -> pd.DataFrame:
        #TODO: add pos_cont_type mapping

        # Get positive control df and only keep the sample name and assay name columns
        positive_samp_df = self.exp_metadata_df[self.exp_metadata_df['samp_name'].str.contains('POSITIVE', case=True)][['samp_name', 'assay_name']]

        # Update sample_category
        positive_samp_df['samp_category'] = positive_samp_df.apply(
            lambda row: self.add_samp_category_by_sample_name(metadata_row=row, faire_col='samp_category', metadata_col='samp_name'),
            axis=1
        )

        positive_samp_df['neg_cont_type'] = 'not applicable'

        positive_samp_df['eventDate'] = 'missing: not provided'

        positive_samp_df['pos_cont_type'] = positive_samp_df['samp_name'].apply(self.add_pos_cont_type)

        # fill res of empty values
        faire_pos_df = pd.concat([self.sample_faire_template_df, positive_samp_df])

        new_cols = [col for col in final_sample_df.columns if col not in faire_pos_df.columns]
        for col in new_cols: 
            faire_pos_df[col] = 'not applicable: control sample'
        
        faire_pos_df = self.fill_empty_sample_values(df=faire_pos_df, default_message='not applicable: control sample')

        return faire_pos_df




                



