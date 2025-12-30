from .faire_mapper import OmeFaireMapper
from pathlib import Path
from datetime import datetime
import isodate
import pandas as pd
import yaml
import requests
import numpy as np
import xarray as xr
import geopandas as gpd
import gsw
from shapely.geometry import Point
# from geopy.distance import geodesic
from bs4 import BeautifulSoup
from .custom_exception import NoInsdcGeoLocError
from .lists import nc_faire_field_cols
from geopy.distance import geodesic
from faire_mapping.utils import fix_cruise_code_in_samp_names, load_google_sheet_as_df
from faire_mapping.sample_mapper.extraction_standardizer import ExtractionStandardizer
from faire_mapping.mapping_builders.extraction_blank_mapping_dict_builder import ExtractionBlankMappingDictBuilder
from faire_mapping.mapping_builders.sample_extract_mapping_dict_builder import SampleExtractionMappingDictBuilder


# TODO: Turn nucl_acid_ext for DY20/12 into a BeBOP and change in extraction spreadsheet. Link to spreadsheet: https://docs.google.com/spreadsheets/d/1iY7Z8pNsKXHqsp6CsfjvKn2evXUPDYM2U3CVRGKUtX8/edit?gid=0#gid=0
# TODO: continue update pos_df - add not applicable: sample to user defined fields, also add pos_cont_type


class FaireSampleMetadataMapper(OmeFaireMapper):

    faire_sample_name_col = "samp_name"
    faire_sample_category_name_col = "samp_category"
    faire_neg_cont_type_name_col = "neg_cont_type"
    sample_mapping_sheet_name = "sampleMetadata"
    # extraction_mapping_sheet_name = "extractionMetadata"
    replicate_parent_sample_metadata_col = "replicate_parent"
    faire_lat_col_name = "decimalLatitude"
    faire_lon_col_name = "decimalLongitude"
    # faire_samp_vol_we_dna_ext_col_name = "samp_vol_we_dna_ext"
    # faire_pool_num_col_name = "pool_dna_num"
    faire_rel_cont_id_col_name = "rel_cont_id"
    faire_temp_col_name = "temp"
    faire_pressure_col_name = "pressure"
    faire_salinity_col_name = "salinity"
    faire_density_col_name = "density"
    faire_max_depth_col_name = "maximumDepthInMeters"
    faire_min_depth_col_name = "minimumDepthInMeters"
    # faire_nucl_acid_ext_method_additional_col_name = "nucl_acid_ext_method_additional"
    faire_tot_depth_water_col_method_col_name = 'tot_depth_water_col_method'
    not_applicable_to_samp_faire_col_dict = {"neg_cont_type": "not applicable: sample group",
                                             "pos_cont_type": "not applicable: sample group"}
    gebco_file = "/home/poseidon/zalmanek/FAIRe-Mapping/utils/GEBCO_2024.nc"

    # # common extraction columns created during extraction manipulation
    # extract_samp_name_col = "samp_name"
    # extract_conc_col = "extraction_conc"
    # extract_date_col = "extraction_date"
    # extraction_set_col = "extraction_set"
    # extraction_cruise_key_col = "extraction_cruise_key"
    # extraction_name_col = "extraction_name"
    # extraction_blank_vol_we_dna_ext_col = "extraction_blank_vol_dna_ext"
    # extract_id_col = 'extract_id'

    def __init__(self, config_yaml: yaml):
        # TODO: used to have exp_metadata_df: pd.Series as init, but removed because of abstracting out sequencing yaml. See all associated commented out portions
        # May need to move this part into a separate class that combines after all sample_metadata is generated for each cruise
        super().__init__(config_yaml)

        self.sample_metadata_sample_name_column = self.config_file[
            'sample_metadata_sample_name_column']
        self.sample_metadata_file_neg_control_col_name = self.config_file[
            'sample_metadata_file_neg_control_col_name']
        self.sample_metadata_cast_no_col_name = self.config_file['sample_metadata_cast_no_col_name']
        self.sample_metadata_bottle_no_col_name = self.config_file[
            'sample_metadata_bottle_no_col_name']
        self.nc_samp_mat_process = self.config_file['nc_samp_mat_process']
        self.vessel_name = self.config_file['vessel_name']
        self.faire_template_file = self.config_file['faire_template_file']
        self.google_sheet_mapping_file_id = self.config_file['google_sheet_mapping_file_id']
        self.unwanted_cruise_code = self.config_file['cruise_code_fixes']['unwanted_cruise_code'] if 'cruise_code_fixes' in self.config_file else None
        self.desired_cruise_code = self.config_file['cruise_code_fixes']['desired_cruise_code'] if 'cruise_code_fixes' in self.config_file else None

        self.samp_dur_info = self.config_file['samp_store_dur_sheet_info'] if 'samp_store_dur_sheet_info' in self.config_file else None
        self.samp_stor_dur_dict = self.create_samp_stor_dict() if 'samp_store_dur_sheet_info' in self.config_file else None
        
        #Instantiate mapping dict builder
        self.sample_extract_mapping_builder = SampleExtractionMappingDictBuilder(google_sheet_mapping_file_id=self.google_sheet_mapping_file_id)

        # self.extractions_info = self.config_file['extractions']
        # self.extractions_df = self.create_concat_extraction_df()
        # self.extraction_blank_rel_cont_dict = {}

        self.extraction_standardizer = ExtractionStandardizer(extractions_info=self.config_file['extractions'],
                                                              google_sheet_json_cred=self.config_file['json_creds'],
                                                              unwanted_cruise_code=self.unwanted_cruise_code,
                                                              desired_cruise_code=self.desired_cruise_code)
        
        self.sample_metadata_df = self.filter_metadata_dfs()[0]
        self.nc_df = self.filter_metadata_dfs()[1]
        # self.extraction_blanks_df = self.get_extraction_blanks_applicable_to_cruise_samps()
        self.sample_faire_template_df = self.load_faire_template_as_df(
            file_path=self.config_file['faire_template_file'], sheet_name=self.sample_mapping_sheet_name, header=self.faire_sheet_header).dropna()
        self.replicates_dict = self.create_biological_replicates_dict()
        self.insdc_locations = self.extract_insdc_geographic_locations()


        # stations/reference stations stuff
        self.station_name_reference_google_sheet_id = self.config_file['station_name_reference_google_sheet_id'] if 'station_name_reference_google_sheet_id' in self.config_file else None # Some projects won't have reference stations (RC0083)
        station_dicts = self.create_station_ref_dicts() if 'station_name_reference_google_sheet_id' in self.config_file else None # Some projects won't have reference stations (RC0083)
        self.station_lat_lon_ref_dict = station_dicts[0] if 'station_name_reference_google_sheet_id' in self.config_file else None # Some projects won't have reference stations (RC0083)
        self.standardized_station_dict = station_dicts[1] if 'station_name_reference_google_sheet_id' in self.config_file else None # Some projects won't have reference stations (RC0083)
        self.station_line_dict = station_dicts[2] if 'station_name_reference_google_sheet_id' in self.config_file else None # Some projects won't have reference stations (RC0083)

    # def create_sample_mapping_dict(self) -> dict:
    #     # creates a mapping dictionary and saves as self.mapping_dict

    #     # First concat sample_mapping_df with extractions_mapping_df
    #     sample_mapping_df = load_google_sheet_as_df(
    #         google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.sample_mapping_sheet_name, header=0)
    #     extractions_mapping_df = load_google_sheet_as_df(
    #         google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.extraction_mapping_sheet_name, header=1)
 
    #     mapping_df = pd.concat([sample_mapping_df, extractions_mapping_df])

    #     # Group by the mapping type
    #     group_by_mapping = mapping_df.groupby(
    #         self.mapping_file_mapped_type_column)

    #     # Create nested dictionary {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
    #     mapping_dict = {}
    #     for mapping_value, group in group_by_mapping:
    #         column_map_dict = {k: v for k, v in zip(
    #             group[self.mapping_file_FAIRe_column], group[self.mapping_file_metadata_column]) if pd.notna(v)}
    #         mapping_dict[mapping_value] = column_map_dict

    #     return mapping_dict

    def create_nc_mapping_dict(self) -> dict:

        nc_mapping_dict = {}
        for mapping_type, col_dict in self.sample_extract_mapping_builder.sample_mapping_dict.items():
            if isinstance(col_dict, dict):
                filtered_nested = {
                    k: v for k, v in col_dict.items() if k in nc_faire_field_cols}
                nc_mapping_dict[mapping_type] = filtered_nested

        # change values that will be differenct for NC's
        nc_mapping_dict[self.constant_mapping]['habitat_natural_artificial_0_1'] = '1'
        nc_mapping_dict[self.constant_mapping]['samp_mat_process'] = self.nc_samp_mat_process
        nc_mapping_dict[self.related_mapping]['prepped_samp_store_dur'] = self.config_file['nc_prepped_samp_store_dur']
        nc_mapping_dict[self.constant_mapping]['samp_store_dur'] = "not applicable: control sample"
        nc_mapping_dict[self.constant_mapping]['samp_store_temp'] = 'ambient temperature'
        nc_mapping_dict[self.constant_mapping]['samp_store_loc'] = self.vessel_name

        return nc_mapping_dict

    # def create_extraction_blank_mapping_dict(self) -> dict:
    #     # Creates a mapping dict for extraction blanks - mapping will be the same for only extractions faire attributes
    #     extractions_mapping_df = self.load_google_sheet_as_df(
    #         google_sheet_id=self.google_sheet_mapping_file_id, sheet_name=self.extraction_mapping_sheet_name, header=1)

    #     group_by_mapping = extractions_mapping_df.groupby(
    #         self.mapping_file_mapped_type_column)

    #     # Create nested dictionary {exact_mapping: {faire_col: metadata_col}, narrow_mapping: {faire_col: metadta_col}, etc.}
    #     mapping_dict = {}
    #     for mapping_value, group in group_by_mapping:
    #         column_map_dict = {k: v for k, v in zip(
    #             group[self.mapping_file_FAIRe_column], group[self.mapping_file_metadata_column]) if pd.notna(v)}
    #         mapping_dict[mapping_value] = column_map_dict

    #     # If there is an exact mapping for samp_vol_we_dna_ext then remove and put constant value base on config file value
    #     if self.faire_samp_vol_we_dna_ext_col_name in mapping_dict[self.exact_mapping]:
    #         mapping_col = mapping_dict[self.exact_mapping][self.faire_samp_vol_we_dna_ext_col_name]
    #         del mapping_dict[self.exact_mapping][self.faire_samp_vol_we_dna_ext_col_name]
    #         mapping_dict[self.related_mapping][self.faire_samp_vol_we_dna_ext_col_name] = mapping_col


    #     # If pool_dna_num in exact mapping automatically make constant mapping of 1 (may need to change if blanks are ever pooled for some reason)
    #     if self.faire_pool_num_col_name in mapping_dict[self.exact_mapping]:
    #         del mapping_dict[self.exact_mapping][self.faire_pool_num_col_name]
    #         del mapping_dict[self.exact_mapping][self.faire_nucl_acid_ext_method_additional_col_name]
    #         mapping_dict[self.constant_mapping][self.faire_pool_num_col_name] = 1
    #         mapping_dict[self.constant_mapping][self.faire_nucl_acid_ext_method_additional_col_name] = "missing: not provided"

    #     return mapping_dict

    def create_samp_stor_dict(self) -> dict:

        samp_dur_df = load_google_sheet_as_df(google_sheet_id=self.samp_dur_info['google_sheet_id'], sheet_name='Sheet1', header=0)

        # fix sample names if cruise-code was corrrected in sample names
        if self.unwanted_cruise_code and self.desired_cruise_code:
            samp_dur_df = fix_cruise_code_in_samp_names(df=samp_dur_df, 
                                                        unwanted_cruise_code=self.unwanted_cruise_code,
                                                        desired_cruise_code=self.desired_cruise_code,
                                                        sample_name_col=self.samp_dur_info['samp_name_col'])
        samp_dur_dict = dict(zip(samp_dur_df[self.samp_dur_info['samp_name_col']], samp_dur_df[self.samp_dur_info['samp_stor_dur_col']]))

        return samp_dur_dict

    def create_station_ref_dicts(self) -> dict:
        # Creates a lat_lon_station_ref_dict reference dictionary for station names and their lat_lon coords (e.g. {'BF2': {'lat': '71.75076', 'lon': -154.4567}, 'DBO1.1': {'lat': '62.01', 'lon': -175.06}}
        # Also creates a standardized station dict
        
        station_ref_df = load_google_sheet_as_df(google_sheet_id=self.station_name_reference_google_sheet_id, sheet_name='Sheet1', header=0)
        station_lat_lon_ref_dict = self.create_station_lat_lon_ref_dict(station_ref_df=station_ref_df)
        station_standardized_name_dict = self.create_standardized_station_name_ref_dict(station_ref_df=station_ref_df)
        station_line_dict = self.create_station_line_id_ref_dict(station_ref_df=station_ref_df)

        return station_lat_lon_ref_dict, station_standardized_name_dict, station_line_dict
    
    def create_station_lat_lon_ref_dict(self, station_ref_df: pd.DataFrame) -> dict:
        station_lat_lon_ref_dict = {}
        for _, row in station_ref_df.iterrows():
            station_name = row['station_name']
            lat = row['LatitudeDecimalDegree']
            lon = row['LongitudeDemicalDegree']
            lat_hem = row['LatitudeHem']
            lon_hem = row['LongitudeHem']

            # Add direction sign to lat/lon
            if 'S' == lat_hem:
                lat = float(-abs(float(lat)))
            if 'W' == lon_hem:
                lon = float(-abs(float(lon)))
            
            station_lat_lon_ref_dict[station_name] = {
                'lat': lat,
                'lon': lon
            }

        return station_lat_lon_ref_dict
    
    def create_standardized_station_name_ref_dict(self, station_ref_df: pd.DataFrame) -> dict:
        station_standardized_name_dict = {}
        for _, row in station_ref_df.iterrows():
            standardized_name = row['station_name']
            non_standardized_names = row['ome_station'].split(' | ')

            station_standardized_name_dict[standardized_name] = non_standardized_names

        return station_standardized_name_dict
    
    def create_station_line_id_ref_dict(self, station_ref_df: pd.DataFrame) -> dict:
        # create reference dict where station name is key and line id is value
        station_line_dict = dict(zip(station_ref_df['station_name'], station_ref_df['line_id']))
        return station_line_dict
     
    # def convert_mdy_date_to_iso8061(self, date_string: str) -> str:
    #     # converts from m/d/y to iso8061
    #     date_string = str(date_string).strip()

    #     # Handle both single and double digit formats
    #     try:
    #         parts = date_string.split('/')
    #         if len(parts) == 3:
    #             month, day, year = parts

    #             formatted_date = f"{int(month):02d}/{int(day):02}/{year}"

    #             # parse date string
    #             date_obj = datetime.strptime(formatted_date, "%m/%d/%Y")

    #             # convert to iso 8601 format
    #             return date_obj.strftime('%Y-%m-%d')

    #         elif len(parts) == 2:
    #             # hande month/year format by assuming day=1
    #             month, year = parts
    #             formatted_date = f"{int(month):02d}/01/{year}"

    #             # parse date string
    #             date_obj = datetime.strptime(formatted_date, "%m/%d/%Y")

    #             # convert to iso 8601 format
    #             return date_obj.strftime('%Y-%m-%d')
    #         else:
    #             raise ValueError(
    #                 f"Date doesn't have two or three parts: {date_string}")

    #     except Exception as e:
    #         print(f"Error converting {date_string}: {str(e)}")

    # def extraction_avg_aggregation(self, extractions_df: pd.DataFrame):
    #     # For extractions, calculates the mean if more than one concentration per sample name.

    #     # Keep Below Range
    #     if all(isinstance(conc, str) and ("below range" in conc.lower() or "br" == conc.lower() or "bdl" == conc.lower()) for conc in extractions_df):
    #         return "BDL"

    #     # For everything else, convert to numeric and calculate mean
    #     numeric_series = pd.to_numeric(
    #         extractions_df, errors='coerce')  # Non numeric becomes NaN

    #     mean_value = numeric_series.mean()

    #     return round(mean_value, 2)

    # def get_extraction_blanks_applicable_to_cruise_samps(self):
    #     # Get extraction blank df (applicable to RC0083 cruise)

    #     blank_df = pd.DataFrame(columns=self.extractions_df.columns)

    #     # TODO: peel out 'Extraction Set' column name into config.yaml file
    #         # try grouping by extraction set if it exists
    #     grouped = self.extractions_df.groupby(
    #         self.extraction_set_col)

    #     try:
    #         for extraction_set, group_df in grouped:
    #             # Check if any in the group contains the extraction cruise key
    #             has_cruise_samps = group_df.apply(
    #                 lambda row: str(row[self.extraction_cruise_key_col]) in str(row[self.extract_samp_name_col]),
    #                 axis = 1
    #             ).any()
                
    #             if has_cruise_samps:
    #                 # find blank samples in this group ('Larson NC are extraction blanks for the SKQ23 cruise)
    #                 extraction_blank_samps = group_df[
    #                     (group_df[self.extract_samp_name_col].str.contains('blank', case=False, na=False)) | 
    #                     (group_df[self.extract_samp_name_col].str.contains('Larson NC', case=False, na=False))]
                    
    #                 try:
    #                     # get list of samples associated with blanks and put into dict for rel_cont_id
    #                     valid_samples = group_df[self.extract_samp_name_col].tolist()
    #                     for sample in valid_samples:
    #                         if 'blank' in sample.lower() or 'Larson NC' in sample:
    #                             other_samples = [samp for samp in valid_samples if samp != sample]
    #                             # update cruise codes in sample names
    #                             if self.unwanted_cruise_code and self.desired_cruise_code:
    #                                 sample = sample.replace(self.unwanted_cruise_code, self.desired_cruise_code)
    #                                 other_samples = [samp.replace(self.unwanted_cruise_code, self.desired_cruise_code) for samp in other_samples]
    #                             self.extraction_blank_rel_cont_dict[sample] = other_samples
    #                 except:
    #                     raise ValueError("blank dictionary mapping not working!")


    #                 blank_df = pd.concat([blank_df, extraction_blank_samps])

    #         blank_df[self.extract_conc_col] = blank_df[self.extract_conc_col].replace(
    #             "BR", "BDL").replace("Below Range", "BDL").replace("br", "BDL")
    #     except:
    #         raise ValueError(
    #             "Warning: Extraction samples are not grouped, double check this")
        
    #     return blank_df
     
    # def create_concat_extraction_df(self) -> pd.DataFrame:
    #     # Concatenate extractions together and create common column names

    #     extraction_dfs = []
    #     # loop through extractions and append extraction dfs to list
    #     for extraction in self.extractions_info:
    #         # mappings from config file to what we want to call it so it has a common name 
    #         extraction_column_mappings = {extraction['extraction_sample_name_col']: self.extract_samp_name_col,
    #                                      extraction['extraction_conc_col_name']: self.extract_conc_col,
    #                                      extraction['extraction_date_col_name']: self.extract_date_col,
    #                                      extraction['extraction_set_grouping_col_name']: self.extraction_set_col,}
            
    #         extraction_df = self.load_google_sheet_as_df(google_sheet_id=extraction['extraction_metadata_google_sheet_id'], sheet_name=extraction['extraction_metadata_sheet_name'], header=0)

    #         # change necessary column names so they will match across extraction dfs and add additional info
    #         extraction_df = extraction_df.rename(columns=extraction_column_mappings)
    #         extraction_df[self.extraction_cruise_key_col] = extraction.get('extraction_cruise_key')
    #         extraction_df[self.extraction_blank_vol_we_dna_ext_col] = extraction.get('extraction_blank_vol_we_dna_ext')
    #         extraction_df[self.extraction_name_col] = extraction.get('extraction_name')

    #         extraction_df[self.extract_id_col] = extraction_df[self.extraction_name_col].astype(str).replace(r'\s+', '_', regex=True) + "_" + extraction_df[self.extraction_set_col].astype(str).replace(r'\s+', '_', regex=True)
    #         extraction_dfs.append(extraction_df)

    #     # Concat dataframes
    #     final_extraction_df = pd.concat(extraction_dfs)

    #     # if any columns changed names that are in the mapping dict, change them there too
    #     for maps in self.mapping_dict.values():
    #         for faire_col, metadata_col in maps.items():
    #             if metadata_col in extraction_column_mappings.keys():
    #                 maps[faire_col] = extraction_column_mappings.get(metadata_col)
        
    #     if self.unwanted_cruise_code and self.desired_cruise_code and self.unwanted_cruise_code != '.NO20': # this gets updates twice because unwanted cruise code is in the desired cruise code so will update later
    #         final_extraction_df = fix_cruise_code_in_samp_names(df=final_extraction_df,
    #                                                             unwanted_cruise_code=self.unwanted_cruise_code,
    #                                                             desired_cruise_code=self.desired_cruise_code,
    #                                                             sample_name_col=self.extract_samp_name_col)
     
    #     return final_extraction_df

    # def filter_cruise_avg_extraction_conc(self) -> pd.DataFrame:
    #     # If extractions have multiple measurements for extraction concentrations, calculates the avg.
    #     # and creates a column called pool_num to show the number of samples pooled
    #     # First filter extractions df for samples that contain the cruise key in their sample name
    #     cruise_key_mask = self.extractions_df.apply(
    #         lambda row: str(row[self.extraction_cruise_key_col]) in str(row[self.extract_samp_name_col])
    #         if pd.notna(row[self.extraction_cruise_key_col]) and pd.notna(row[self.extract_samp_name_col])
    #         else False,
    #         axis = 1
    #     )
        
    #     # Then calculate average concentration
    #     extract_avg_df = self.extractions_df[cruise_key_mask].groupby(
    #         self.extract_samp_name_col).agg({
    #             self.extract_conc_col: self.extraction_avg_aggregation,
    #             **{col: 'first' for col in self.extractions_df.columns if col != self.extract_samp_name_col and col != self.extract_conc_col}
    #         }).reset_index()

    #     # Add pool_num column that shows how many samples were averaged for each group
    #     sample_counts = self.extractions_df[cruise_key_mask].groupby(
    #         self.extract_samp_name_col).size().reset_index(name='pool_num')

    #     # merge sample_counts into dataframe
    #     extract_avg_df = extract_avg_df.merge(
    #         sample_counts, on=self.extract_samp_name_col, how='left')

    #     # Add extraction_method_additional for samples that pooled more than one extract
    #     extract_avg_df['extraction_method_additional'] = extract_avg_df['pool_num'].apply(
    #         lambda x: "One sample, but two filters were used because sample clogged. Two extractions were pooled together and average concentration calculated." if x > 1 else "missing: not provided")

    #     # update samp name for DY2012 cruises (from DY20) and remove E numbers from any NC samples
    #     extract_avg_df[self.extract_samp_name_col] = extract_avg_df[self.extract_samp_name_col].apply(
    #         self.str_replace_for_samps)

    #     # update dates to iso8601 TODO: may need to adjust this for ones that are already in this format
    #     extract_avg_df[self.extract_date_col] = extract_avg_df[self.extract_date_col].apply(
    #         self.convert_mdy_date_to_iso8061)
        
    #     if self.unwanted_cruise_code and self.desired_cruise_code:
    #         if 'NO20' not in self.unwanted_cruise_code: # NO20 is updated in str_replace_samps (can't remove it from there because the experimentRunMetadata uses it)
    #             extract_avg_df = fix_cruise_code_in_samp_names(df=extract_avg_df, sample_name_col=self.extract_samp_name_col)

    #     return extract_avg_df

    def transform_metadata_df(self):
        # Converts sample metadata to a data frame and checks to make sure NC samples have NC in the name (dy2206 had this problem)
        # Also fixes sample names for SKQ23 cruise where sample metadata has _, but extraction metadata has -
        samp_metadata_df = self.load_csv_as_df(
            file_path=Path(self.config_file['sample_metadata_file']))

        # Add .NC to sample name if the Negative Control column is True and its missing (in DY2206 cruise)
        samp_metadata_df[self.sample_metadata_sample_name_column] = samp_metadata_df.apply(
            lambda row: self._check_nc_samp_name_has_nc(metadata_row=row),
            axis=1)

        # Fix sample names that have _ with -. For SKQ23 cruises where run and extraction metadata has -, and sample metadata has _
        samp_metadata_df[self.sample_metadata_sample_name_column] = samp_metadata_df[self.sample_metadata_sample_name_column].apply(
            self._fix_samp_names)

        # Remove 'CTD' from Cast_No. value if present
        samp_metadata_df[self.sample_metadata_cast_no_col_name] = samp_metadata_df[self.sample_metadata_cast_no_col_name].apply(
            self.remove_extraneous_cast_no_chars)
        
        if self.unwanted_cruise_code and self.desired_cruise_code:
            samp_metadata_df = fix_cruise_code_in_samp_names(df=samp_metadata_df, 
                                                             unwanted_cruise_code=self.unwanted_cruise_code,
                                                             desired_cruise_code=self.desired_cruise_code,
                                                             sample_name_col=self.sample_metadata_sample_name_column)

        return samp_metadata_df

    def join_sample_and_extract_df(self):
        # join extraction sheet with sample metadata sheet on Sample name - keeping only samples from extraction df
        samp_df = self.transform_metadata_df()

        metadata_df = pd.merge(
            left=self.extraction_standardizer.extraction_df,
            right=samp_df,
            left_on=self.extraction_standardizer.EXTRACT_SAMP_NAME_COL,
            right_on=self.sample_metadata_sample_name_column,
            how='left'
        )

        # Drop rows where the sample name column value is NA. This is for cruises where samples were split up
        # e.g. PPS samples that were deployed from the DY2306 cruise. They will be a separate sample metadata file.
        metadata_df = metadata_df.dropna(
            subset=[self.sample_metadata_sample_name_column])

    
        return metadata_df

    def _check_nc_samp_name_has_nc(self, metadata_row: pd.Series) -> str:
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

    def _fix_samp_names(self, sample_name: str) -> str:
        # Fixes samples names (really just for SKQ23 sample names to replace _ with -. As the extraction and run sheets use -)
        if '_' in sample_name:
            sample_name = sample_name.replace('_', '-')
        if '.DY23-06' in sample_name:
            sample_name = sample_name.replace('-', '')
        if '.SKQ2021': 
            sample_name = sample_name.replace('.SKQ2021', '.SKQ21-15S')

        return sample_name

    def remove_extraneous_cast_no_chars(self, cast_no: str) -> int:
        # If Cast_No. is in the format of CTD001, then returns just the int
        if pd.notna(cast_no) and isinstance(cast_no, str) and 'CTD' in cast_no:
            cast_no = int(cast_no.replace('CTD', ''))

        return cast_no

    def filter_metadata_dfs(self):

        # Join sample metadata with extraction metadata to get samp_df
        samp_df = self.join_sample_and_extract_df()

        try:
            nc_mask = samp_df[self.sample_metadata_sample_name_column].astype(
                str).str.contains('.NC', case=True)
            nc_df = samp_df[nc_mask].copy()
            samp_df_filtered = samp_df[~nc_mask].copy()

            # Replace any - with NaN
            nc_df = nc_df.replace('-', pd.NA)
            samp_df_filtered = samp_df_filtered.replace('-', pd.NA)
            
            return samp_df_filtered, nc_df
        
        except:
            print(
                "Looks like there are no negatives in the sample df, returning an empty nc_df")
            nc_df = pd.DataFrame()
            return samp_df_filtered, nc_df

    def extract_insdc_geographic_locations(self) -> list:

        url = 'https://www.insdc.org/submitting-standards/geo_loc_name-qualifier-vocabulary/'

        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
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
        self.sample_metadata_df[self.replicate_parent_sample_metadata_col] = self.sample_metadata_df[self.sample_metadata_sample_name_column].apply(
            self.extract_replicate_sample_parent)
        # Group by replicate parent
        replicate_dict = self.sample_metadata_df.groupby(self.replicate_parent_sample_metadata_col)[
            self.sample_metadata_sample_name_column].apply(set).to_dict()
        # remove any key, value pairs where there aren't replicates and convert back to list
        replicate_dict = {replicate_parent: list(set(
            sample_name)) for replicate_parent, sample_name in replicate_dict.items() if len(sample_name) > 1}

        return replicate_dict

    def add_biological_replicates(self, metadata_row: pd.Series, faire_missing_val: str) -> dict:

        if self.replicates_dict.get(metadata_row.get(self.replicate_parent_sample_metadata_col)):
            replicates = ' | '.join(self.replicates_dict.get(
                metadata_row[self.replicate_parent_sample_metadata_col], None))
            return replicates
        else:
            return faire_missing_val

    def add_neg_cont_type(self, samp_name: str) -> dict:
        # Adds the negative control type to the neg_cont_type column of the FAIRe template based on strings in the sample name
        # TODO: Add error if it snot an field negative or extraction negative

        field_neg_control_str = '.NC'
        extraction_neg_control_str = 'blank'

        if field_neg_control_str in samp_name:
            neg_cont_type = self.check_cv_word(
                value='field negative', faire_attribute=self.faire_neg_cont_type_name_col)
        elif extraction_neg_control_str in samp_name.lower():
            neg_cont_type = self.check_cv_word(
                value='extraction negative', faire_attribute=self.faire_neg_cont_type_name_col)

        return neg_cont_type

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

    def add_material_sample_id(self, metadata_row, cruise_code: str) -> str:
        # Formats MaterialSampleID to be numerical (discussed with Sean) - double check if this needs to be updated for three digit cast numbers
        # can also be used for sample_derived_from if no other in between parent samples
        cast_int = int(metadata_row.get(self.sample_metadata_cast_no_col_name))
        btl_int = int(metadata_row.get(
            self.sample_metadata_bottle_no_col_name))

        formatted_cast = f'{cast_int:02d}'
        formatted_btl = f'{btl_int:02d}'

        material_sample_id = cruise_code + '_' + formatted_cast + formatted_btl
        return str(material_sample_id)

    def add_material_samp_id_for_pps_samp(self, metadata_row: pd.Series, cast_or_event_col: str, prefix: str):
        # Creates a material sample id in the format of "M2-PPS-0423_Port1" Where the cruise name _ cast
        # "M2-PPS-0423" is the prefix and not the cruise name because tehcnically the PPs was part of a cruise (e.g. DY2306)
        # the cast will have 'Event1" so need to extract the 1
        cast_val = metadata_row[cast_or_event_col]
        port_num = cast_val.replace('Event','')
        return f"{prefix}_Port{port_num.strip()}"

    def get_well_number_from_well_field(self, metadata_row: pd.Series, well_col: str) -> int:
        # Gets the well number from a row that has a value like G1 -> 1
        try:
            well = metadata_row[well_col]
            return well[-1]
        except:
            None
    
    def get_well_position_from_well_field(self, metadata_row: pd.Series, well_col: str) -> int:
        # Get the well position from a row that has a value like G1 -> G
        try:
            well = metadata_row[well_col]
            return well[0]
        except:
            None

    # def fix_cruise_code_in_samp_names(self, df: pd.DataFrame, sample_name_col: str = None) -> pd.DataFrame:
    #     # fixes the cruise code in the sample names to be SKQ21-15S as requested by Shannon on 07/23/2025
    #     if self.desired_cruise_code == '.DY20-12': # since the extraction sheet used .DY20 and teh sample metadata use .DY2012 need to replace for both
    #         mask = (df[sample_name_col].str.endswith('.DY20')) | (df[sample_name_col].str.endswith(self.unwanted_cruise_code))
    #         # Apply the replacement only to the rows where the mask is True
    #         df.loc[mask, sample_name_col] = df.loc[mask, sample_name_col].str.replace(
    #             self.unwanted_cruise_code, 
    #             self.desired_cruise_code
    #         )
    #     else: # everything else just replaces with the desired cruise code
    #         df[sample_name_col] = df[sample_name_col].str.replace(self.unwanted_cruise_code, self.desired_cruise_code)

    #     return df 
    
    def calculate_dna_yield(self, metadata_row: pd.Series, sample_vol_metadata_col: str) -> float:
        # calculate the dna yield based on the concentration (ng/uL) and the sample_volume (mL)
        concentration = str(metadata_row[self.extract_conc_col])
        sample_vol = str(metadata_row[sample_vol_metadata_col]).replace('~','')
        
        if concentration == '' or concentration == None or concentration == 'nan':
            return 'not applicable'

        try:
            concentration = float(concentration)
            sample_vol = float(sample_vol)
            dna_yield = (concentration * 100)/sample_vol
            return round(dna_yield, 3)
        except:
            if '.nc' in metadata_row[self.faire_sample_name_col].lower():
                return 'not applicable: control sample'
            return 'not applicable'

    def calculate_altitude(self, metadata_row: pd.Series, depth_col: str, tot_depth_col: str) -> float:
        # Calculates the altitude by subtracting the depth from the tot_depth_col
        depth = metadata_row[depth_col]
        tot_depth_water_col = metadata_row[tot_depth_col]

        return round((tot_depth_water_col - depth), 2)

    def get_line_id(self, station) -> str:
        # Get the line id by the referance station (must be standardized station name)
        if station in self.station_line_dict:
            if self.station_line_dict.get(station):
                return self.station_line_dict.get(station)
            else:
                return "not applicable"

    def convert_wind_degrees_to_direction(self, degree_metadata_row: pd.Series) -> str:
        # converts wind direction  to cardinal directions

        if pd.isna(degree_metadata_row) or degree_metadata_row is None:
            return "missing: not collected"
        else:
            direction_labels = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE",
                                "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
            ix = np.round(degree_metadata_row /
                          (360. / len(direction_labels))).astype('i')

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
                sea = metadata_row[geo_loc_metadata_col].replace(
                    'Beiring', 'Bering')
            else:
                sea = metadata_row[geo_loc_metadata_col]
            geo_loc = 'USA: ' + sea
        else:
            geo_loc = metadata_row[geo_loc_metadata_col]

        # check that geo_loc_name (first string before first :) is an acceptes insdc word
        if ':' in geo_loc:
            geo_loc_name = geo_loc.split(':')[0]
        # accounts for ones that did not have 'sea' in their name, thus no USA in front - e.g. Arctic Ocean
        else:
            geo_loc_name = metadata_row[geo_loc_metadata_col]
        if geo_loc_name not in self.insdc_locations:
            raise NoInsdcGeoLocError(
                f'There is no geographic location in INSDC that matches {metadata_row[geo_loc_metadata_col]}, check sea_name and try again')

        return geo_loc

    def find_geo_loc_by_lat_lon(self, metadata_row: pd.Series, metadata_lat_col: str, metadata_lon_col: str) -> str:
        # World Seas IHO downloaded from: https://marineregions.org/downloads.php

        print(
            f"Getting geo_loc_name for {metadata_row[self.sample_metadata_sample_name_column]}")
        lat = metadata_row.get(metadata_lat_col)
        lon = metadata_row.get(metadata_lon_col)

        marine_regions = gpd.read_file(
            "/home/poseidon/zalmanek/FAIRe-Mapping/utils/World_Seas_IHO_v3/World_Seas_IHO_v3.shp")
        # create a point object
        point = Point(lon, lat)

        # Check for which marin region contains this point
        for idx, region in marine_regions.iterrows():
            if region.geometry.contains(point):
                sea = region.get('NAME')
            
                if sea == 'Arctic Ocean':
                    geo_loc = sea
                elif 'and British Columbia' in sea:
                    geo_loc = f"USA: {sea.replace('and British Columbia', '')}"
                else:
                    geo_loc = f"USA: {sea}"

                try:
                    geo_loc_name = geo_loc.split(':')[0]
                except:
                    geo_loc_name = [geo_loc]
                if geo_loc_name not in self.insdc_locations:
                    raise NoInsdcGeoLocError(
                        f'There is no geographic location in INSDC that matches {geo_loc_name}, check sea_name and try again')
                else:
                    return geo_loc
            # else:
            #     raise ValueError(f"No sea is found at the lat/lon ({lat}/{lon}) of sample: {metadata_row[self.sample_metadata_sample_name_column]}")

    def calculate_env_local_scale(self, depth: float) -> str:
        # uses the depth to assign env_local_scale
        aphotic = "marine aphotic zone [ENVO:00000210]"
        photic = "marine photic zone [ENVO:00000209]"

       
        if float(depth) <= 200:
            env_local_scale = photic
        elif float(depth) > 200:
            env_local_scale = aphotic

        return env_local_scale

    def format_dates_for_duration_calculation(self, date: str) -> datetime:
        if date in  [None, 'nan', 'missing: not collected', '', 'missing: not provided']:
            return "missing: not provided"
        
        # Handle different date formats and timezone indicators (used with calculate_date_duration function)
        if 'T' in date:
            if 'Z' in date:
                # Convert 'Z' timezone to UTC offset format
                date = date.replace('Z', '+00:00')
                # Create datetime and strip timezone info
                dt = datetime.fromisoformat(date).replace(tzinfo=None)
        # To account for dates in form 9/5/20 - may need to refactor
        elif '/' in date and ' ' in date:  # from like 2022/01/03 12:34:54
            dt = datetime.strptime(date, "%Y/%m/%d %H:%M:%S")
        elif '/' in date:
            try:  # 05/22/92 (two digit year format)
                date = datetime.strptime(date, "%m/%d/%y")
            except ValueError:  # 05/22/1992 (four digit format)
                date = datetime.strptime(date, "%m/%d/%Y")
            date = date.strftime("%Y-%m-%d")
            dt = datetime.fromisoformat(date)
        else:
            dt = datetime.fromisoformat(date)

        return dt

    def switch_lat_lon_degree_to_neg(self, lat_or_lon_deg) -> float:
        # If the sign is positive and should be negative, switch the sign of longitude or latitude

        return -(float(lat_or_lon_deg))

    def calculate_date_duration(self, metadata_row: pd.Series, start_date_col: str, end_date_col: str) -> datetime:
        # takes two dates and calcualtes the difference to find the duration of time in ISO 8601 format
        # Handles both simple date format (2021-04-01) and dattime format (2020-09-05T02:50:00Z)
        # reformat date: # Handles both simple date format (2021-04-01) and dattime format (2020-09-05T02:50:00Z)
        start_date = self.convert_date_to_iso8601(metadata_row[start_date_col])
        end_date = self.convert_date_to_iso8601(metadata_row[end_date_col])

        empty_dates = ['None', 'nan', 'missing: not collected', '', 'missing: not provided']
        if pd.notna(start_date) and pd.notna(end_date) and start_date not in empty_dates and end_date not in empty_dates:
            start_date = self.format_dates_for_duration_calculation(
                date=start_date)
                
            end_date = self.format_dates_for_duration_calculation(
                date=end_date)

            # Calculate the difference
            duration = end_date - start_date

            # Convert to ISO 8061
            iso_duration = isodate.duration_isoformat(duration)

            return iso_duration
    

        else:
            # if start date or end date is NA will return missing: not collected
            return "missing: not collected "

    def get_tot_depth_water_col_from_lat_lon(self, metadata_row: pd.Series, lat_col: float, lon_col: float, exact_map_col: str = None) -> float:

        try:
            if pd.notna(metadata_row[exact_map_col]):
                return metadata_row[exact_map_col]
            else:
                lat = metadata_row[lat_col]
                lon = metadata_row[lon_col]
        except:
            if exact_map_col == None:
                lat = metadata_row[lat_col]
                lon = metadata_row[lon_col]

        # Check if verbatim column and use to determin negative or positive sign
        # pandas has a bug where it removes the negative using .apply()
        # TODO: This is old. If any merges were updated with Brynn's merge script,
        # this should no longer be needed.
        try:
            if 'S' in metadata_row['verbatimLatitude']:
                lat = float(-abs(float(lat)))
            if 'W' in metadata_row['verbatimLongitude']:
                lon = float(-abs(float(lon)))
        except:
            lat = lat
            lon = lon

        # open the gebco dataset
        ds = xr.open_dataset(self.gebco_file)

        # get the closest point in the dataset to the coordinates
        elevation = ds.elevation.sel(lat=lat, lon=lon, method='nearest').values

        # close the dataset
        ds.close()

        # make positive value
        return abs(elevation)

    def get_samp_store_dur(self, sample_name: pd.Series) -> str:
        # Get the samp_store_dur based on the sample name
        
        samp_stor_dur = self.samp_stor_dur_dict.get(sample_name, '')
        
        if self.samp_dur_info['dur_units'] == 'hour':
            samp_stor_dur_formatted = f"T{samp_stor_dur}H"
            return samp_stor_dur_formatted
        else: 
            raise ValueError("samp_stor_dur unit is not yet functional in this code - update get_samp_stor_dur function for unit")

    def get_samp_store_loc_by_samp_store_dur(self, sample_name: pd.Series) -> str:
        # Gets the samp_store_loc based on the samp_name, based on the samp_store_dur. If samp_stor_dur > 1 hr, loc is vessel name fridge else just vessel name
        samp_stor_dur = int(self.samp_stor_dur_dict.get(sample_name, ''))
         
        if self.samp_dur_info['dur_units'] == 'hour':
            if samp_stor_dur > 1:
                samp_store_loc = f"{self.vessel_name} fridge"
            else:
                samp_store_loc = self.vessel_name
            return samp_store_loc
        else:
            raise ValueError(f"samp_store_loc not able to be calculated by {self.samp_dur_info['dur_units']}, add functionality to get_samp_store_loc_by_samp_store_dur method")

    def get_samp_sore_temp_by_samp_store_dur(self, sample_name: pd.Series) -> str:
        samp_stor_dur = int(self.samp_stor_dur_dict.get(sample_name, ''))

        if self.samp_dur_info['dur_units'] == 'hour':
            if samp_stor_dur > 1:
                samp_store_temp = 4
            else:
                samp_store_temp = 'ambient temperature'
            return samp_store_temp
        else:
            raise ValueError(f"samp_store_loc not able to be calculated by {self.samp_dur_info['dur_units']}, add functionality to get_samp_sore_temp_by_samp_store_dur method")
    
    def get_depth_from_pressure(self, metadata_row: pd.Series, press_col_name: str, lat_col_name: str) -> float:
        # Calculates depth from pressure and latitude
        pressure = metadata_row[press_col_name]
        lat = metadata_row[lat_col_name]

        if pressure:
            depth = gsw.conversions.z_from_p(pressure, lat)
            return round(abs(depth), 2)
        elif pressure == 0:
            return 0
        else:
            return ''

    def get_station_id_from_unstandardized_station_name(self, metadata_row: pd.Series, unstandardized_station_name_col: str) -> str:
        # Gets the standardized station name from the unstandardized station name
        station_name = metadata_row[unstandardized_station_name_col]
        sample_name = metadata_row[self.sample_metadata_sample_name_column]

        if 'NC' not in sample_name or 'blank' not in sample_name.lower():
            # Standardizes station ids to be from the reference station sheet
            for standard_station_name, unstandardized_station_list in self.standardized_station_dict.items():
                if station_name in unstandardized_station_list or station_name == standard_station_name:
                    return standard_station_name
        
            # only print if no match was found after whole iteration
            print(f"\033[33m{station_name} listed for sample {sample_name} is missing from station reference dictionary as an ome_station. Please check\033[0m]")
            return station_name
        else:
            return station_name

    def calculate_distance_btwn_lat_lon_points(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
            # Calculates the surface distance between two points in lat/lon using the great_circle package of GeoPy
            return geodesic((lat1, lon1), (lat2, lon2)).kilometers   
    
    def get_stations_within_5km(self, metadata_row: pd.Series, station_name_col: str, lat_col: str, lon_col: str) -> str:
        # Get alternate station names based on lat/lon coords and grabs all stations within 5 km as alternate stations - need to use standardized station names for station_name_col to be able to look 
        # up in station_lat_lon_dict
        lat = metadata_row[lat_col]
        lon = metadata_row[lon_col]
        listed_station = metadata_row[station_name_col]
        sample_name = metadata_row[self.sample_metadata_sample_name_column]

        if 'NC' not in sample_name or 'blank' not in sample_name.lower():
        
            # alt_stations = []
            distances = []
            error_distances = []

            for station_name, coords in self.station_lat_lon_ref_dict.items():
                station_lat = coords['lat']
                station_lon = coords ['lon']

                # calculate distance
                distance = self.calculate_distance_btwn_lat_lon_points(lat1=lat, lon1=lon, lat2=station_lat, lon2=station_lon)

                # Account for DBO1.9 which moved but coordinates haven't been updated yet - see email from Shaun
                # Also make exception for DBO4.1 (took this out since changing from 1 km to 3 km)
                if distance <= 5 or (station_name == 'DBO1.9' and distance <=10.5):
                    distances.append({
                        'station': station_name,
                        'distance_km': distance,
                        'coords': coords
                    })

                else:
                    error_distances.append({
                        'station': station_name,
                        'distance_km': distance,
                        'coords': coords
                    })
                    error_distances.sort(key=lambda x: x['distance_km'])

            # sort by distance and return top n
            distances.sort(key=lambda x: x['distance_km'])
            alt_station_names = ' | '.join([item['station'] for item in distances])
            # QC check reported station is in 5km list
            self.check_station_is_in_stations_in_5_km(alt_station_names=alt_station_names, reported_station=listed_station, samp_name=sample_name, distances=distances)
            if alt_station_names:
                return alt_station_names
            else:
                print(ValueError(f"\033[31m{sample_name} listed station {listed_station}, but it is not picking up on any stations with 5 km based on its lat/lon {lat, lon}. Closest station is {error_distances[0]}!\033[0m]"))
        else:
            return 'not applicable: control sample'
    
    def check_station_is_in_stations_in_5_km(self, alt_station_names: str, reported_station: str, samp_name:str, distances: dict) -> None:
        # Check that reported station showed up in the stations within 5 km
        alt_stations = alt_station_names.split(' | ')
        if reported_station not in alt_stations:
            closest_alt_station = alt_stations[0]
            for distance in distances:
                if closest_alt_station == distance.get('station'):
                    print(f"\033[36m{samp_name}'s reported station ({reported_station}) is not found within 5 km, the closest station found to it's lat/lon coords is {closest_alt_station} with a distance of {distance.get('distance_km')}\033[0m")   
    
    def update_companion_colums_with_no_corresponding_val(self, df: pd.DataFrame) -> pd.DataFrame: 
        # Update the final sample dataframe unit, woce flag, and method columns to be "not applicable" if there is no value in its corresponding column
        unit_cols = [col for col in df.columns if col.endswith('unit')]
        unit_str = '_unit'
        woce_cols = [col for col in df.columns if col.endswith('WOCE_flag')]
        woce_str = 'WOCE_flag'

        # method has some exceptions
        method_cols = [col for col in df.columns if col.endswith('method') and col != 'samp_collect_method']
        method_str = '_method'

        updated_df_for_unit = self.update_companion_cols(df=df, companion_col_list=unit_cols, str_to_remove_for_main_col=unit_str)
        updated_df_for_woce = self.update_companion_cols(df=updated_df_for_unit, companion_col_list=woce_cols, str_to_remove_for_main_col=woce_str)
        updated_df_for_method = self.update_companion_cols(df=updated_df_for_woce, companion_col_list=method_cols, str_to_remove_for_main_col=method_str)

        return updated_df_for_method
    
    def update_companion_cols(self, df: pd.DataFrame, companion_col_list: list, str_to_remove_for_main_col: str) -> pd.DataFrame:
            # iterates through a list of unit columns or WOCE flag columns and updates df
            for companion_col in companion_col_list:
                if companion_col == 'DepthInMeters_method':
                    main_col = 'maximumDepthInMeters'
                else:
                    main_col = companion_col.replace(str_to_remove_for_main_col, '') # Remove the 'unit' or 'WOCE_flag' from the column name

                if main_col in df.columns:
                    for idx in df.index:
                        val = df.at[idx, main_col]
                        # Check if we should update the unit
                        try:
                            should_update = False
                            if pd.isna(val) or val == None:
                                should_update = True
                            elif isinstance(val, str):
                                val_lower = val.lower()
                                if 'missing' in val_lower or 'not applicable' in val_lower:
                                    should_update = True
                            
                            if should_update:
                                df.at[idx, companion_col] = 'not applicable'
                        except:
                            print("updating companion col is not working!")

            return df
    
    def fill_empty_sample_values_and_finalize_sample_df(self, df: pd.DataFrame, default_message="missing: not collected"):
        # fill empty values for samples after mapping over all sample data without control samples
        if df.empty: # for empty NC dataframes (like in Aquamonitor)
            pass
        # check if data frame is sample data frame and if so, then adds not applicable: control sample to control columns
        else:
            if '.NC' not in df[self.faire_sample_name_col].iloc[0] and 'POSITIVE' not in df[self.faire_sample_name_col].iloc[0] and 'blank' not in df[self.faire_sample_name_col].iloc[0].lower():
                for col, message in self.not_applicable_to_samp_faire_col_dict.items():
                    df[col] = message
            elif '.NC' in df[self.faire_sample_name_col]:
                # for NC sample pos_cont_type is just not applicable
                df['pos_cont_type'] = "not applicable"

        # Use default message for all other empty values - handles None, Nan
        df = df.fillna(default_message)

        # Handles empty strings (which might not be caught by fillna) - with default message or a -
        df = df.map(lambda x: default_message if x == "" or x == "-" else x)

        # update unit cols for non-values
        updated_df = self.update_companion_colums_with_no_corresponding_val(df=df)

        # replace niskin anywhere in the df with captial "Niskin" - Sean request 07/23/2025
        final_df = updated_df.replace('niskin', 'Niskin')

        # Fix depths that are less than three to make it 3 m - Requested by Sean/Shannon on 07/23/2025
        update_depths_df = self.fix_too_low_depths(df=final_df)


        return update_depths_df

    def fix_too_low_depths(self, df: pd.DataFrame) -> pd.DataFrame:
        # If the depth is less than 3 m, change to 3 m, and fix min depth to be -1
        try:
            condition = df[self.faire_max_depth_col_name] < 3
            df.loc[condition, self.faire_max_depth_col_name] = 3
            df.loc[condition, self.faire_min_depth_col_name] = df.loc[condition, self.faire_max_depth_col_name] - 1
            return df
        except:
            return df
    
    def fill_nc_metadata(self) -> pd.DataFrame:
        # Fills the negative control data frame

        nc_mapping_dict = self.create_nc_mapping_dict()

        nc_results = {}
        # add exact mappings
        for faire_col, metadata_col in nc_mapping_dict[self.exact_mapping].items():
            nc_results[faire_col] = self.nc_df[metadata_col].apply(
                lambda row: self.apply_exact_mappings(metadata_row=row, faire_col=faire_col))

        # Step 2: Add constant mappings
        for faire_col, static_value in nc_mapping_dict[self.constant_mapping].items():
            nc_results[faire_col] = self.apply_static_mappings(
                faire_col=faire_col, static_value=static_value)

        # Step 3. Add related mappings
        # Step 3: Add related mappings
        for faire_col, metadata_col in nc_mapping_dict[self.related_mapping].items():
            # Add samp_category
            if faire_col == self.faire_sample_category_name_col and metadata_col == self.sample_metadata_sample_name_column:
                nc_results[faire_col] = self.nc_df.apply(
                    lambda row: self.add_samp_category_by_sample_name(
                        metadata_row=row, faire_col=faire_col, metadata_col=metadata_col),
                    axis=1
                )

            elif faire_col == 'prepped_samp_store_dur':
                if metadata_col != 'missing: not collected':
                    date_col_names = metadata_col.split(' | ')
                    nc_results[faire_col] = self.nc_df.apply(
                        lambda row: self.calculate_date_duration(
                            metadata_row=row, start_date_col=date_col_names[0], end_date_col=date_col_names[1]),
                        axis=1
                    )
                else:
                    # if metadata_col = "missing: not collected, then will be value"
                    nc_results[faire_col] = metadata_col

            elif faire_col == 'date_ext':
                nc_results[faire_col] = self.nc_df[metadata_col].apply(
                    self.convert_date_to_iso8601)

            elif faire_col == self.faire_neg_cont_type_name_col:
                nc_results[faire_col] = self.nc_df[metadata_col].apply(
                    self.add_neg_cont_type)

            elif faire_col == 'dna_yield':
                vol_col = self.sample_extract_mapping_builder.sample_mapping_dict[self.exact_mapping].get('samp_vol_we_dna_ext')
                nc_results[faire_col] = self.nc_df.apply(
                        lambda row: self.calculate_dna_yield(metadata_row=row, sample_vol_metadata_col=vol_col),
                        axis = 1
                    )
            elif faire_col == 'extract_well_number':
                nc_results[faire_col] = self.nc_df.apply(
                    lambda row: self.get_well_number_from_well_field(metadata_row=row, well_col=metadata_col),
                    axis=1
                )
        
            elif faire_col == 'extract_well_position':
                nc_results[faire_col] = self.nc_df.apply(
                    lambda row: self.get_well_position_from_well_field(metadata_row=row, well_col=metadata_col),
                    axis = 1 
                )

        # First concat with sample_faire_template to get rest of columns,
        # # and add user_defined columnsthen
        # Fill na and empty values with not applicable: control sample
        try:
            nc_results_df = pd.DataFrame(nc_results)

            # compare columns in df to nc_faire_fields list and if value is missing, fill with missing: not provided
            nc_results_updated = nc_results_df.reindex(columns=nc_faire_field_cols, fill_value="missing: not collected")
            nc_results_updated = nc_results_updated.fillna("missing: not collected")
            nc_df = pd.concat(
                [self.sample_faire_template_df, nc_results_updated])
            
            return nc_df
        except: # If empty just return empty dataframe
            return self.sample_faire_template_df

    def finish_up_controls_df(self, final_sample_df: pd.DataFrame) -> pd.DataFrame:

        nc_df = self.fill_nc_metadata()
        extraction_blanks_df = self.fill_extraction_blanks_metadata()

        if extraction_blanks_df is not None:
            neg_controls_df = pd.concat([nc_df, extraction_blanks_df])
        else:
            neg_controls_df = nc_df

        new_cols = [
            col for col in final_sample_df.columns if col not in neg_controls_df.columns]
        for col in new_cols:
            neg_controls_df[col] = 'not applicable: control sample'
        neg_controls_df = self.fill_empty_sample_values_and_finalize_sample_df(
            df=neg_controls_df, default_message='not applicable: control sample')
        
        # repalce any just "not applicable" values with "not applicable: control sample"
        # Hard time figuring out why some would pop up as just "not applicable"
        neg_controls_df = neg_controls_df.replace("not applicable", "not applicable: control sample")

        return neg_controls_df

    def fill_extraction_blanks_metadata(self) -> pd.DataFrame:

        extracton_blanks_mapping_builder = ExtractionBlankMappingDictBuilder(google_sheet_mapping_file_id=self.google_sheet_mapping_file_id)

        extract_blank_results = {}

        if not self.extraction_standardizer.extraction_blanks_df.empty:

            extract_blank_results[self.faire_sample_name_col] = self.extraction_standardizer.extraction_blanks_df[self.extraction_standardizer.EXTRACT_SAMP_NAME_COL]
            extract_blank_results[self.faire_sample_category_name_col] = "negative control"
            extract_blank_results[self.faire_neg_cont_type_name_col] = "extraction negative"
            extract_blank_results['habitat_natural_artificial_0_1'] = 1

            # Add mappings from mappings dict which is just mapping from extractionMetadata sheet
            # add exact mappings
            for faire_col, metadata_col in extracton_blanks_mapping_builder.extraction_blanks_mapping_dict[self.exact_mapping].items():
                extract_blank_results[faire_col] = self.extraction_standardizer.extraction_blanks_df[metadata_col].apply(
                    lambda row: self.apply_exact_mappings(metadata_row=row, faire_col=faire_col))

            # Step 2: Add constant mappings
            for faire_col, static_value in extracton_blanks_mapping_builder.extraction_blanks_mapping_dict[self.constant_mapping].items():
                extract_blank_results[faire_col] = self.apply_static_mappings(
                    faire_col=faire_col, static_value=static_value)

            # Step 3: Add related mappings
            for faire_col, metadata_col in extracton_blanks_mapping_builder.extraction_blanks_mapping_dict[self.related_mapping].items():
                if faire_col == 'date_ext':
                    extract_blank_results[faire_col] = self.extraction_standardizer.extraction_blanks_df[metadata_col].apply(
                        self.convert_date_to_iso8601)
                if faire_col == 'dna_yield':
                    extract_blank_results[faire_col] = self.extraction_standardizer.extraction_blanks_df.apply(
                        lambda row: self.calculate_dna_yield(metadata_row=row, sample_vol_metadata_col=self.extraction_standardizer.EXTRACT_BLANK_VOL_WE_DNA_EXT_COL),
                        axis = 1
                    )
                if faire_col == 'samp_vol_we_dna_ext':
                    extract_blank_results[faire_col] = self.extraction_standardizer.extraction_blanks_df[self.extraction_standardizer.EXTRACT_BLANK_VOL_WE_DNA_EXT_COL]

            extract_blanks_df = pd.concat(
                [self.sample_faire_template_df, pd.DataFrame(extract_blank_results)])

            return extract_blanks_df

        else:
            return None

    def add_nc_rel_cont_id_to_samp_df(self, final_sample_df: pd.DataFrame) -> pd.DataFrame:
        # Adds the .NC samples to the rel_cont_id of all other samples.
        # get list of nc samples. Uses metadatA_sample_name_column because this happens before nc_df is transformed
        nc_samps = self.nc_df[self.sample_metadata_sample_name_column].tolist()
        
        for idx, row in final_sample_df.iterrows():
            if 'NC' not in row[self.faire_sample_name_col] and 'blank' not in row[self.faire_sample_name_col].lower():
                final_sample_df.at[idx, 'rel_cont_id'] = ' | '.join(nc_samps)

        

        return final_sample_df
    
    def add_extraction_blanks_to_rel_cont_id(self, final_sample_df: pd.DataFrame):

        final_sample_df = self.add_nc_rel_cont_id_to_samp_df(final_sample_df=final_sample_df)

        for idx, row in final_sample_df.iterrows():
            current_samp = row[self.faire_sample_name_col]
            related_blanks = []

            for extraction_blank, associated_samples in self.extraction_standardizer.extraction_blank_rel_cont_dict.items():
                if current_samp in associated_samples:
                    related_blanks.append(extraction_blank)

            # get already existing rel_cont_id and add to blanks related ids
            if row['rel_cont_id']:
                related_ids = row['rel_cont_id'].split(' | ')
                all_related_ids = related_ids + related_blanks
            else:
                all_related_ids = related_blanks
            
            # remove any "not applicable" if there are other ids in all_related_ids
            final_sample_df.at[idx, self.faire_rel_cont_id_col_name] = ' | '.join(all_related_ids)
              
       
        return final_sample_df
   
    def add_constant_value_based_on_str_in_col(self, metadata_row: pd.Series, col_name: str, str_condition: str, pos_condition_const: str, neg_condition_const: str) -> str:
        # Adds a constant value based on a str being present in a column. col_value is the column value where the str will or won't be present, 
        # str_condition is the str that might be present to test against
        # pos_condition_const is the value that should be return if the str_condition is present in col_value
        # neg_condition_const is the value that should be returned if the str_condition is absent in col_value
        col_value = metadata_row[col_name]
        if str_condition.lower() in col_value.lower().strip():
            return pos_condition_const
        else:
            return neg_condition_const