from pydantic import BaseModel, field_validator, Field, model_validator
from typing import Literal, Optional, Union, Any, List, ClassVar, Dict
from shapely.geometry import Point
import geopandas as gpd
import warnings
from pathlib import Path
import math
from datetime import datetime
from collections import defaultdict
 # Field(default=None) is for USer defined fields that might be missing in each csv
# TODO: Add custom data validation for controlled vocabs that accept 'other: <description>' (samp_category, neg_cont_type, verbatimCoordinateSystem, verbatimSRS,
# samp_size_unit, samp_store_temp, samp_store_sol, precip_chem_prep, filter_material)
# TODO: Add list validation abstracted out (sample_derived_from)
# TODO: Add custom validation for cross-field referencing? E.g. if sample in rel_cont_id, must exist as a samp_name? biological_rep_relation
# TODO: Add custom validation for decimalLat/Lon and checking its in Arctic, and not outside of allowable lat/lon limits - see og validation script
# TODO: Add custom validation for geo_loc_name
# TODO: Add custom validation for cross referencing geo_loc_name with decimalLat/Lon - see og sample validation for code
# TODO: Add custom validation for EventDate for formatting? Might be too hard for all the different variations
# TODO: Add custom validation for eventDurationValue and samp_store_dur, stationed_sample_dur, prepped_samp_store_dur
# TODO: Add custom validation for env scale variable - use API to check

# Load World SEas IHO dataset once at modul level (when file is imported)
def load_iho_dataset():
    try:
    # Get the parent directory of the current script
        parent_dir = Path(__file__).parent.parent
        # reference the file relative to the script's location
        target_file = parent_dir / "utils" / "World_Seas_IHO_v3/World_Seas_IHO_v3.shp"
        return gpd.read_file(target_file)
    except Exception as e:
        print(f"Warning: Could not load IHO datset: {e}")
        return None

# This runs once when the module is imported 
_iho_dataset = load_iho_dataset()

class SampleMetadata(BaseModel):
    samp_name: str
    samp_category: Literal['sample', 'negative control', 'positive control', 'PCR standard']
    neg_cont_type: Optional[Literal['site negative', 'field negative', 'process negative', 'extraction negative', 'PCR negative']]
    pos_cont_type: Optional[str]
    materialSampleID: Optional[int]
    sample_derived_from: Optional[str] # needs custom list validation
    sample_composed_of: Optional[str] # needs custom list validation
    rel_cont_id: Optional[str] # needs custom list validation
    biological_rep_relation: Optional[str] # needs custom list validation
    decimalLongitude: Optional[float]
    decimalLatitude: Optional[float]
    verbatimLongitude: Optional[str]
    verbatimLatitude: Optional[str]
    verbatimCoordinateSystem: Optional[Literal['decimal degrees', 'degrees minutes seconds', 'UTM']]
    verbatimSRS: Optional[Literal['WGS84', 'NAD84', 'NAD27', 'GDA94', 'GDA2020 ', 'ETRS89', 'JGD2000']]
    geo_loc_name: Optional[str] # Needs custom validation - see mapping functions for this
    eventDate: Optional[str] 
    eventDurationValue: Optional[str]
    verbatimEventDate: Optional[str]
    verbatimEventTime: Optional[str]
    env_broad_scale: Optional[str]
    env_local_scale: Optional[str]
    env_medium: Optional[str]
    habitat_natural_artificial_0_1: Optional[Literal[0, 1]]
    samp_collect_method: Optional[str]
    samp_collect_device: Optional[str]
    samp_size: Optional[float]
    samp_size_unit: Optional[Literal['mL', 'L', 'mg', 'g', 'kg', 'cm2', 'm2', 'cm3', 'm3']]
    samp_store_temp: Optional[Union[float, Literal['ambient temperature']]]
    samp_store_sol: Optional[Literal['ethanol', 'sodium acetate', 'longmire', 'lysis buffer ', 'none']]
    samp_store_dur: Optional[str]
    samp_store_loc: Optional[str]
    dna_store_loc: Optional[str]
    samp_store_method_additional: Optional[str]
    samp_mat_process: Optional[str]
    filter_passive_active_0_1: Optional[Literal[0, 1]]
    stationed_sample_dur: Optional[str]
    pump_flow_rate: Optional[float]
    pump_flow_rate_unit: Optional[Literal['m3/s ', 'm3/min ', 'm3/h', 'L/s', 'L/min', 'L/h']]
    prefilter_material: Optional[str]
    size_frac_low: Optional[float] = Field(description = "in µm")
    size_frac: Optional[float] = Field(description = "in µm")
    filter_diameter: Optional[float] = Field(description = "in mm")
    filter_surface_area: Optional[float] = Field(description = "in mm2")
    filter_material: Optional[Literal['cellulose', 'cellulose ester',  'glass fiber', 'thermoplastic membrane', 'track etched polycarbonate', ' nylon', 'polyethersulfone']]
    filter_name: Optional[str]
    precip_chem_prep: Optional[Literal['ethanol', 'isopropanol', 'sodium chloride']]
    precip_force_prep: Optional[float] = Field(description = "x g")
    precip_time_prep: Optional[float] = Field(description = "minute")
    precip_temp_prep: Optional[Union[float, Literal['ambient temperature']]] = Field(description = "degree Celsius")
    prepped_samp_store_temp: Optional[Union[float, Literal['ambient temperature']]] = Field(description = "degree Celsius")
    prepped_samp_store_sol: Optional[Literal['ethanol', 'sodium acetate', 'longmire', 'lysis buffer']]
    prepped_samp_store_dur: Optional[str]
    prep_method_additional: Optional[str] 
    assay_name: Optional[Union[Literal['ssu18sv9_amaralzettler', 'dloopv1.5v4_baker', 'lsu16s_2434-2571_kelly', 
                        'COI_1835-2198_lerayfolmer', 'ssu18sv8_machida', 'ssu18sv8_machida_OSUmod', 
                        'ssu16sv4v5_parada', 'ssu16sv4v5_parada_OSUmod', 'ssu18sv4_stoeck', 
                        'ITS1_sterling', 'ssu12sv5v6_mifish_u_sales'], None]] = None
    samp_weather: Optional[str]
    minimumDepthInMeters: Optional[float]
    maximumDepthInMeters: Optional[float]
    tot_depth_water_col: Optional[float]
    elev: Optional[float]
    temp: Optional[float] = Field(description = 'in degree Celsius')
    chlorophyll: Optional[float] = Field(description = 'in mg/m3')
    light_intensity: Optional[float] = Field(description = 'in lux')
    ph: Optional[float]
    ph_meth: Optional[str]
    salinity: Optional[float] = Field(description = 'partical salinity unit (psu)')
    salinity_flag: Optional[int] = Field(default=None)
    suspend_part_matter: Optional[float] = Field(description = 'in mg/L')
    tidal_stage: Optional[str]
    turbidity: Optional[float] = Field(description = 'nephelometric turbidity unit (ntu)')
    water_current: Optional[float] = Field(description = 'in m/s')
    solar_irradiance: Optional[str]
    wind_direction: Optional[str]
    wind_speed: Optional[float] = Field(descripton = 'in m/s')
    diss_inorg_carb: Optional[float]
    diss_inorg_carb_unit: Optional[str]
    diss_inorg_carb_flag: Optional[int] = Field(default=None)
    diss_inorg_nitro: Optional[float]
    diss_inorg_nitro_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    diss_org_carb: Optional[float]
    diss_org_carb_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    diss_org_nitro: Optional[float]
    diss_org_nitro_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    diss_oxygen: Optional[float]
    diss_oxygen_unit: Optional[Literal['mg/L', 'µg/L', 'µM', 'mol/m3', 'mmol/m3', 'µmol/m3', 'mol/L', 'mmol/L', 'µmol/L' , 'mg/L ', 'µg/L x', 'mL/L', 'mmol/kg', 'parts per million']]
    tot_diss_nitro: Optional[float]
    tot_diss_nitro_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    tot_inorg_nitro: Optional[float]
    tot_inorg_nitro_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    tot_nitro: Optional[float]
    tot_nitro_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    tot_part_carb: Optional[float]
    tot_part_carb_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    tot_org_carb: Optional[float]
    tot_org_carb_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    tot_org_c_meth: Optional[str]
    tot_nitro_content: Optional[float]
    tot_nitro_content_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    tot_nitro_cont_meth: Optional[str]
    tot_carb: Optional[float]
    tot_carb_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    part_org_carb: Optional[float]
    part_org_carb_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    part_org_nitro: Optional[float]
    part_org_nitro_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    nitrate: Optional[float]
    nitrate_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    nitrate_flag: Optional[str] = Field(default=None)
    nitrite: Optional[float]
    nitrite_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    nitrite_flag: Optional[int] = Field(default=None)
    nitro: Optional[float]
    nitro_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    org_carb: Optional[float]
    org_carb_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    org_matter: Optional[float]
    org_matter_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    org_nitro: Optional[float]
    org_nitro_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    ammonium: Optional[float]
    ammonium_unit: Optional[Literal['µmol/L']]
    ammonium_flag: Optional[int] = Field(default=None)
    carbonate: Optional[float]
    carbonate_unit: Optional[Literal['']]  # need to add cv here when known
    hydrogen_ion: Optional[float] = Field(default=None)
    hydrogen_ion_unit: Optional[int] = Field(default=None)
    nitrate_plus_nitrite: Optional[float]
    nitrate_plus_nitrite_unit: Optional[Literal['']]  # need to add cv here when known
    omega_arag: Optional[float]
    omega_calc: Optional[float] = Field(default=None)
    pco2: Optional[float]
    pco2_unit: Optional[Literal['uatm']]
    phosphate: Optional[float]
    phosphate_unit: Optional[Literal['µmol/L']]
    phosphate_flag: Optional[int] = Field(default=None)
    pressure: Optional[float]
    pressure_unit: Optional[Literal['dbar']]
    silicate: Optional[float]
    silicate_unit: Optional[Literal['µmol/L']]
    silicate_flag: Optional[int] = Field(default=None)
    tot_alkalinity: Optional[float]
    tot_alkalinity_unit: Optional[Literal['µmol/kg']]
    tot_alkalinity_flag: Optional[int] = Field(default=None)
    transmittance: Optional[float]
    transmittance_unit: Optional[Literal['']] # need to add cv here when known
    serial_number: Optional[str]
    line_id: Optional[str]
    station_id: Optional[str]
    ctd_cast_number: Optional[int]
    ctd_bottle_number: Optional[int]
    replicate_number: Optional[int]
    biosample_accession: Optional[str]
    organism: Optional[str]
    samp_collect_notes: Optional[str]
    percent_oxygen_sat: Optional[float]
    density: Optional[float]
    density_unit: Optional[Literal['kg/m3']]
    air_temperature: Optional[float]
    air_temperature_unit: Optional[Literal['degree Celsius']]
    par: Optional[float]
    par_unit: Optional[Literal['µmol s-1 m-2']]
    air_pressure_at_sea_level: Optional[float]
    air_pressure_at_sea_level_unit: Optional[Literal['mb']]
    d18O_permil: Optional[float] = Field(default=None, description='18O oxygen iostope ratio, expressed in per mill (%) unit deviations from the international standard which is Standard Mean Ocean Water, as distributed by the International Atomic Energy Agency.')
    d18O_permil_flag: Optional[int] = Field(default=None)
    expedition_id: Optional[str]
    expedition_name: Optional[str]
    rosette_position: Optional[int]
    collected_by: Optional[str]
    meaurements_from: Optional[str]
    niskin_flag: Optional[int] = Field(default=None)

    
    # class variables loaded once and shared across all datasets
    # list of arctic region keywods
    _artic_region_keywords: ClassVar[List[str]] = ['bering', 'chukchi', 'arctic', 'north pacific', 'alaska', 'british columbia']
    _faire_missing_fields: ClassVar[List[str]] = ["not applicable: control sample",
                                                  "not applicable: sample group",
                                                  "not applicable",
                                                  "missing: not collected: synthetic construct",
                                                  "missing: not collected: lab stock",
                                                  "missing: not collected: third party data",
                                                  "missing: not collected",
                                                  "missing: not provided",
                                                  "missing: restricted access: endangered species", 
                                                  "missing: restricted access: human-identifiable", 
                                                  "missing: restricted access"]
    _expedition_dates: ClassVar[Dict[str, List[datetime]]] = defaultdict(list) #For tracking dates to make sure ranges are valid
  
    
    # Treat missing faire field strings as empty pre-validation
    @model_validator(mode='before')
    @classmethod
    def convert_missing_strings(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # Process each filed in the dictionary
            for key, value in data.items():
                if (
                    (isinstance(value, str) and value in cls._faire_missing_fields) or
                    (isinstance(value, float) and math.isnan(value)) or 
                    (isinstance(value, str) and value.lower() in ['nan', 'null', ''])
                ):
                    data[key] = None
            return data
        
    @model_validator(mode='after')
    def validate_required_fields_for_samples(self):
        # Ensure sample records have required fields that are not required for controls
        required = [self.decimalLatitude, self.decimalLongitude, self.geo_loc_name, self.env_broad_scale, self.env_local_scale, self.env_medium, self.eventDate]
        if self.samp_category == 'sample':
            for attribute in required:
                if attribute is None:
                    raise ValueError(f"Sample {self.samp_name} mus have {attribute}")
        return self

    # validate materialSampleID as only allowed 4 or 6 digit
    @field_validator('materialSampleID')
    @classmethod
    def validate_parent_samp_digits(cls, v):
        if v:
            digit_count = len(str(abs(v)))
            if digit_count not in [4, 6]:
                raise ValueError(f'materialSampleID must have exactly 4 or 6 digits, got {digit_count}')
            return v

    @field_validator('decimalLatitude')
    @classmethod
    def validate_latitude(cls, v, info):
        # only apply to sample records, not controls
        if info.data.get('samp_category') != 'sample':
            return v
        if not(-90 <= v <= 90):
            raise ValueError(f"Latitude {v} is outside valid latitude range (-90 to 90)")
        return v
    
    @field_validator('decimalLongitude')
    @classmethod
    def validate_latitude(cls, v, info):
        # only apply to sample records, not controls
        if info.data.get('samp_category') != 'sample':
            return v
        if not(-180 <= v <= 180):
            raise ValueError(f"Longitude {v} is outside valid latitude range (-180 to 180)")
        return v
    
    @field_validator('eventDate')
    @classmethod
    def validate_event_date_constraints(cls, v):
        """ensure no sampling before 2018"""
        if not v:
            return v
        
        try:
            event_datetime = datetime.fromisoformat(v.replace('Z', '+00:00'))
            if event_datetime.year < 2018:
                raise ValueError(f"eventDate year {event_datetime.year} is before 2018.")
            
            return v
            
        except ValueError as e:
            if "eventDate year" in str(e):
                raise e # Re-raise our custom year validation error
            else:
                raise ValueError(f"eventDate: '{v}' is not in valid ISO format")

    @model_validator(mode='after')
    def validate_expedition_date_ranges(self):
        """Class method to validate date ranges across all records in an expedition. Call this after validating individual records"""
        if not self.eventDate:
            return self

        try:
            event_datetime = datetime.fromisoformat(self.eventDate.replace('Z', '+00:00'))
            self._expedition_dates[self.expedition_id].append(event_datetime)
            expedition_dates = self._expedition_dates[self.expedition_id]

            if len(expedition_dates) > 1:
                date_range_days = (max(expedition_dates) - min(expedition_dates)).days
                max_days = 400 if 'pps' in self.expedition_id.lower() else 60

                if date_range_days > max_days:
                    raise ValueError(f"Expedition '{self.expedition_id}: dat range of {date_range_days} days "
                                    f"exceeds maximum {max_days}")
        
        except ValueError as e:
            if "Expedition" in str(e) or "eventDate" in str(e):
                raise e
            else:
                raise ValueError(F"Error validateing eventDate for {self.samp_name}")
            
        return self
    
    @classmethod
    def reset_expedition_tracking(cls):
        """Call this before processing a new dataset"""
        cls._expedition_dates.clear()

    @model_validator(mode='after')
    def validate_arctic_region_and_geo_loc_name(self):
        """Validate that coordinates are in Arctic Region and geo_loc_name makes sense"""
        if _iho_dataset is None:
            warnings.warn("IHO dataset not available, skipping geographics validation")
            return self
        # skip validation for non-sample records that won't have lat lon or geo_loc
        if self.samp_category != 'sample':
            return self
        
        point = Point(self.decimalLongitude, self.decimalLatitude)
        try:
            geo_loc_sea_area = self.geo_loc_name.split(':')[1]
            geo_loc_country = self.geo_loc_name.split(':')[0]
        # Sometimes Arctic Ocean won't have USA in front because its an INSDC acceptable region
        except:
            geo_loc_country = None
            geo_loc_sea_area = self.geo_loc_name

         # Check country is USA
        if geo_loc_country and geo_loc_country not in 'USA':
            raise ValueError(f"{self.samp_name} has country {geo_loc_country}, which is not acceptable.")
        
        # check sea area has key words
        for _, region in _iho_dataset.iterrows():
            if region.geometry.contains(point):
                supposed_sea = region.get('NAME')
                # Check if any of the arctic keywords exist in the supposed sea to make sure the lat/lon coords are indeed in the Arctic
                if not any(keyword in supposed_sea.lower() for keyword in self._artic_region_keywords):
                    raise ValueError(f"{self.samp_name} has a lat/lon with found in the area of {supposed_sea}, which does not have keywords {self._artic_region_keywords}")
                
                # Check geo_loc kind of matches
                if supposed_sea.lower().strip() != geo_loc_sea_area.lower().strip():
                    warnings.warn(f"{self.samp_name} has lat lon coordinates that point to {supposed_sea}, but geo_loc is listed as {geo_loc_sea_area}, double check this!")
                break
        return self # Always return self in mode='after' validators

    @model_validator(mode='after')
    def validate_neg_cont_type_conditions(self):
        """Validate that neg_cont_type is required when samp_category is 'negative control"""
        if self.samp_category == 'negative control':
            if self.neg_cont_type is None:
                raise ValueError(f"{self.samp_name}: neg_cont_type is required when samp_category is 'negative control'")
        return self
    
    @model_validator(mode='after')
    def validate_pos_cont_type_conditions(self):
        """Validate that pos_cont_type is required when samp_category is 'positive control"""
        if self.samp_category == 'positive control':
            if self.pos_cont_type is None:
                raise ValueError(f"{self.samp_name}: pos_cont_type is required when samp_category is 'positive control'")
        return self
    

## Notes: without @classmethod you can access self.other_field, but iwth @class_method you cannot. Use @classmethod when you only need to validate a single field.
## mode='after' runs after all the fields are processed and vliadted
## Notes: @model_Validator with @class_method mode='before' runs before the model is constructed. your working with data before feild validation. Must use 
# @class_method because no instance exists yet
## NOtes @model_validator without @class_method mode='after' runs after the model is fully constructed and vliadted