from pydantic import BaseModel, field_validator, Field, model_validator
from typing import Literal, Optional, Union, Any, List, ClassVar
from shapely.geometry import Point
import geopandas as gpd
import warnings
from pathlib import Path

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

class SampleMetadata(BaseModel):
    samp_name: str
    samp_category: Literal['sample', 'negative control', 'positive control', 'PCR standard']
    neg_cont_type: Literal['site negative', 'field negative', 'process negative', 'extraction negative', 'PCR negative']
    pos_cont_type: str
    materialSampleID: Optional[int]
    sample_derived_from: Optional[str] # needs custom list validation
    sample_composed_of: Optional[str] # needs custom list validation
    rel_cont_id: Optional[str] # needs custom list validation
    biological_rep_relation: Optional[str] # needs custom list validation
    decimalLongitude: float
    decimalLatitude: float
    verbatimLongitude: Optional[str]
    verbatimLatitude: Optional[str]
    verbatimCoordinateSystem: Optional[Literal['decimal degrees', 'degrees minutes seconds', 'UTM']]
    verbatimSRS: Optional[Literal['WGS84', 'NAD84', 'NAD27', 'GDA94', 'GDA2020 ', 'ETRS89', 'JGD2000']]
    geo_loc_name: str # Needs custom validation - see mapping functions for this
    eventDate: str 
    eventDurationValue: Optional[str]
    verbatimEventDate: Optional[str]
    verbatimEventTime: Optional[str]
    env_broad_scale: str
    env_local_scale: str
    env_medium: str
    habitat_natural_artificial_0_1: Optional[Literal['0', '1]']]
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
    filter_passive_active_0_1: Optional[Literal['0', '1]']]
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
    assay_name: Literal['ssu18sv9_amaralzettler', 'dloopv1.5v4_baker', 'lsu16s_2434-2571_kelly', 
                        'COI_1835-2198_lerayfolmer', 'ssu18sv8_machida', 'ssu18sv8_machida_OSUmod', 
                        'ssu16sv4v5_parada', 'ssu16sv4v5_parada_OSUmod', 'ssu18sv4_stoeck', 
                        'ITS1_sterling', 'ssu12sv5v6_mifish_u_sales']
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
    salinity_flag: Optional[int]
    suspend_part_matter: Optional[float] = Field(description = 'in mg/L')
    tidal_stage: Optional[str]
    turbidity: Optional[float] = Field(description = 'nephelometric turbidity unit (ntu)')
    water_current: Optional[float] = Field(description = 'in m/s')
    solar_irradiance: Optional[str]
    wind_direction: Optional[str]
    wind_speed: Optional[float] = Field(descripton = 'in m/s')
    diss_inorg_carb: Optional[float]
    diss_inorg_carb_unit: Optional[str]
    diss_inorg_carb_flag: Optional[int]
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
    nitrate_flag: Optional[str]
    nitrite: Optional[float]
    nitrite_unit: Optional[Literal['µM', 'mol/m3', 'mmol/m3', 'µmol/m3 ', 'mol/L', 'mmol/L', 'µmol/L', 'mg/L',  'µg/L', 'µmol/kg', 'mmol/kg ', 'parts per million']]
    nitrite_flag: Optional[int]
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
    ammonium_flag: Optional[int]
    carbonate: Optional[float]
    carbonate_unit: Optional[Literal['']]  # need to add cv here when known
    hydrogen_ion: Optional[float]
    hydrogen_ion_unit: Optional[int]
    nitrate_plus_nitrite: Optional[float]
    nitrate_plus_nitrite_unit: Optional[Literal['']]  # need to add cv here when known
    omega_arag: Optional[float]
    omega_calc: Optional[float]
    pco2: Optional[float]
    pco2_unit: Optional[Literal['uatm']]
    phosphate: Optional[float]
    phosphate_unit: Optional[Literal['µmol/L']]
    phosphate_flag: Optional[int]
    pressure: Optional[float]
    pressure_unit: Optional[Literal['dbar']]
    silicate: Optional[float]
    silicate_unit: Optional[Literal['µmol/L']]
    silicate_flag: Optional[int]
    tot_alkalinity: Optional[float]
    tot_alkalinity_unit: Optional[Literal['µmol/kg']]
    tot_alkalinity_flag: Optional[int]
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
    d18O_permil: Optional[float] = Field(description = '18O oxygen iostope ratio, expressed in per mill (%) unit deviations from the international standard which is Standard Mean Ocean Water, as distributed by the International Atomic Energy Agency.')
    d18O_permil_flag: Optional[int]
    expedition_id: Optional[str]
    expedition_name: Optional[str]
    rosette_position: Optional[int]
    collected_by: Optional[str]
    meaurements_from: Optional[str]
    niskin_flag: Optional[int]

    
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
    _iho_dataset: ClassVar[gpd.GeoDataFrame] = None

    def __init__(self, **data):
        # Load IHO dataset once
        if self.__class__._iho_dataset is None: 
            self.__class__._iho_dataset = self.load_iho_dataset()
            super().__init__(**data)
            

    def load_iho_dataset(self):
        # Get the parent directory of the current script
        parent_dir = Path(__file__).parent.parent
        # reference the file relative to the script's location
        target_file = parent_dir / "utils" / "World_Seas_IHO_v3/World_Seas_IHO_v3.shp"
        return gpd.read_file(target_file)
    
    # Treat missing faire field strings as empty pre-validation
    @model_validator(mode='before')
    @classmethod
    def convert_missing_strings(cls, v: Any) -> Any:
        if isinstance(v, str) and v in cls._faire_missing_fields:
            return None
        return v

    # validate materialSampleID as only allowed 4 or 6 digits
    @field_validator('materialSampleID')
    @classmethod
    def validate_parent_samp_digits(cls, v):
        digit_count = len(str(abs(v)))
        if digit_count not in [4, 6]:
            raise ValueError(f'materialSampleID must have exactly 4 or 6 digits, got {digit_count}')
        return v

    @field_validator('decimalLatitude')
    @classmethod
    def validate_latitude(cls, v):
        if not(-90 <= v <= 90):
            raise ValueError(f"Latitude {v} is outside valid latitude range (-90 to 90)")
        return v
    
    @field_validator('decimalLongitude')
    @classmethod
    def validate_latitude(cls, v):
        if not(-180 <= v <= 180):
            raise ValueError(f"Longitude {v} is outside valid latitude range (-180 to 180)")
        return v
    
    @model_validator(mode='after')
    def validate_arctic_region_and_geo_loc_name(self):
        """Validate that coordinates are in Arctic Region and geo_loc_name makes sense"""
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
        for _, region in self._iho_dataset.iterrows():
            if region.geometry.contains(point):
                supposed_sea = region.get('NAME')
                # Check if any of the arctic keywords exist in the supposed sea to make sure the lat/lon coords are indeed in the Arctic
                if not any(keyword in supposed_sea.lower() for keyword in self._artic_region_keywords):
                    raise ValueError(f"{self.samp_name} has a lat/lon with found in the area of {supposed_sea}, which does not have keywords {self._artic_region_keywords}")
                
                # Check geo_loc kind of matches
                if supposed_sea.lower().strip() != geo_loc_sea_area.lower().strip():
                    warnings.warn(f"{self.samp_name} has lat lon coordinates that point to {supposed_sea}, but geo_loc is listed as {geo_loc_sea_area}, double check this!")
                break

    
