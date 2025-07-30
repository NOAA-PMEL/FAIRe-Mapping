from pydantic import BaseModel, field_validator, model_validator
from typing import Literal, Optional
import math
import warnings

# TODO: Need to add validator for controlled vocabulary attributes to have other: <description> (e.g. lib_conc_unit)
# TODO: Need to add validator for pipe separated string lists (associatedSequences)

class ExperimentRunMetadata(BaseModel):
    samp_name: str
    assay_name: Literal['ssu18sv9_amaralzettler', 'dloopv1.5v4_baker', 'lsu16s_2434-2571_kelly', 
                        'COI_1835-2198_lerayfolmer', 'ssu18sv8_machida', 'ssu18sv8_machida_OSUmod', 
                        'ssu16sv4v5_parada', 'ssu16sv4v5_parada_OSUmod', 'ssu18sv4_stoeck', 
                        'ITS1_sterling', 'ssu12sv5v6_mifish_u_sales']
    pcr_plate_id: str
    lib_id: str
    seq_run_id: str
    lib_conc: Optional[float]
    lib_conc_unit: Optional[Literal['ng/µL', 'nM', 'pM']]
    lib_conc_meth: Optional[str]
    phix_perc: Optional[float]
    mid_forward: Optional[str]
    mid_reverse: Optional[str]
    filename: Optional[str]
    filename2: Optional[str]
    checksum_filename: Optional[str]
    checksum_filename2: Optional[str]
    associatedSequences: Optional[str] = None
    input_read_count: Optional[int]
    output_read_count: Optional[int]
    output_otu_num: Optional[int]
    otu_num_tax_assigned: Optional[int]

    @field_validator('associatedSequences', mode='before')
    @classmethod
    def empty_associatedSequences_to_none(cls, v):
                if (
                    (isinstance(v, float) and math.isnan(v)) or 
                    (isinstance(v, str) and v.lower() in ['nan', 'null', '']) or
                    (isinstance(v, float) and math.isnan(v))):
                    return None
                return v
    
    @model_validator(mode='after')
    def valdate_input_read_count_is_bigger_than_output_read_count(self):
        # Checks ito make sure that input_read_count is bigger than output_read_count
        if self.input_read_count < self.output_read_count:
            raise ValueError(f"Sample: {self.samp_name} has an input_read_count of {self.input_read_count} which is less than the output_read_count of {self.output_read_count}")
        return self
    
    @model_validator(mode='after')
    def validate_count_metadata_is_not_none_for_all(self):
        # throws warning if input_read_count, output_read_count, output_otu_num, and otu_num_tax_assigned are all 0 - insinuates sample mismatches when running experimentRunMetadata code
        if self.input_read_count == 0 and self.output_read_count == 0 and self.output_otu_num == 0 and self.otu_num_tax_assigned == 0:
             warnings.warn(f"{self.samp_name} has 0 for input_read_count, output_read_count, output_otu_num, and otu_num_tax_assigned. Check metadata/data. There may be as sample name mismatch!")
        return self






