from pydantic import BaseModel
from typing import Literal, Optional, List
import csv

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
    associatedSequences: Optional[str]
    input_read_count: Optional[int]
    output_read_count: Optional[int]
    output_otu_num: Optional[int]
    otu_num_tax_assigned: Optional[int]






