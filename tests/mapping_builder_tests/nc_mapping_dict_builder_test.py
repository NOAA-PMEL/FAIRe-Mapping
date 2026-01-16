from faire_mapping.mapping_builders.nc_mapping_dict_builder import  NcMappingDictBuilder
from faire_mapping.mapping_builders.sample_extract_mapping_dict_builder import SampleExtractionMappingDictBuilder

mapping_file_id = "1PKYStbZN3ygUvjXi9SmaXGwmwt-eJCZyDCZ8xzlQq0E"
json_cred_path = "/home/poseidon/zalmanek/FAIRe-Mapping/credentials.json"
nc_samp_mat_process = "Water samples were filtered through a 0.22 µm swinnex filter using a vacuum pump."
nc_prepped_samp_store_dur = "verbatimEventDate | extraction_date"
vessel_name = "Dyson"
sample_mapping_builder = SampleExtractionMappingDictBuilder(google_sheet_mapping_file_id=mapping_file_id, google_sheet_json_cred=json_cred_path)

nc_mapping_builder = NcMappingDictBuilder(google_sheet_mapping_file_id=mapping_file_id,
                                          google_sheet_json_cred=json_cred_path,
                                          sample_mapping_builder=sample_mapping_builder,
                                          nc_samp_mat_process=nc_samp_mat_process,
                                          nc_prepped_samp_stor_dur=nc_prepped_samp_store_dur,
                                          vessel_name=vessel_name)

print(nc_mapping_builder.nc_mapping_dict)