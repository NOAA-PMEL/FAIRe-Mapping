"""
Compare samples from the sample metadata to all the sequencing runs and 
flag for any mismatches
"""
from pathlib import Path
import pandas as pd
import yaml

def concat_csv_files(root_dir: str):

    csv_files = list(root_dir.rglob("*_faire.csv"))
    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

    return df

def samp_match_dict(exp_metadata_df: pd.DataFrame, samp_metadata_df: pd.DataFrame):

    exp_samps = exp_metadata_df['samp_name'].unique()
    samp_samps = samp_metadata_df['samp_name'].unique()

    mismatches = {}
    not_in_samp_metadata = []

    # {short_samp_name: full_samp_name}
    samp_name_dict = {}
    for full_samp_name in samp_samps:
        if '.nc' not in full_samp_name:
            samp_parts = full_samp_name.split('.')
            del samp_parts[-1]
            short_match_name = '.'.join(samp_parts)
            samp_name_dict[short_match_name] = full_samp_name
        else:
            samp_name_dict[full_samp_name] = full_samp_name

    for full_samp_name in exp_samps:
        if not 'positive' in full_samp_name.lower():
            samp_parts = full_samp_name.split('.')
            if not 'pcr' in full_samp_name.lower():
                del samp_parts[-1]
                short_match_name = '.'.join(samp_parts)
            else:
                del samp_parts[-2:]
                short_match_name = '.'.join(samp_parts)
        
            if short_match_name in samp_name_dict:
                if 'pcr' in full_samp_name.lower():
                    samp_parts = full_samp_name.split('.')
                    del samp_parts[-1]
                    full_samp_name_for_pcr = '.'.join(samp_parts)
                    if full_samp_name_for_pcr.strip() != samp_name_dict.get(short_match_name).strip():
                        mismatches[short_match_name] = {'sample_metadata': samp_name_dict.get(short_match_name),
                                                        'exp_metadata': full_samp_name}
                else:
                    if full_samp_name.strip() != samp_name_dict.get(short_match_name).strip():
                        mismatches[short_match_name] = {'sample_metadata': samp_name_dict.get(short_match_name),
                                                        'exp_metadata': full_samp_name}
            else:
                not_in_samp_metadata.append(full_samp_name)

    return mismatches, not_in_samp_metadata

def get_wcoa_e_numbers():

    df = pd.read_csv('/home/poseidon/zalmanek/FAIRe-Mapping/projects/FloatingSamples/mixed_sean/data/orphan_faire.csv')
    samps = df['samp_name'].to_list()

    e_nums = []
    for samp in samps:
        if not 'blank' in samp.lower() and '.TN409' in samp:
            e_num = samp.replace('.TN409','').strip()
            e_nums.append(e_num)

    print(e_nums)

def main():

    all_exp_metadata_df = concat_csv_files(Path("/home/poseidon/zalmanek/FAIRe-Mapping/runs"))
    all_samp_metadata_df = concat_csv_files(Path("/home/poseidon/zalmanek/FAIRe-Mapping/projects"))

    mismatches, not_in_samp_metadata = samp_match_dict(exp_metadata_df=all_exp_metadata_df, samp_metadata_df=all_samp_metadata_df)

    with open('/home/poseidon/zalmanek/FAIRe-Mapping/scripts/check_mismatch_samps/mismatch_samp.yaml', 'w') as f:
        yaml.dump(mismatches, f, default_flow_style=False)

    with open('/home/poseidon/zalmanek/FAIRe-Mapping/scripts/check_mismatch_samps/missing_in_samp_metadata.yaml', 'w') as f:
        yaml.dump(not_in_samp_metadata, f, default_flow_style=False)

    # get_wcoa_e_numbers()


if __name__ == "__main__":
    main()