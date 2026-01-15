from utils.project_mapper import ProjectMapper
import argparse
import pandas as pd

def fix_zenodo_version(df: pd.DataFrame) -> pd.DataFrame:
    nucl_acid_ext_and_samp_method_updates = {"https://doi.org/10.5281/zenodo.11398154": "https://doi.org/10.5281/zenodo.17655148",
                             "https://doi.org/10.5281/zenodo.15793435": "https://doi.org/10.5281/zenodo.17655184",
                             "https://doi.org/10.5281/zenodo.16850033": "https://doi.org/10.5281/zenodo.17401131",
                             "https://doi.org/10.5281/zenodo.16740832": "https://doi.org/10.5281/zenodo.17655213",
                             "https://doi.org/10.5281/zenodo.11398178": "https://doi.org/10.5281/zenodo.17655027",
                            "https://doi.org/10.5281/zenodo.16755251": "https://doi.org/10.5281/zenodo.17655056"}
    
    df_replaced = df.replace(nucl_acid_ext_and_samp_method_updates)

    return df_replaced

def fix_nc_dates(df: pd.DataFrame) -> pd.DataFrame:

    nc_date_update_mapping_dict = mapping_dict = {
        "E24.NC.DY20-12": {"eventDate": "2020-09-05T02:50:00Z", "verbatimEventDate": "9/5/2020", "verbatimEventTime": "2:50"},
        "E41.NC.DY20-12": {"eventDate": "2020-09-09T22:31:00Z", "verbatimEventDate": "9/9/2020", "verbatimEventTime": "22:31"},
        "E56.NC.DY20-12": {"eventDate": "2020-09-13T08:32:00Z", "verbatimEventDate": "9/13/2020", "verbatimEventTime": "8:32"},
        "E73.NC.DY20-12": {"eventDate": "2020-09-18T08:11:00Z", "verbatimEventDate": "9/18/2020", "verbatimEventTime": "8:11"},
        "E1157.NC.DY22-06": {"eventDate": "2022-05-13T01:59:00Z", "verbatimEventDate": "5/13/2022", "verbatimEventTime": "1:59"},
        "E1717.NC.DY23-06": {"eventDate": "2023-04-24T08:51:00Z", "verbatimEventDate": "4/24/2023", "verbatimEventTime": "8:51"},
        "E232.NC.NO20-01": {"eventDate": "2020-10-06T05:59:00Z", "verbatimEventDate": "10/5/2020", "verbatimEventTime": "21:59"},
        "E301.NC.NO20-01": {"eventDate": "2020-10-20T08:15:00Z", "verbatimEventDate": "10/20/2020", "verbatimEventTime": "0:15"},
        "MID.NC.SKQ21-15S": {"eventDate": "2021-11-14T09:53:49Z", "verbatimEventDate": "11/14/2021", "verbatimEventTime": "0:51"},
        "E2028.NC.SKQ23-12S": {"eventDate": "2023-10-03T11:55:16Z", "verbatimEventDate": "10/3/2023", "verbatimEventTime": "4:02"},
        "E2029.NC.SKQ23-12S": {"eventDate": "2023-09-21T09:49:17Z", "verbatimEventDate": "9/21/2023", "verbatimEventTime": "1:59"},
        "E2030.NC.SKQ23-12S": {"eventDate": "2023-09-14T13:19:49Z", "verbatimEventDate": "9/14/2023", "verbatimEventTime": "5:34"}
    }

    # orient='index' makes the dictionary keys (samp_name) the rows.
    update_df_temp = pd.DataFrame.from_dict(nc_date_update_mapping_dict, orient='index')

    df.set_index('samp_name', inplace=True)
    df.update(update_df_temp)
    df.reset_index(inplace=True)
    return df

def main() -> None:

    parser = argparse.ArgumentParser(description='Process project configuration path for FAIRe mapping of projects for GBIF/OBIS')
    parser.add_argument('gh_token', type=str, help='Github personal access token') 


    args = parser.parse_args()

    project_creator = ProjectMapper(config_yaml="/home/poseidon/zalmanek/FAIRe-Mapping/projects/EcoFoci/faire_project_config.yaml", gh_token=args.gh_token)

    # Calling spearatly because need to fix zenodo links
    sample_df, exp_df = project_creator.process_sample_run_data()

    # Fix Zenodo links
    sample_df_zenodo_fixed = fix_zenodo_version(df=sample_df)
    sample_df_nc_dates_fixed = fix_nc_dates(df=sample_df_zenodo_fixed)

    project_creator.process_whole_project_and_save_to_excel(sample_metadata_df=sample_df_nc_dates_fixed, experiment_run_metadata_df=exp_df)

if __name__ == "__main__":
    main()
    