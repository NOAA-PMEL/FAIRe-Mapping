from faire_mapping.dataframe_and_dict_builders.base_df_builder import BaseDfBuilder
from faire_mapping.utils import fix_cruise_code_in_samp_names


# TODO: remove samp_name samp_name_col and samp_stor_dur_col from config.yaml file samp_store_dur_info because these will always be consistent - will standardize these fields, but need to keep samp_dur in the config.yaml
class SampleStoreBuilder(BaseDfBuilder):

    SAMP_NAME_COL_NAME = "FINAL Sample NAME" # Will always stay this if we make this again. Righ now in config.yaml, but can move out
    SAMP_STORE_DUR_COL_NAME = "sample_stor_dur" # Will always stay this if we make this again. Righ now in config.yaml, but can move out

    def __init__(self, unwanted_cruise_code: str, desired_cruise_code: str, header: int = 0, google_sheet_id: str = None, json_creds_path: str = None, sheet_name='Sheet1'):
        
        super().__init__(header=header, sep=',', csv_path=None, google_sheet_id=google_sheet_id, json_creds_path=json_creds_path, sheet_name=sheet_name)

        self.unwanted_cruise_code = unwanted_cruise_code
        self.desired_cruise_code = desired_cruise_code
        self.samp_store_dur_dict = self.create_samp_store_dict()

    def create_samp_store_dict(self) -> dict:
        """
        Creates a dictionary of the sample names and their duration. Based on the google sheet created for this.
        """

        # fix sample names if cruise-code was corrrected in sample names
        if self.unwanted_cruise_code and self.desired_cruise_code:
            self.df = fix_cruise_code_in_samp_names(df=self.df, 
                                                        unwanted_cruise_code=self.unwanted_cruise_code,
                                                        desired_cruise_code=self.desired_cruise_code,
                                                        sample_name_col=self.SAMP_NAME_COL_NAME)
        samp_dur_dict = dict(zip(self.df[self.SAMP_NAME_COL_NAME], self.df[self.SAMP_STORE_DUR_COL_NAME]))

        return samp_dur_dict