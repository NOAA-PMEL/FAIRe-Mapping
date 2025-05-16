from .faire_mapper import OmeFaireMapper

class ProjectMapper(OmeFaireMapper):
    
    def __init__(self, config_yaml: str):

        super().__init__(config_yaml)

        self.config_file = self.load_config(config_yaml)