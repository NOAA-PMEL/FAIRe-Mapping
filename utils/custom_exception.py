class ControlledVocabDoesNotExistError(Exception):
    """
    Exception raised when using a vocabulary word that does not exist in the controlled vocabulary.

    Attributes:
    -----------
    message : str
        Explanation of the error message.

    Methods:
    --------
    __init__(self, message: str):
        Initializes the ControlledVocabDoesNotExistError instance with an error message.

        Parameters:
        -----------
        message : str
            Explanation of the error message.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class NoInsdcGeoLocError(Exception):
    """
    Exception raised when the location does not match any INSDC Geopraphic Locations.

    Attributes:
    -----------
    message : str
        Explanation of the error message.

    Methods:
    --------
    __init__(self, message: str):
        Initializes the NoInsdcGeoLocError instance with an error message.

        Parameters:
        -----------
        message : str
            Explanation of the error message.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class NoAcceptableAssayMatch(Exception):
    """
    Exception raised when there are no acceptable assays that match the input string.

    Attributes:
    -----------
    message : str
        Explanation of the error message.

    Methods:
    --------
    __init__(self, message: str):
        Initializes the NoInsdcGeoLocError instance with an error message.

        Parameters:
        -----------
        message : str
            Explanation of the error message.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)