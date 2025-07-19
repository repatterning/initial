"""
Module config
"""
import os


class Config:
    """
    Class Config

    For project settings
    """

    def __init__(self):
        """
        Constructor
        """

        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.series_ = os.path.join(self.warehouse, 'data', 'series')

        # Keys
        self.s3_parameters_key = 's3_parameters.yaml'
        self.attributes_key = 'data/daily/attributes.json'
