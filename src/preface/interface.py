"""Module interface.py"""
import typing
import datetime

import boto3

import config
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.functions.service
import src.preface.setup
import src.s3.configurations
import src.s3.s3_parameters


class Interface:
    """
    Interface
    """

    def __init__(self):
        """
        Constructor
        """

        self.__configurations = config.Config()

    def __get_attributes(self, connector: boto3.session.Session) -> dict:
        """

        :return:
        """

        key_name = self.__configurations.attributes_key
        dictionary = src.s3.configurations.Configurations(
            connector=connector).objects(key_name=key_name)

        value = datetime.datetime.now()
        dictionary['starting'] = f'{value.year}-01-01'
        dictionary['ending'] = f'{value.year}-01-01'

        return dictionary

    def exc(self, codes: list[int]) -> typing.Tuple[boto3.session.Session, s3p.S3Parameters, sr.Service, dict]:
        """

        :return:
        """

        connector = boto3.session.Session()
        s3_parameters: s3p.S3Parameters = src.s3.s3_parameters.S3Parameters(connector=connector).exc()
        service: sr.Service = src.functions.service.Service(
            connector=connector, region_name=s3_parameters.region_name).exc()
        attributes: dict = self.__get_attributes(connector=connector)

        if codes is None:
            attributes['excerpt'] = []
        else:
            attributes['excerpt'] = codes

        src.preface.setup.Setup(
            service=service, s3_parameters=s3_parameters).exc()

        return connector, s3_parameters, service, attributes
