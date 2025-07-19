"""Module interface.py"""
import logging
import sys

import boto3
import pandas as pd

import src.data.gauges
import src.data.partitions
import src.data.points
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.functions.cache


class Interface:
    """
    Interface
    """

    def __init__(self, connector: boto3.session.Session, service: sr.Service,
                 s3_parameters: s3p.S3Parameters, attributes: dict):
        """

        :param connector: A boto3 session instance, it retrieves the developer's <default> Amazon
                          Web Services (AWS) profile details, which allows for programmatic interaction with AWS.
        :param service:
        :param s3_parameters:
        :param attributes: A set of data acquisition attributes.
        """

        self.__connector = connector
        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__attributes = attributes

    def __filter(self, gauges: pd.DataFrame) -> pd.DataFrame:
        """

        :param gauges:
        :return:
        """

        codes: list = self.__attributes.get('excerpt')

        # Daily
        if len(codes) == 0:
            return gauges

        # Feed
        catchments = gauges.copy().loc[gauges['ts_id'].isin(codes), 'catchment_id'].unique()
        if sum(catchments) == 0:
            src.functions.cache.Cache().exc()
            sys.exit('None of the time series codes is valid')

        _gauges = gauges.copy().loc[gauges['catchment_id'].isin(catchments), :]

        # Logging
        elements = _gauges['ts_id'].unique()
        logging.info('The feed is requesting emergency intelligence for %s gauges, %s.  '
                     'Intelligence is possible for %s gauges, %s', len(codes), codes, elements.shape[0], elements)

        return _gauges

    def exc(self):
        """

        :return:
        """

        # Gauges
        gauges = src.data.gauges.Gauges(service=self.__service, s3_parameters=self.__s3_parameters).exc()
        gauges = self.__filter(gauges=gauges.copy())

        # Partitions for parallel data retrieval; for parallel computing.
        partitions = src.data.partitions.Partitions(data=gauges).exc(attributes=self.__attributes)

        # Retrieving time series points
        src.data.points.Points(connector=self.__connector, period=self.__attributes.get('period')).exc(partitions=partitions)
