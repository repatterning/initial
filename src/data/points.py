"""Module points.py"""
import logging
import os
import boto3

import dask
import pandas as pd

import config
import src.data.special
import src.elements.partitions as pr
import src.functions.directories
import src.functions.streams


class Points:
    """
    <b>Notes</b><br>
    ------<br>

    The time series data.
    """

    def __init__(self, connector: boto3.session.Session, period: str):
        """

        :param connector: A boto3 session instance, it retrieves the developer's <default> Amazon
                          Web Services (AWS) profile details, which allows for programmatic interaction with AWS.
        :param period: The data acquisition chunk size, vis-à-vis time
        """

        self.__configurations = config.Config()

        # An instance for reading & writing JSON (JavaScript Object Notation) objects, CSV, ...
        self.__streams = src.functions.streams.Streams()
        self.__directories = src.functions.directories.Directories()
        self.__special = src.data.special.Special(connector=connector)

        # The uniform resource locator, data columns, etc.
        self.__url = ('https://timeseries.sepa.org.uk/KiWIS/KiWIS?service=kisters&type=queryServices&datasource=0'
                      '&request=getTimeseriesValues&ts_id={ts_id}'
                      f'&period={period}'
                      '&from={datestr}&returnfields=Timestamp,Value,Quality Code&metadata=true'
                      '&md_returnfields=ts_id,ts_name,ts_unitname,ts_unitsymbol,station_id,'
                      'catchment_id,parametertype_id,parametertype_name,river_name&dateformat=UNIX&format=json')

        self.__rename = {'Timestamp': 'timestamp', 'Value': 'value', 'Quality Code': 'quality_code'}

    @dask.delayed
    def __get_data(self, url: str):
        """

        :param url:
        :return:
        """

        parts = self.__special.exc(url=url)

        # The data in data frame form
        columns = parts[0]['columns'].split(',')
        frame = pd.DataFrame.from_records(data=parts[0]['data'], columns=columns)
        frame.rename(columns=self.__rename, inplace=True)

        # The identification codes of the time series
        frame = frame.assign(ts_id=parts[0]['ts_id'])

        return frame

    @dask.delayed
    def __persist(self, data: pd.DataFrame, partition: pr.Partitions) -> str:
        """

        :param data:
        :param partition:
        :return:
        """

        directory = os.path.join(self.__configurations.series_, str(partition.catchment_id), str(partition.ts_id))
        self.__directories.create(path=directory)

        message = self.__streams.write(
            blob=data, path=os.path.join(directory, f'{partition.datestr}.csv'))

        return message

    def exc(self, partitions: list[pr.Partitions]):
        """

        :param partitions: ts_id, datestr, catchment_size, gauge_datum, on_river
        :return:
        """

        computations = []
        for partition in partitions:
            url = self.__url.format(ts_id=partition.ts_id, datestr=partition.datestr)
            data = self.__get_data(url=url)
            message = self.__persist(data=data, partition=partition)
            computations.append(message)
        calculations = dask.compute(computations, scheduler='threads')[0]

        logging.info(calculations)
