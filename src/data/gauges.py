"""Module gauges.py"""
import itertools
import os

import dask
import numpy as np
import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.keys


class Gauges:
    """
    Retrieves the catchment & time series codes of the gauges in focus.
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters):
        """

        :param service:
        :param s3_parameters:
        """

        self.__service = service
        self.__s3_parameters = s3_parameters

        self.__objects = src.s3.keys.Keys(service=self.__service, bucket_name=self.__s3_parameters.internal)

    @dask.delayed
    def __get_section(self, listing: str) -> pd.DataFrame:
        """

        :param listing:
        :return:
        """

        catchment_id = os.path.basename(os.path.dirname(listing))

        # The corresponding prefixes
        prefixes = self.__objects.excerpt(prefix=listing, delimiter='/')
        series_ = [os.path.basename(os.path.dirname(prefix)) for prefix in prefixes]

        # A frame of catchment & time series identification codes
        frame = pd.DataFrame(
            data={'catchment_id': itertools.repeat(catchment_id, len(series_)),
                  'ts_id': series_})

        return frame

    def exc(self) -> pd.DataFrame:
        """

        :return:
        """

        listings = self.__objects.excerpt(prefix='data/series/', delimiter='/')

        computations = []
        for listing in listings:
            frame = self.__get_section(listing=listing)
            computations.append(frame)
        frames = dask.compute(computations, scheduler='threads')[0]
        codes = pd.concat(frames, ignore_index=True, axis=0)

        codes['catchment_id'] = codes['catchment_id'].astype(dtype=np.int64)
        codes['ts_id'] = codes['ts_id'].astype(dtype=np.int64)

        return codes
