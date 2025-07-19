"""Module partitions.py"""
import datetime

import dask
import pandas as pd

import src.elements.partitions as pr


class Partitions:
    """
    Partitions for parallel computation.
    """

    def __init__(self, data: pd.DataFrame):
        """

        :param data:
        """

        self.__data = data

        # Fields
        self.__fields = ['ts_id', 'catchment_id', 'datestr']

    @dask.delayed
    def __matrix(self, start: str) -> list:
        """

        :param start: The date string of the start date of a period; format YYYY-mm-dd.
        :return:
        """

        data = self.__data.copy()

        data = data.assign(datestr = str(start))
        records: pd.DataFrame = data[self.__fields]
        objects: pd.Series = records.apply(lambda x: pr.Partitions(**dict(x)), axis=1)

        return objects.tolist()

    def exc(self, attributes: dict) -> list[pr.Partitions]:
        """

        :param attributes:
        :return:
        """

        # The boundaries of the dates; datetime format
        starting = datetime.datetime.strptime(attributes.get('starting'), '%Y-%m-%d')
        ending = datetime.datetime.strptime(attributes.get('ending'), '%Y-%m-%d')

        # Create series
        frame = pd.date_range(start=starting, end=ending, freq=attributes.get('frequency')
                              ).to_frame(index=False, name='date')
        starts: pd.Series = frame['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        # Compute partitions matrix
        computations = []
        for start in starts.values:
            matrix = self.__matrix(start=start)
            computations.append(matrix)
        calculations = dask.compute(computations, scheduler='threads')[0]

        return sum(calculations, [])
