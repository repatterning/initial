"""Module specific.py"""
import argparse

import src.functions.cache


class Specific:
    """
    Specific
    """

    def __init__(self):
        pass

    @staticmethod
    def codes(value: str=None) -> list[int]:
        """

        :param value:
        :return:
        """

        if value is None:
            return []

        # Split and strip
        elements = [e.strip() for e in value.split(',')]

        try:
            _codes = [int(element) for element in elements]
        except argparse.ArgumentTypeError as err:
            src.functions.cache.Cache().exc()
            raise err from err

        return _codes
