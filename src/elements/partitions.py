"""Module partitions.py"""
import typing


class Partitions(typing.NamedTuple):
    """
    The data type class ⇾ Partitions<br><br>

    Attributes<br>
    ----------<br>
    <b>ts_id</b>: int<br>
        The identification code of a time series.<br><br>
    <b>catchment_id</b>: int<br>
        The identification code of a catchment area.<br><br>
    <b>datestr</b>: str<br>
        The <b>date string of the start date of a period</b>, format %Y-%m-%d,  i.e., YYYY-mm-dd.<br><br>
    <b>catchment_size</b>: float<br>
        The size of the catchment the time-series-measuring-station belongs to.<br><br>
    <b>gauge_datum</b>: float<br>
        References:
            <a href="https://timeseriesdoc.sepa.org.uk/api-documentation/before-you-start/what-data-are-available/#Level"
              target="_blank">River Level (Stage)</a>,
            <a href="https://www.sepa.org.uk/environment/water/water-levels/"  target="_blank">Water Levels</a>,
            <a href="https://waterdata.usgs.gov/blog/gage_height/" target="_blank">United States Geological Survey</a><br><br>
    <b>on_river</b>: bool<br>
        An on-river measuring station?<br>
    """

    ts_id: int
    catchment_id: int
    datestr: str
