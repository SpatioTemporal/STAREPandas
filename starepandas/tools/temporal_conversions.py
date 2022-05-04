import astropy.time
import pystare


def tivs_from_timeseries(series, scale='utc', format='datetime64', forward_res=48, reverse_res=48):
    """ Converts a timeseries to temporal index values.
    A timeseries is to be understood as either

    - a pandas.Series of dtype('<M8[ns]') as retrieved by pandas.to_datetime() or
    - a pandas.DatetimeArray
    - a 1D numpy.array of dtype('<M8[ns]')

    The forward_res and reverse_res are STARE temporal resolutions. Their ranges are as follows

    ..tabularcolumns::
    +-------------+----------------------------+
    | Resolutions | Unit                       |
    +=============+============================+
    | 48-39       | Millisecond                |
    +-------------+----------------------------+
    | 38-33       | Second                     |
    +-------------+----------------------------+
    | 32-27       | Minute                     |
    +-------------+----------------------------+
    | 26-22       | Hour                       |
    +-------------+----------------------------+
    | 21-19       | Day-of-week                |
    +-------------+----------------------------+
    | 18-17       | Week-of-month              |
    +-------------+----------------------------+
    | 16-13       | Month-of-year              |
    +-------------+----------------------------+
    | 12-00       | Year                       |
    +-------------+----------------------------+

    Parameters
    -----------
    series: array-like
        the series to be converted to tivs
    scale: str
         time scale (e.g., UTC, TAI, UT1, TDB).
         c.f. `astropy.time#scale <https://docs.astropy.org/en/stable/time/index.html#time-scale>`_
    format: str
        time format. c.f. `astropy.time#format <https://docs.astropy.org/en/stable/time/index.html#format>`_
    forward_res: int. Valid range is 0..48
        The forward resolution (c.f pystare.coarsest_resolution_finer_or_equal_ms())
    reverse_res: int. Valid range is 0..48
        The reverse resolution (c.f. pystare.coarsest_resolution_finer_or_equal_ms())

    Returns
    ----------
    tivs: numpy.array
        STARE temporal index values

    Examples
    ------------
    >>> import pandas
    >>> import starepandas
    >>> dates = ['2021-09-03', '2021-07-17 11:16']
    >>> dates = pandas.to_datetime(dates)
    >>> starepandas.tivs_from_timeseries(dates)
    array([2276059438861267137, 2275939265676325057])
    """
    if not series.dtype == '<M8[ns]':
        raise ValueError()
    times = astropy.time.Time(series, scale=scale, format=format)
    tivs = pystare.from_julian_date(times.jd1, times.jd2, scale=scale, forward_res=forward_res, reverse_res=reverse_res)
    return tivs
