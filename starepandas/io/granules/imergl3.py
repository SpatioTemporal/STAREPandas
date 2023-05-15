#! /usr/bin/env python -tt
# -*- coding: utf-8; mode: python -*-
"""General expansion of the Granule super-class specific to reading data from (or related to) NASA's Integrated Multi-satellitE Retrievals for GPM (IMERG, L3)

imergL3
~~~~~~~

IMERG is part of NASA's Global Precipitation Measurement Mission (GPM).
"""
# Standard Imports
# import os
from typing import Optional, Union
from datetime import datetime, timezone, timedelta
from string import Formatter

# Third-Party Imports
import numpy
import pandas
import numpy.typing as npt

# STARE Imports
from starepandas.io.granules.granule import Granule
import starepandas
import pystare

# Local Imports

##
# List of Public objects from this module.
__all__ = ['L3IMERG', 'DYAMONDv2']

##
# Markup Language Specification (see Numpydoc Python Style Guide https://numpydoc.readthedocs.io/en/latest/format.html)
__docformat__ = "Numpydoc"
# ------------------------------------------------------------------------------

# Define Global Constants and State Variables
# -------------------------------------------


###############################################################################
# PUBLIC Class: L3IMERG
# ---------------------
class L3IMERG(Granule):
    """Derived-class of the Granule super-class specific to reading data from NASA's IMERG L3 products.

    Parameters
    ----------
    Granule : STAREPandas.io.granules.granule.Granule
        General Granule super-class for interacting with data files.
    """

    ###########################################################################
    # PRIVATE Instance-Constructor: __init__()
    # ----------------------------------------
    def __init__(self, file_path: str, sidecar_path: Optional[Union[str, None]]=None, nom_res: Optional[Union[str, None]]=None):
        """Instance-Constructor for L3IMERG class (granule loader).

        Parameters
        ----------
        file_path : str
            Path to granule file you intend to read.
        sidecar_path : Optional[Union[str, None]]
            Path to STARE sidecar file associated with this granule. Defaults to None.
        nom_res : Optional[Union[str, None]]
            Specifies the resolution to read for multi-resolution products (passed to set_nom_res()). Defaults to None.

        Based on Granule.__init__() and Granule.read_sidecar() this instance (self) defines the following attributes (in addition to the Parameters listed above):

        Attributes
        ----------
        file_path : str
        sidecar_path : Union[str, None]
        nom_res : Union[str, None]
        companion_prefix : None
            Used by self.guess_companion_path() to guess a granule's (file_path) companion-file path. Defaults to None.
        data : dict
            Dictionary holding the granule data used to populate a STAREPandas dataframe. Defaults to {}.
        lat : Union[npt.ArrayLike, None]
            Array holding latitude values for the data (populated by self.read_sidecar_latlon() or user provided). Defaults to None.
        lon : Union[npt.ArrayLike, None]
            Array holding longitude values for the data (populated by self.read_sidecar_latlon() or user provided). Defaults to None.
        sids : Union[npt.ArrayLike, None]
            Array holding STARE indices (SIDs) for the data (populated by self.add_sids(), self.read_sidecar_index() or user provided). Defaults to None.
        stare_cover : Union[npt.ArrayLike, None]
            Array holding STARE cover SIDs for the data (populated by self.read_sidecar_cover() or user provided). Defaults to None.
        ts_start : Union[datetime, None]
            Datetime object holding the start time for the data (populated by user provided). Defaults to None.
        ts_end : Union[datetime, None]
            Datetime object holding the end time for the data (populated by user provided). Defaults to None.
        """
        ##
        # Use Granule.__init__() to construct this instance (self).
        super().__init__(file_path, sidecar_path, nom_res)
        # print(f"L3IMERG(): {self.file_path}    = ")
        # print(f"L3IMERG(): {self.sidecar_path} = ")
        # print(f"L3IMERG(): {self.nom_res = }")

        ##
        # Opens file_path using netCDF4
        self.netcdf = starepandas.io.s3.nc4_dataset_wrapper(self.file_path, 'r', format='NETCDF4')
        # print(f"L3IMERG(): {self.netcdf = }")

    ###########################################################################
    # PUBLIC Instance-Method: read_timestamps()
    # -----------------------------------------
    def read_timestamps(self):
        """Read the time stamp(s) from the netCDF4 file and returns them as a pandas.DatetimeIndex.

        For IMERG a single day consists of 48 half-open intervals centered on the hour or half-hour [1]_: `['00:00', '00:30', ... '23:00', '23:30']`.

        The half-open intervals are such that the Lower Bound (LB), Center (CR) and Upper Bound (UB) of the interval are written as `(LB CR UB]` or `LB < CR <= UB`.

        For example,

        .. code-block:: text

            (00:15 00:30 00:45]
                              (00:45 01:00 01:15]


        In practice, a 1 millisecond forward offset is used to ensure that the LB is half-open or `((CR - 15m) + 1ms, CR, CR + 15m]`.

        For example,

        .. code-block:: text

            (00:15.001 00:30 00:45]
                                  (00:45.001 01:00 01:15]


        Attributes
        ----------
        ts_start : Union[datetime, None]
            Datetime object holding the start time for the data. Defaults to None.
        ts_end : Union[datetime, None]
            Datetime object holding the end time for the data. Defaults to None.

        References
        ----------
        .. [1] Integrated Multi-satellitE Retrievals for GPM (IMERG) Technical Documentation: https://docserver.gesdisc.eosdis.nasa.gov/public/project/GPM/IMERG_doc.06.pdf
        """
        # To be defined in derived classes.
        pass

    ###########################################################################
    # PUBLIC Instance-Method: read_latlon()
    # ------------------------------------~
    def read_latlon(self):
        """Read and return the datagrid longitude and latitude from the netCDF4 file.

        Attributes
        ----------
        lat : Union[npt.ArrayLike, None]
            Array holding latitude values for the data. Defaults to None.
        lon : Union[npt.ArrayLike, None]
            Array holding longitude values for the data. Defaults to None.
        """
        # To be defined in derived classes.
        pass


###############################################################################
# PUBLIC Class: DYAMONDv2
# -----------------------
class DYAMONDv2(L3IMERG):
    """Special case of L3IMERG for simulated IMERG precipitation files.

    Notes
    -----
    * Base class for IMERG granule (STAREPandas.io.granules.imerg.L3IMERG), which is derived from Granule super-class.
    * Spatial Resolution   : IMERG 0.1 x 0.1 grid.
    * Time Resolution      : Instantaneous sample every 30 minutes, rather than 30 minute average typical of IMERG data.
    * Relationship to IMERG: Free-run simulation, which diverges from reality after a few days.
    * Provided by          : Jiun-Dar Chern, ESSIC/UMD & MESOSCALE ATMOSPHERIC PROCESSES LAB, NASA/GSFC Code 612, Greenbelt, MD 20771, E-mail: jiun-dar.chern-1@nasa.gov
    * File name example    : 'DYAMONDv2_PE3600x1800-DE.prectot.20200116_0000z.nc4'

    Attributes
    ----------
    file_path : str
        Path to granule file you intend to read.
    sidecar_path : Union[str, None]
        Path to STARE sidecar file associated with this granule. Defaults to None.
    nom_res : Union[str, None]
        Specifies the resolution to read for multi-resolution products (passed to set_nom_res()). Defaults to None.
    companion_prefix : None
        Used by self.guess_companion_path() to guess the path to a granule's (file_path) companion file. Defaults to None.
    data : dict
        Dictionary holding the data read from the granule file. Used to populate a STAREPandas dataframe. Defaults to {}.
    lat : ndarray(dtype=float64, ndim=2)
        Array holding latitude values for the data (populated by self.read_sidecar_latlon() or user provided). Defaults to None.
    lon : ndarray(dtype=float64, ndim=2)
        Array holding longitude values for the data (populated by self.read_sidecar_latlon() or user provided). Defaults to None.
    sids : Union[npt.ArrayLike, None]
        Array holding STARE indices (SIDs) for the data (populated by self.add_sids(), self.read_sidecar_index() or user provided). Defaults to None.
    stare_cover : Union[npt.ArrayLike, None]
        Array holding STARE cover SIDs for the data (populated by self.read_sidecar_cover() or user provided). Defaults to None.
    ts_start : Union[datetime, None]
        Datetime object holding the start time for the data (populated by user provided). Defaults to None.
    ts_end : Union[datetime, None]
        Datetime object holding the end time for the data (populated by user provided). Defaults to None.
    imerg_tstep : int
        IMERG time step (units milliseconds).
    imerg_halfstep : pandas.Timedelta
        IMERG half-step as a pandas.Timedelta (units nanoseconds).
    imerg_time_res : int64
        STARE time resolution for IMERG data.
    iso8601_HRF_0 : str
        ISO8601 human readable format '%Y-%m-%d'
    iso8601_HRF_1 : str
        ISO8601 human readable format '%Y-%m-%d %H:%M:%S'
    iso8601_HRF_3 : str
        ISO8601 human readable format '%Y-%m-%d %H:%M:%S.%f'
    iso8601_HRF_4 : str
        ISO8601 human readable format '%Y-%m-%d %H:%M:%S.%f%z'
    """


    ###########################################################################
    # PRIVATE Instance-Constructor: __init__()
    # ----------------------------------------
    def __init__(self, file_path: str, sidecar_path: Optional[Union[str, None]]=None, nom_res: Optional[Union[str, None]]=None):
        """Instance-Constructor for class DYAMONDv2.

        Parameters
        ----------
        file_path : str)
            Path to data you intend to read.
        sidecar_path : Optional[Union[str, None]]
            Path to STARE sidecar file for this data. Defaults to None.
        nom_res : Optional[Union[str, None]]
            String holding the STARE spatial resolution to use for encoding. Defaults to None.
        """
        verbose = [False, True][1]
        header = "DYAMONDv2.__init__():"
        ##
        # Use L3IMERG.__init__(), which calls Granule.__init__() for this instance (self)
        super().__init__(file_path, sidecar_path=sidecar_path, nom_res=nom_res)
        if verbose:
            print(f"{header:<30s} file_path      = {self.file_path}")
            print(f"{header:<30s} sidecar_path   = {self.sidecar_path}")
            print(f"{header:<30s} nom_res        = {self.nom_res}")

        ##
        # Define some datetime formats
        self.iso8601_HRF_0 = '%Y-%m-%d'
        self.iso8601_HRF_1 = '%Y-%m-%d %H:%M:%S'
        self.iso8601_HRF_3 = '%Y-%m-%d %H:%M:%S.%f'
        self.iso8601_HRF_4 = '%Y-%m-%d %H:%M:%S.%f%z'

        ##
        # IMERG time interval info
        # self.imerg_tstep = 1800000    # 30 minutes in milliseconds
        self.imerg_tstep = 120000     # 2 minutes in milliseconds
        halfstep = 60000000000        # 1 minutes in nanoseconds
        self.imerg_halfstep = pandas.Timedelta(halfstep, unit='ns')
        if verbose:
            print(f"{header:<30s} imerg_tstep    = {self.imerg_tstep} ms")
            print(f"{header:<30s} imerg_halfstep = {self.imerg_halfstep} ns")

        ##
        # Determine the STARE time resolution
        times = numpy.array([self.imerg_tstep], dtype=numpy.int64)
        time_res_array = pystare.coarsest_resolution_finer_or_equal_ms(times)
        self.imerg_time_res = int(time_res_array[0])
        if verbose:
            print(f"{header:<30s} imerg_time_res = {self.imerg_time_res} for a time interval of {self.strfdelta(timedelta(milliseconds=self.imerg_tstep), '{H:02}h{M:02}m{S:02}s')}")


    ###########################################################################
    # PUBLIC Instance-Method: strfdelta()
    # -----------------------------------
    def strfdelta(self, tdelta: timedelta, fmt: str) -> str:
        """Convert a timedelta object tdelta into a string according to the format string fmt.

        Parameters
        ----------
        tdelta : datetime.timedelta
            The timedelta object to convert.
        fmt : str
            String defining the format of the output string.

        Returns
        -------
        str
            The formatted string.
        """
        f = Formatter()
        d = {}
        l = {'D': 86400, 'H': 3600, 'M': 60, 'S': 1}
        k = list(map( lambda x: x[1], list(f.parse(fmt))))
        rem = int(tdelta.total_seconds())
        for i in ('D', 'H', 'M', 'S'):
            if i in k and i in l.keys():
                d[i], rem = divmod(rem, l[i])
        return f.format(fmt, **d)

    ###########################################################################
    # PUBLIC Instance-Method: read_timestamps()
    # -----------------------------------------
    def read_timestamps(self):
        """Read the temporal data from the netCDF4 file and returns them as a pandas.DatetimeIndex.

        DYAMONDv2 follows the IMERG pattern a single day consists of 48 daily samples centered on the hour or half-hour: `['00:00', '00:30', ... '23:00', '23:30']`.

        Unlike IMERG, DYAMONDv2 data **represent instantaneous values not an interval**.

        In practice, a narrow interval (1 minute) is created by adding a small offset around the center datetime (CR) or `[(CR - 1m), CR, CR + 1m]`.

        For example,

        .. code-block:: text

            [23:59 00:00 00:01]
                                [00:29 00:30 00:31]
                                                    [00:59 01:00 01:01]


        Parameters
        ----------
        cr_ts_str : str
            String holding the center time stamp (e.g., '2020-01-16 00:00:00').
        self.ts_end, interval_tid_cover : int64
            STARE TID (cover) for the time interval.
        """
        verbose = [False, True][0]
        header = "DYAMONDv2.read_timestamps():"

        ##
        # Pull time units, which is the time interval CR value as a string.
        t_units = self.netcdf['time'].units
        # Add nanoseconds
        # t_units = t_units[14:] + ".000000"
        # >>>> 2020-01-16 00:00:00.000000
        if verbose:
            print(f"{header:<30s} t_units             = {t_units}")

        ##
        # Use time units string as the CR datetime.
        self.cr_ts_str = t_units[14:]
        if verbose:
            print(f"{header:<30s} cr_ts_str           = {self.cr_ts_str}")

        ##
        # Form the CR datetime
        cr_ts = datetime.strptime(self.cr_ts_str, '%Y-%m-%d %H:%M:%S')
        if verbose:
            print(f"{header:<30s} cr_ts               = {cr_ts}, {type(cr_ts)}")

        ##
        # Make it a Pandas TimeStamp
        self.ts_start = cr_dt = pandas.to_datetime(cr_ts, utc=False, unit='ns')
        if verbose:
            print(f"{header:<30s} cr_dt               = {cr_dt}, {type(cr_dt)}")

        ##
        # Find the closed Lower Bound (LB)
        lb_ts = cr_ts - self.imerg_halfstep
        lb_dt = pandas.to_datetime(lb_ts, utc=False, unit='ns')
        if verbose:
            print(f"{header:<30s} lb_ts               = {lb_ts}, {type(lb_ts)}")
            print(f"{header:<30s} lb_dt               = {lb_dt}, {type(lb_dt)}")

        ##
        # Find the closed Upper Bound (UB)
        ub_ts = cr_ts + self.imerg_halfstep
        ub_dt = pandas.to_datetime(ub_ts, utc=False, unit='ns')
        if verbose:
            print(f"{header:<30s} ub_ts               = {ub_ts}, {type(ub_ts)}")
            print(f"{header:<30s} ub_dt               = {ub_dt}, {type(ub_dt)}")

        ##
        # Create an interval
        interval = [lb_dt, cr_dt, ub_dt]
        if verbose:
            print(f"{header:<30s} interval            = {interval}, {type(interval)}")

        ##
        # Convert interval to numpy datetimes
        interval_npdt = numpy.array(interval, dtype='datetime64[ms]')
        if verbose:
            print(f"{header:<30s} interval_npdt       = {interval_npdt.tolist()}, {type(interval_npdt[0]) = }")

        ##
        # Convert to 64-bit integer representation (based on milliseconds & UTC)
        interval_npdt_int = numpy.array(interval, dtype='datetime64[ms]').astype(numpy.int64)
        if verbose:
            print(f"{header:<30s} interval_npdt_int   = {interval_npdt_int.tolist()}, {type(interval_npdt_int[0]) = }")

        ##
        # Convert integer representation to TID triplet
        imerg_time_res_arr = numpy.array([self.imerg_time_res, self.imerg_time_res, self.imerg_time_res], dtype=numpy.int64)
        interval_tid_triple = pystare.from_utc_variable(interval_npdt_int, imerg_time_res_arr, imerg_time_res_arr)
        if verbose:
            print(f"{header:<30s} interval_tid_triple = {interval_tid_triple.tolist()}, {type(interval_tid_triple[0]) = }")

        ##
        # Calculate an interval TID (cover).
        self.ts_end = self.interval_tid_cover = pystare.from_temporal_triple(interval_tid_triple)[0]
        if verbose:
            print(f"{header:<30s} interval_tid_cover  = {self.interval_tid_cover.tolist()}, {type(self.interval_tid_cover) = }")

        if verbose:
            ##
            # Summary
            print(f"{header:<30s} Summary for {self.cr_ts_str}")
            print(f"{'':<35s}TimeStamps    ({interval[0].strftime('%Y-%m-%d %H:%M:%S.%f'):<26} <-> {interval[1].strftime('%Y-%m-%d %H:%M:%S.%f'):^26} <-> {interval[2].strftime('%Y-%m-%d %H:%M:%S.%f'):>26})")
            print(f"{'':<35s}TID triple    ({interval_tid_triple[0]:<26d} <-> {interval_tid_triple[1]:^26d} <-> {interval_tid_triple[2]:>26d})")
            print(f"{'':<35s}TID cover     ({'':<26s}     {self.interval_tid_cover:^26d}     {'':>26s})")
            print(f"{'':<35s}TID cover hex ({'':<26s}     {hex(self.interval_tid_cover):^26}     {'':>26s})")
            print(f"{'':<35s}TID cover bin ({'':<10s}0b{numpy.binary_repr(self.interval_tid_cover, width=64):64s}{'':>12s})")


    ###########################################################################
    # PUBLIC Instance-Method: read_latlon()
    # ------------------------------------~
    def read_latlon(self):
        """Read lat/lon from netCDF file.

        Parameters
        ----------
        lon : ndarray(dtype=float64, ndim=2)
            Longitude values as a numpy.meshgrid('xy') with the shape (nlat, nlon).
        lat : ndarray(dtype=float64, ndim=2)
            Latitude values as a numpy.meshgrid('xy') with the shape (nlat, nlon).
        """
        verbose = [False, True][0]
        header = "DYAMONDv2.read_latlon():"

        # self.lat = self.netcdf['lat'][:].astype(numpy.double)
        # self.lon = self.netcdf['lon'][:].astype(numpy.double)
        # if verbose:
        #     print(f"{header:<30s} self.lat ({len(self.lat)}) = [{self.lat[0]:+8.3f} ... {self.lat[-1]:+8.3f}]")
        #     print(f"{header:<30s} self.lat ({len(self.lon)}) = [{self.lon[0]:+8.3f} ... {self.lon[-1]:+8.3f}]")

        ##
        # Fetch lat/lon arrays
        lat = self.netcdf['lat'][:].astype(numpy.double)
        lon = self.netcdf['lon'][:].astype(numpy.double)
        if verbose:
            print(f"{header:<30s} lat ({len(lat)}) = [{lat[0]:+8.3f} ... {lat[-1]:+8.3f}]")
            print(f"{header:<30s} lat ({len(lon)}) = [{lon[0]:+8.3f} ... {lon[-1]:+8.3f}]")

        ##
        # Make a (lat=1800, lon=3600) mesh grid to match the sidecar SIDs array
        #   self.sids.shape = (1800, 3600)
        lon, lat = numpy.meshgrid(lon, lat, copy=True, indexing='xy')
        # if verbose:
        #     print(f"{header:<30s} lat {lat.shape}")
        #     print(f"{header:<30s} lon {lon.shape}")
        #     lat2 = lat.flatten()
        #     print(f"{header:<30s} lat2 {lat2.shape}")
        self.lon = lon
        self.lat = lat
        if verbose:
            lat2 = lat.flatten()
            print(f"{header:<30s} self.lat {self.lat.shape} or {lat2.shape}")
            print(f"{header:<30s} self.lon {self.lon.shape} or {lat2.shape}")


# >>>> ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: <<<<
# >>>> END OF FILE | END OF FILE | END OF FILE | END OF FILE | END OF FILE | END OF FILE <<<<
# >>>> ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::: <<<<
