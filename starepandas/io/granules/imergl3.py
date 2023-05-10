#! /usr/bin/env python -tt
# -*- coding: utf-8; mode: python -*-
r"""

imergL3
~~~~~~~

General expansion of the Granule super-class specific to reading data from (or related to) NASA's Integrated Multi-satellitE Retrievals for GPM (IMERG, L3)
    Which is part of NASA's Global Precipitation Measurement Mission (GPM).
"""
# Standard Imports
import os
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
# Markup Language Specification (see Google Python Style Guide https://google.github.io/styleguide/pyguide.html)
__docformat__ = "Google en"
# ------------------------------------------------------------------------------

# Define Global Constants and State Variables
# -------------------------------------------


###############################################################################
# PUBLIC Class: L3IMERG
# ---------------------
class L3IMERG(Granule):
    """Derived-class of the Granule super-class specific to reading data from NASA's Integrated Multi-satellitE Retrievals for GPM (IMERG, L3).
    """

    ###########################################################################
    # PRIVATE Instance-Constructor: __init__()
    # ----------------------------------------
    def __init__(self, file_path: str, sidecar_path: Optional[Union[str, None]]=None, nom_res: Optional[Union[str, None]]=None):
        """_summary_

        Args:
            file_path (str): Path to data you intend to read.
            sidecar_path (Optional[Union[str, None]], optional): Path to STARE sidecar file for this data. Defaults to None.
            nom_res (Optional[Union[str, None]], optional): String holding the STARE spatial resolution to use for encoding. Defaults to None.
        """

        # Use Granule.__init__() for this instance (self)
        # Sets
        #   self.file_path = file_path
        #   self.sidecar_path = sidecar_path
        #   self.data = {}
        #   self.lat = None
        #   self.lon = None
        #   self.sids = None
        #   self.stare_cover = None
        #   self.ts_start = None
        #   self.ts_end = None
        #   self.nom_res = None
        #   self.companion_prefix = None
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

        IMERG/XCAL
            For IMERG a single day consists of 48 half-open intervals centered on the hour or half-hour:
                ['00:00', '00:30', ... '23:00', '23:30']

            The half-open intervals are such that the Lower Bound (LB), Center (CR) and Upper Bound (UB)
            of the interval are written as '(LB CR UB]' or 'LB < CR <= UB'.

                For example, (00:15 00:30 00:45]
                                               (00:45 01:00 01:15]

            In practice, a 1 millisecond forward offset is used to ensure that the LB is
            half-open or ((CR - 15m) + 1ms, CR, CR + 15m].

                For example, (00:15.001 00:30 00:45]
                                                   (00:45.001 01:00 01:15]

        Reference: Integrated Multi-satellitE Retrievals for GPM (IMERG) Technical Documentation
                   https://docserver.gesdisc.eosdis.nasa.gov/public/project/GPM/IMERG_doc.06.pdf
        """
        pass

    ###########################################################################
    # PUBLIC Instance-Method: read_latlon()
    # ------------------------------------~
    def read_latlon(self):
        """_summary_
        """
        pass

###############################################################################
# PUBLIC Class: DYAMONDv2
# -----------------------
class DYAMONDv2(L3IMERG):
    """Special case for simulated IMREG precipitation files.

    Args:
        L3IMERG (_type_): General IMERG derived-Class based on the Granule super-class.

    Notes:
        Spatial Resolution   : IMERG 0.1x0.1 grid.
        Time Resolution      : Instantaneous 30 min sample, rather than 30 min average.
        Relationship to IMERG: Free-run simulation so diverge from reality after a few days.

        Provided by:
            Jiun-Dar Chern
                ESSIC/UMD & MESOSCALE ATMOSPHERIC PROCESSES LAB
                NASA/GSFC Code 612
                Greenbelt, MD 20771
                Phone: (301) 614-6175
                Fax:   (301) 614-5492
                E-mail: jiun-dar.chern-1@nasa.gov

        File name example:
            DYAMONDv2_PE3600x1800-DE.prectot.20200116_0000z.nc4
    """

    ###########################################################################
    # PRIVATE Instance-Constructor: __init__()
    # ----------------------------------------
    def __init__(self, file_path: str, sidecar_path: Optional[Union[str, None]]=None, nom_res: Optional[Union[str, None]]=None):
        """Instance-Constructor for class DYAMONDv2.

        Args:
            file_path (str): Path to data you intend to read.
            sidecar_path (Optional[Union[str, None]], optional): Path to STARE sidecar file for this data. Defaults to None.
            nom_res (Optional[Union[str, None]], optional): String holding the STARE spatial resolution to use for encoding. Defaults to None.

        Defines:
            self.imerg_tstep                 (int): IMERG time step (units milliseconds).
            self.imerg_halfstep (pandas.Timedelta): IMERG half-step as a pandas.Timedelta (units nanoseconds).
            self.imerg_time_res      (numpy.int64): STARE time resolution for IMERG data.

        STARE temporal resolutions: Resolutions go from 0 being coarsest to 48 being the finest.
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

        STARE uses ISO 8601 timestamps
        ------------------------------
        An international standard for the exchange and communication of date and time-related data.
            * Applies to dates (Gregorian calendar)
            * Times (24-hour timekeeping system)
                * Optional UTC offset/timezone

        strftime Formating: https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
            * %Y    The year as a whole number including the century (range 0001 to 9999).
            * %m    The month as a whole number (range 01 to 12).
            * %d    The day of the month as a whole number (range 01 to 31).
            * %H    The hour as a whole number using a 24-hour clock (range 00 to 23).
            * %M    The minute as a whole number (range 00 to 59).
            * %S    The second as a whole number (range 00 to 60).
            * %f    The microsecond as a whole number, zero-padded to 6 digits (000000, 000001, ... 999999).
                        - Milliseconds would be the first 3 digits (000, 001, ... 999)

        Time zone aware formatting:
            * For a non-time-zone aware or 'native' object, the %z and %Z format codes are replaced by empty strings.
            * %z    The UTC offset (the signed hour, minute [second, millisecond] offset from UTC in the form +/-HHMM[SS[.ffffff]] (+0000, -0400, +1030, +063415, -030712.345216)
                        - 'HH' is a 2-digit string giving the number of UTC offset hours.
                        - 'MM' is a 2-digit string giving the number of UTC offset minutes.
                        - 'SS' is a 2-digit string giving the number of UTC offset seconds.
                        - 'ffffff' is a 6-digit string giving the number of UTC offset microseconds.
            * %Z    The timezone abbreviation (UTC, GMT).

        Human-Readable Format (8601_HRF):
            8601_HRF_0  '%Y-%m-%d'                  2010-12-16                          8601 basic format, date only.
            8601_HRF_1  '%Y-%m-%d %H:%M:%S'         2010-12-16 17:22:15                 ISO 8601 with date and time (w.out time-zone or sub-seconds).
            8601_HRF_2  '%Y-%m-%dT%H:%M:%S'         2010-12-16T17:22:15                 Same as 8601_HRF_1 with the optional date-time separator (default 'T').
            8601_HRF_3  '%Y-%m-%d %H:%M:%S.%f'      2010-12-16 17:22:15.000000          Same as 8601_HRF_1 with microsecond or millisecond encoding.
                                                    2010-12-16 17:22:15.000
            8601_HRF_4  '%Y-%m-%d %H:%M:%S.%f%z'    2010-12-16 17:22:15.000000+00:00    Same as 8601_HRF_3, with UTC encoding (here no offset).

        Compact Integer Format (CIF)
            8601_CIF_0  '%Y%m%d'                    20101216                            8601_HRF_0 without spaces or separators.
            8601_CIF_1  '%Y%m%d%H%M%S'              20101216172215                      8601_HRF_1 without spaces or separators.
            8601_CIF_2  '%Y%m%dT%H%M%S'             20101216T172215                     8601_HRF_2 without spaces.
            8601_CIF_3  '%Y%m%d%H%M%S.%f'           20101216172215.000000               8601_HRF_3 without spaces or separators.
                                                    20101216172215.000
            8601_CIF_4  '%Y%m%dT%H%M%S.%f'          20101216T172215.000000              8601_CIF_3 with date-time separator.

        """
        verbose = [False, True][1]
        if verbose:
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
    def strfdelta(self, tdelta, fmt):
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

        Defines:
            self.cr_ts_str                  (str): Strings holding the center time stamp (e.g., '2020-01-16 00:00:00').
            self.interval_tid_cover (numpy.int64): STARE TID (cover) for the time interval.

        STARE spatial index value (SID)
        STARE temporal index value (TID or TIV)

        DYAMONDv2:
            Follows the IMERG pattern a single day consists of 48 daily samples centered on the hour or half-hour:
                ['00:00', '00:30', ... '23:00', '23:30']

            Unlike IMERG, DYAMONDv2 data represent instantaneous values not an interval.

            In practice, a narrow interval (1 minute) is created by adding a small offset around the center datetime (CR).
                [(CR - 1m), CR, CR + 1m].

            For example,
                [23:59 00:00 00:01]
                                    [00:29 00:30 00:31]
                                                        [00:59 01:00 01:01]


        This is likely because there is a finite, irregular resolution with which intervals are covered with STARE tids.
        The forward and reverse resolutions each point to a bit in the representation.
        When a tid is constructed from a triple (t0,t1,t2), t1 is used to set the temporal location bits.

        Then the reverse resolution is set so that a decrement associated with that resolution's bit position is less than t0 (for the case where a cover is desired).

        Similarly, the forward resolution is set so that an increment at the associated bit position is greater than t1.

        Therefore, the actual lower and upper bounds that one can calculate from the above tid (a single 64-bit integer) are generally not t0 and t2.

        Generally, we've set the tid to cover the time interval of the granule, which leads to "overestimates" in joins or search queries.

        One can check this by "round-tripping" from a temporal triple to a tid/tiv and back again.

        This also means that when doing searches, the results using only the tid will be approximate, and there will need to be another step or pass for a more accurate comparison.

        Part of this is due to putting three temporal instants into a single 64-bit integer. (If I'm gauging your issue correctly.)

        """
        verbose = [False, True][0]
        if verbose:
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

        Defines:
            self.lon (numpy.ndarray): Longitude values as a numpy.meshgrid('xy') with the shape (nlat, nlon).
            self.lat (numpy.ndarray): Latitude values as a numpy.meshgrid('xy') with the shape (nlat, nlon).

        _extended_summary_
        """
        verbose = [False, True][0]
        if verbose:
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
