import glob
import starepandas
import numpy
import pystare
import pandas
import netCDF4


class Sidecar:
    """Helper class for creating STARE sidecar files."""
    
    def __init__(self, granule_path, out_path=None):
        self.file_path = self.name_from_granule(granule_path, out_path)
        self.create()
        self.zlib = True
        self.shuffle = True
        
    def name_from_granule(self, granule_path, out_path):
        if out_path:
            return out_path + '.'.join(granule_path.split('/')[-1].split('.')[0:-1]) + '_stare.nc'
        else:
            return '.'.join(granule_path.split('.')[0:-1]) + '_stare.nc'
        
    def create(self):
        with netCDF4.Dataset(self.file_path, "w", format="NETCDF4") as rootgrp:
            pass
        
    def write_dimension(self, name, length, group=None):
        with netCDF4.Dataset(self.file_path , 'a', format="NETCDF4") as rootgrp:
            if group:
                grp = rootgrp.createGroup(group)
            else:
                grp = rootgrp
            grp.createDimension(name, length)
        
    def write_dimensions(self, i, j, l, nom_res=None, group=None):
        i_name = 'i'
        j_name = 'j'
        if nom_res:
            i_name += '_{nom_res}'.format(nom_res=nom_res)
            j_name += '_{nom_res}'.format(nom_res=nom_res)
        with netCDF4.Dataset(self.file_path, 'a', format="NETCDF4") as rootgrp:
            if group:
                grp = rootgrp.createGroup(group)
            else:
                grp = rootgrp
            grp.createDimension(i_name, i)
            grp.createDimension(j_name, j)

    def write_lons(self, lons, nom_res=None, group=None, fill_value=None):
        i = lons.shape[0]
        j = lons.shape[1]
        varname = 'Longitude'
        i_name = 'i'
        j_name = 'j'
        if nom_res:
            varname += '_{nom_res}'.format(nom_res=nom_res)
            i_name += '_{nom_res}'.format(nom_res=nom_res)
            j_name += '_{nom_res}'.format(nom_res=nom_res)
        with netCDF4.Dataset(self.file_path, 'a', format="NETCDF4") as rootgrp:
            if group:
                grp = rootgrp.createGroup(group)
            else:
                grp = rootgrp            
            lons_netcdf = grp.createVariable(varname=varname, 
                                             datatype='f4', 
                                             dimensions=(i_name, j_name),
                                             chunksizes=[i, j],
                                             shuffle=self.shuffle,
                                             zlib=self.zlib,
                                             fill_value=fill_value)
            lons_netcdf.long_name = 'Longitude'
            lons_netcdf.units = 'degrees_east'
            lons_netcdf[:, :] = lons
    
    def write_lats(self, lats, nom_res=None, group=None, fill_value=None):
        i = lats.shape[0]
        j = lats.shape[1]
        varname = 'Latitude'
        i_name = 'i'
        j_name = 'j'
        if nom_res:
            varname += '_{nom_res}'.format(nom_res=nom_res)
            i_name += '_{nom_res}'.format(nom_res=nom_res)
            j_name += '_{nom_res}'.format(nom_res=nom_res)
        with netCDF4.Dataset(self.file_path, 'a', format="NETCDF4") as rootgrp:
            if group:
                grp = rootgrp.createGroup(group)
            else:
                grp = rootgrp    
            lats_netcdf = grp.createVariable(varname=varname, 
                                             datatype='f4', 
                                             dimensions=(i_name, j_name),
                                             chunksizes=[i, j],
                                             shuffle=self.shuffle,
                                             zlib=self.zlib,
                                             fill_value=fill_value)
            lats_netcdf.long_name = 'Latitude'
            lats_netcdf.units = 'degrees_north'
            lats_netcdf[:, :] = lats            
        
    def write_sids(self, sids, nom_res=None, group=None, fill_value=0):
        i = sids.shape[0]
        j = sids.shape[1]
        varname = 'STARE_index'.format(nom_res=nom_res)
        i_name = 'i'
        j_name = 'j'
        if nom_res:
            varname += '_{nom_res}'.format(nom_res=nom_res)
            i_name += '_{nom_res}'.format(nom_res=nom_res)
            j_name += '_{nom_res}'.format(nom_res=nom_res)   
        with netCDF4.Dataset(self.file_path, 'a', format="NETCDF4") as rootgrp:
            if group:
                grp = rootgrp.createGroup(group)
            else:
                grp = rootgrp    
            sids_netcdf = grp.createVariable(varname=varname, 
                                             datatype='u8', 
                                             dimensions=(i_name, j_name),
                                             chunksizes=[i, j],
                                             shuffle=self.shuffle,
                                             zlib=self.zlib,
                                             fill_value=fill_value)
            sids_netcdf.long_name = 'SpatioTemporal Adaptive Resolution Encoding (STARE) index'
            sids_netcdf[:, :] = sids

    def write_cover(self, cover, nom_res=None, group=None, fill_value=None):
        l = cover.size
        varname = 'STARE_cover'
        l_name = 'l'

        with netCDF4.Dataset(self.file_path, 'a', format="NETCDF4") as rootgrp:
            if group:
                grp = rootgrp.createGroup(group)
            else:
                grp = rootgrp
            # Only create the 'l' dimension if it does not already exist
            if l_name not in grp.dimensions:
                grp.createDimension(l_name, l)
            cover_netcdf = grp.createVariable(varname=varname, 
                                              datatype='u8', 
                                              dimensions=(l_name),
                                              chunksizes=[l],
                                              shuffle=self.shuffle,
                                              zlib=self.zlib,
                                              fill_value=fill_value)
            cover_netcdf.long_name = 'SpatioTemporal Adaptive Resolution Encoding (STARE) cover'
            cover_netcdf[:] = cover


class Granule:

    def __init__(self, file_path, sidecar_path=None, nom_res=None):
        self.file_path = file_path
        self.sidecar_path = sidecar_path
        self.data = {}
        self.lat = None
        self.lon = None
        self.sids = None
        self.stare_cover = None
        self.ts_start = None
        self.ts_end = None
        if nom_res:
            self.nom_res = nom_res
        else:
            self.nom_res = ''
        self.companion_prefix = None

    def guess_sidecar_path(self):
        name = '.'.join(self.file_path.split('.')[0:-1]) + '_stare.nc'
        if 's3://' == self.file_path[0:5]:
            tokens = starepandas.io.s3.parse_s3_url(name)
            names, _ = starepandas.io.s3.s3_glob(name)
            target = '{prefix}/{resource}'.format(prefix=tokens['prefix'], resource=tokens['resource'])
            if target in names:
                return name
            else:
                raise starepandas.io.granules.SidecarNotFoundError(self.file_path)
        else:
            if glob.glob(name):
                return name
            else:
                raise starepandas.io.granules.SidecarNotFoundError(self.file_path)

    def guess_companion_path(self, prefix=None, folder=None):
        if prefix is None:
            prefix = self.companion_prefix
        return starepandas.io.granules.guess_companion_path(self.file_path, prefix=prefix, folder=folder)

    def add_sids(self, adapt_resolution=True):
        self.sids = pystare.from_latlon_2d(lat=self.lat, lon=self.lon, adapt_level=adapt_resolution)

    def create_sidecar(self, n_workers=1, cover_res=None, out_path=None):
        """Create a STARE sidecar file for this granule.
        
        Parameters
        ----------
        n_workers : int, optional
            Number of workers for parallel processing. Default is 1.
        cover_res : int, optional
            Resolution for the cover. If None, will be automatically determined.
        out_path : str, optional
            Output path for the sidecar file. If None, uses default naming convention.
            
        Returns
        -------
        Sidecar
            The created sidecar object.
        """
        if self.lat is None or self.lon is None:
            raise ValueError("Latitude and longitude data must be loaded before creating sidecar. Call read_latlon() first.")
            
        sidecar = Sidecar(self.file_path, out_path)
        
        # Generate STARE indices
        sids = pystare.from_latlon_2d(lat=self.lat, lon=self.lon, adapt_level=True)
        
        if not cover_res:
            cover_res = 10  # Use a fixed default cover resolution
        # Clamp cover_res to [0, 27]
        cover_res = max(0, min(27, cover_res))
        
        sids_adapted = pystare.spatial_coerce_resolution(sids, cover_res)
        cover_sids = numpy.unique(sids_adapted)
        
        i = self.lat.shape[0]
        j = self.lat.shape[1]
        l = cover_sids.size
        
        # Write dimensions and data
        sidecar.write_dimensions(i, j, l, nom_res=self.nom_res)
        sidecar.write_lons(self.lon, nom_res=self.nom_res)
        sidecar.write_lats(self.lat, nom_res=self.nom_res)
        sidecar.write_sids(sids, nom_res=self.nom_res)
        sidecar.write_cover(cover_sids, nom_res=self.nom_res)
        
        return sidecar

    def read_sidecar_index(self, sidecar_path=None):
        if sidecar_path is not None:
            scp = sidecar_path
        elif self.sidecar_path is not None:
            scp = self.sidecar_path
        else:
            scp = self.guess_sidecar_path()
        ds = starepandas.io.s3.nc4_dataset_wrapper(scp)
        try:
            self.sids = ds['STARE_index_{}'.format(self.nom_res)][:, :].astype(numpy.int64)
        except IndexError:
            # If we don't have a nomres?
            self.sids = ds['STARE_index{}'.format(self.nom_res)][:, :].astype(numpy.int64)

    def read_sidecar_cover(self, sidecar_path=None):
        if sidecar_path:
            scp = sidecar_path
        elif self.sidecar_path:
            scp = self.sidecar_path
        else:
            scp = self.guess_sidecar_path()
        ds = starepandas.io.s3.nc4_dataset_wrapper(scp)
        self.stare_cover = ds['STARE_cover'][:].astype(numpy.int64)

    def read_sidecar_latlon(self, sidecar_path=None):
        if sidecar_path is not None:
            scp = sidecar_path
        elif self.sidecar_path is not None:
            scp = self.sidecar_path
        else:
            scp = self.guess_sidecar_path()
        ds = starepandas.io.s3.nc4_dataset_wrapper(scp)
        try:
            self.lat = ds['Latitude_{}'.format(self.nom_res)][:, :]
            self.lon = ds['Longitude_{}'.format(self.nom_res)][:, :]
        except IndexError:
            self.lat = ds['Latitude{}'.format(self.nom_res)][:, :]
            self.lon = ds['Longitude{}'.format(self.nom_res)][:, :]

    def to_df(self, xy=False):
        """ Converts the granule object to a dataframe

        Parameters
        -----------
        xy: bool
            If true, add columns for the original array coordinates
            For MODIS, x will be in scan direction / across swath / data samples

        Returns
        --------
        df: STAREDataFrame
            dataframe containing lat, lon, xy, and data; one row per observation

        """
        df = {}

        if self.lat is not None:
            df['lat'] = self.lat.flatten()
            df['lon'] = self.lon.flatten()
        if self.sids is not None:
            # Converting to nullable series (capitalized "I" in Int64)
            sids = pandas.Series(pandas.array(self.sids.flatten(), dtype='Int64'))
            df['sids'] = sids
        if self.ts_start is not None and self.ts_end is not None:
            df['ts_start'] = self.ts_start
            df['ts_end'] = self.ts_end
        if xy:
            if self.lat is not None:
                indices = numpy.indices(self.lat.shape, dtype='uint16')
            elif self.sids is not None:
                indices = numpy.indices(self.sids.shape, dtype='uint16')
            else:
                ds = list(self.data.values())[0]
                indices = numpy.indices(ds.shape, dtype='uint16')
            # Careful indices() gives first the row (y), then the column (x) indices
            df['x'] = indices[1].flatten()
            df['y'] = indices[0].flatten()
        for key in self.data.keys():
            dtype = self.data[key].dtype

            # We have to convert to nullable integer types (IntegerArray) to deal with the masks
            # https://pandas.pydata.org/docs/user_guide/integer_na.html
            if dtype.kind == 'i':
                dtype = dtype.name.replace('i', 'I')
            elif dtype.kind == 'u':
                dtype = dtype.name.replace('ui', 'UI')
            series = pandas.Series(self.data[key].flatten(), dtype=dtype)
            df[key] = series
        return starepandas.STAREDataFrame(df)


