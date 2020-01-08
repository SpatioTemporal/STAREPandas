from pyhdf.SD import SD
from starepandas import STAREDataFrame
import geopandas


def read_mod09(file_path, row_min=0, row_max=-1, col_min=0, col_max=-1):
    hdf = SD(file_path)
    lat = hdf.select('Latitude').get()[row_min:row_max, col_min:col_max].flatten()
    lon = hdf.select('Longitude').get()[row_min:row_max, col_min:col_max].flatten()
    modis = {'lat': lat, 'lon': lon}
    for dataset_name in dict(filter(lambda elem: '1km' in elem[0], hdf.datasets().items())).keys():
        modis[dataset_name] = hdf.select(dataset_name).get()[row_min:row_max, col_min:col_max].flatten()
    modis = STAREDataFrame(modis, geometry=geopandas.points_from_xy(modis['lon'], modis['lat']))
    return modis
