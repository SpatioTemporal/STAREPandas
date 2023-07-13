import geopandas
import pandas
import shapely.wkb
import shapely.geos
#import geoalchemy2
#import sqlalchemy
import psycopg2.extensions
import numpy


def load_geom_text(x):
        """Load from binary encoded as text."""
        return shapely.wkb.loads(str(x), hex=True)

    
def read(table_name, con):
    gdf = pandas.read_sql(f'SELECT * FROM {table_name}', con=con)    
    query = f"SELECT * FROM geometry_columns WHERE f_table_name = '{table_name}'"
    geom_columns = pandas.read_sql(query, con=con)
    
    query = """SELECT column_name 
               FROM information_schema.columns 
               WHERE table_name='cells_roi' 
               AND data_type='ARRAY'"""
    arrays = pandas.read_sql(query, con=con)['column_name'].tolist()

    for array in arrays:
        gdf[array] = gdf[array].apply(numpy.array)
    
    for column in geom_columns.f_geometry_column:                        
        geoms = gdf[column].apply(load_geom_text)
        crs = shapely.geos.lgeos.GEOSGetSRID(geoms[0]._geom)
        gdf[column] = geopandas.GeoSeries(geoms, crs=crs)
        
    return gdf


def get_geom_type(gdf, column):
    geom_types = list(gdf[column].geom_type.unique())
    if len(geom_types) == 1:
        target_geom_type = geom_types[0].upper()
    else:
        target_geom_type = "GEOMETRY"
    return target_geom_type


def addapt_numpy_float64(numpy_float64):
    return psycopg2.extensions.AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return psycopg2.extensions.AsIs(numpy_int64)





def write(gdf, engine, table_name):
    """
    We extend the default geopandas capabilities to allow writing dataframes with multiple geometry columns.
    We identify the datatype of each column. If they are geometry datatypes, we manually do a wkb dump and then write
    the dataframe as a conventional table to postgresql.

    Parameters
    ----------
    gdf
    engine
    table_name

    Returns
    -------

    """
    try:
        import geoalchemy2
        import sqlalchemy
    except ImportError:
        raise ImportError("'to_postgis()' requires geoalchemy2 package.")

    typemap = {'int16': sqlalchemy.types.Integer,
               'int64': sqlalchemy.types.BigInteger,
               'object': sqlalchemy.types.ARRAY(sqlalchemy.types.BigInteger),
               'float64': sqlalchemy.types.Float}

    gdf = gdf.copy() # Probably unnecessary?
    gdf = geopandas.GeoDataFrame(gdf)
    
    g_dtypes = {}
    for column, dtype in gdf.dtypes.items():        
        if dtype == 'geometry':
            dtype = get_geom_type(gdf, column)
            dtype = geoalchemy2.Geometry(dtype)
        elif dtype.name in typemap.keys():
            dtype = typemap[dtype.name]
        g_dtypes[column] = dtype
        
    srid = 4326

    for column, dtype in gdf.dtypes.items():
        if dtype == 'geometry':
            gdf[column] = [shapely.wkb.dumps(geom, srid=srid, hex=True) for geom in gdf[column]]
    
    psycopg2.extensions.register_adapter(numpy.float64, addapt_numpy_float64)
    psycopg2.extensions.register_adapter(numpy.int64, addapt_numpy_int64)
        
    
    gdf.to_sql(name=table_name, con=engine, if_exists='replace', dtype=g_dtypes, index=False)
     
