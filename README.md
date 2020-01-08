# STAREPandas
A high-level STARE interface

![Example 1](examples/intersection.png)

## Introduction
STAREpandas adds STARE support to pandas dataframes.
STAREPandas provides high-level functions for users to explore and interact with STARE.
STARE dataframes represent geometries as sets of STARE
triangles or ”trixels” (analogous to GeoPandas geo-
dataframes which represent geometries as WKT.) In
STARE dataframes, points are represented as STARE trixels at the HTM tree’s leaf level.
Polygons are represented as sets of STARE trixels that cover the polygon. The
trixels are stored as integerized STARE index values.


## Installation

    mkvirtualenv --python=/usr/bin/python3 $PROJECT_ENV

    pip3 install -r requirements.txt

    git clone https://github.com/NiklasPhabian/starepandas $STAREPandas
    pip3 install -e $STAREPandas
    
    
## Usage

### Loading Spatial regions

    import starepandas
    countries = starepandas.read file(’regions.gpkg’)
    countries = countries.add stare(level=12)
    countries.head()
    
### Loading MOD09 Data

    file path = ’MOD09.A2019317.0815.006.2019319020759.hdf’
    modis = starepandas.read mod09(file path)
    modis = modis.add stare()
    modis = modis.set index(’stare’)
    modis.trixels().plot()
    
    
### Calculating NDVI, spatial join and mean NDVI

    modis[’ndvi’] = (modis.b2−modis.b1)/(modis.b2+modis.b1)
    modis = starepandas.stare join(modis, regions)
    modis grouped = modis.groupby(’country’).mean()
    modis grouped.head()



