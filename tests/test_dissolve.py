import geopandas as gpd
import starepandas
import numpy
from shapely.geometry import Polygon

# Create sample geometries and data
data = {
    'continent': ['Europe', 'Europe', 'North America', 'North America', 'Asia', 'Asia'],
    'name': ['Country1', 'Country2', 'Country3', 'Country4', 'Country5', 'Country6'],
    'pop_est': [100000, 200000, 300000, 400000, 500000, 600000],
    'geometry': [
        Polygon([(-10, 40), (-5, 40), (-5, 50), (-10, 50), (-10, 40)]),  # Europe
        Polygon([(0, 40), (5, 40), (5, 50), (0, 50), (0, 40)]),          # Europe
        Polygon([(-120, 30), (-110, 30), (-110, 40), (-120, 40), (-120, 30)]),  # North America
        Polygon([(-100, 30), (-90, 30), (-90, 40), (-100, 40), (-100, 30)]),    # North America
        Polygon([(100, 10), (110, 10), (110, 20), (100, 20), (100, 10)]),       # Asia
        Polygon([(110, 10), (120, 10), (120, 20), (110, 20), (110, 10)]),       # Asia
    ]
}

# Create a GeoDataFrame with the sample data
world = gpd.GeoDataFrame(data, crs="EPSG:4326")

# Filter to focus on Europe and North America
west = world[world['continent'].isin(['Europe', 'North America'])]

# Convert to STAREDataFrame
west = starepandas.STAREDataFrame(west, add_sids=True, level=4, add_trixels=False)

# Expected sids
europe_sids = numpy.array([4262657047306174468, 4269412446747230211, 4278419646001971204,
                           4280671445815656452, 4285175045443026948, 4289678645070397444,
                           4294182244697767940, 4300937644138823684, 4318952042648305668,
                           4548635623644200964])

def test_dissolve():
    dissolved = west.stare_dissolve(by='continent', aggfunc='sum')
    assert len(dissolved) == 2
    assert numpy.array_equal(dissolved.sids['Europe'], europe_sids)

