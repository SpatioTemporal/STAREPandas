import starepandas
import geopandas as gpd
from shapely.geometry import Polygon

def test_enclave():
    # Create a sample geometry for South Africa
    data = {
        'name': ['South Africa'],
        'continent': ['Africa'],
        'pop_est': [58000000],
        'geometry': [
            Polygon([(15, -35), (35, -35), (35, -22), (15, -22), (15, -35)])  # Rough outline of South Africa
        ]
    }
    # Create a GeoDataFrame with the sample data
    world = gpd.GeoDataFrame(data, crs="EPSG:4326")
    # Filter for South Africa
    rsa = world[world.name == 'South Africa']
    # Convert to STAREDataFrame
    rsa = starepandas.STAREDataFrame(rsa, add_sids=True, level=5, add_trixels=False)
    # Verify the length
    assert len(rsa.sids.iloc[0]) == 65
