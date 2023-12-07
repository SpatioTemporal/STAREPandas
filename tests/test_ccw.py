import starepandas
import shapely


def test_antimeridan_crossing():
    # Polygon crossing antimeridian in the pacific
    # This polygon IS CCW, however shapely will tell us it is not CCW
    lons = [-100, 160, 165, -160, 170, -100]
    lats = [25, 20, -25, -20, 0, 25]
    polygon = shapely.geometry.Polygon(zip(lons, lats))
    ring = polygon.exterior
    assert ring.is_ccw == False
    assert starepandas.spatial_conversions.ring_is_ccw(ring) == True


def test_northpole():
    # Polygon around the northpole
    # This polygon IS CCW, however shapely will tell us it is not CCW
    lons = [0, 90, 180, -90]
    lats = [50, 50, 50, 50]
    polygon = shapely.geometry.Polygon(zip(lons, lats))
    ring = polygon.exterior
    assert ring.is_ccw == False
    assert starepandas.spatial_conversions.ring_is_ccw(ring) == True


def test_southpole():
    # Polygon around the northpole
    # This polygon IS not CCW, and shapely will tell us it is not CCW
    lons = [0, 90, 180, -90]
    lats = [-50, -50, -50, -50]
    polygon = shapely.geometry.Polygon(zip(lons, lats))
    ring = polygon.exterior
    assert ring.is_ccw == False
    assert starepandas.spatial_conversions.ring_is_ccw(ring) == False
