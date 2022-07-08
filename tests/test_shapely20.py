import starepandas
import warnings
import shapely

sids = [4611686018427387903, 4611686018427387903]
sdf = starepandas.STAREDataFrame(sids=[sids])
trixels = sdf.make_trixels()


def test_settrixels():
    with warnings.catch_warnings(record=True) as w:
        s = sdf.set_trixels(trixels)
        if len(w) > 0:
            print(w[0])
            raise shapely.errors.ShapelyDeprecationWarning



