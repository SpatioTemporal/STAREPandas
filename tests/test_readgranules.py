import unittest
import starepandas
import os


class MainTest(unittest.TestCase):
    
    def test_read_cldmsk_viirsl2(self):
        granule = starepandas.io.granules.CLDMSKL2VIIRS('tests/data/granules/CLDMSK_L2_VIIRS_SNPP.A2020219.0742.001.2020219190616.nc')
        granule.read_timestamps()
        with self.subTest():
            self.assertEqual(granule.ts_start, '2020-08-06T07:42:00.000Z')
            self.assertEqual(granule.ts_end, '2020-08-06T07:48:00.000Z')
        granule.read_data()
        granule.read_latlon()
        #granule.to_df()

    def test_read_vnp02dnb(self):
        granule = starepandas.io.granules.VNP02DNB('tests/data/granules/VNP02DNB.A2020219.0742.001.2020219125654.nc')
        granule.read_timestamps()
        with self.subTest():
            self.assertEqual(granule.ts_start, '2020-08-06T07:42:00.000Z')
            self.assertEqual(granule.ts_end, '2020-08-06T07:48:00.000Z')
        granule.read_data()
        companion = granule.guess_companion_path(prefix='VNP03DNB')
        with self.subTest():
            self.assertEqual(companion, 'tests/data/granules/VNP03DNB.A2020219.0742.001.2020219124651.nc')
        
    def test_read_vnp03dnb(self):
        granule = starepandas.io.granules.VNP03DNB('tests/data/granules/VNP03DNB.A2020219.0742.001.2020219124651.nc')
        granule.guess_sidecar_path()
        with self.subTest():
            self.assertEqual(granule.sidecar_path, 'tests/data/granules/VNP03DNB.A2020219.0742.001.2020219124651_stare.nc')
        granule.read_timestamps()
        with self.subTest():
            self.assertEqual(granule.ts_start, '2020-08-06T07:42:00.000Z')
            self.assertEqual(granule.ts_end, '2020-08-06T07:48:00.000Z')
        granule.read_data()
        granule.read_latlon()
        granule.read_sidecar_cover()
        granule.read_sidecar_index()
        df = granule.to_df()
        with self.subTest():
            self.assertEqual(78809088, df.size)
        
    def test_read_mod05(self):
        granule = starepandas.io.granules.Mod05('tests/data/granules/MOD05_L2.A2019336.0000.061.2019336211522.hdf')
        granule.read_timestamps()
        with self.subTest():
            self.assertEqual(str(granule.ts_start), '2019-12-02 00:00:00')
            self.assertEqual(str(granule.ts_end), '2019-12-02 00:05:00')        
        granule.read_data()
        granule.read_latlon()        
        granule.guess_sidecar_path()
        with self.subTest():
            self.assertEqual(granule.sidecar_path, 'tests/data/granules/MOD05_L2.A2019336.0000.061.2019336211522_stare.nc')
        granule.read_sidecar_cover()
        granule.read_sidecar_index()
        df = granule.to_df()
        with self.subTest():
            self.assertEqual(986580, df.size)

    def test_read_granules(self):
        starepandas.read_granule('tests/data/granules/MOD05_L2.A2019336.0000.061.2019336211522.hdf')
        starepandas.read_granule('tests/data/granules/VNP02DNB.A2020219.0742.001.2020219125654.nc')

    def test_sidecar_not_found(self):
        with self.assertRaises(Exception) as context:
            fname = '../tests/data/granules/MYD05_L2.A2020060.1635.061.2020061153519.hdf'
            starepandas.read_granule(fname, latlon=False, sidecar=True)
            self.assertTrue('Could not find sidecar' in context.exception)

    def test_bootstrap(self):
        fname = 'tests/data/granules/MYD05_L2.A2020060.1635.061.2020061153519.hdf'
        modis = starepandas.read_granule(fname, add_stare=True, adapt_resolution=True, track_first=False)
        #trixels = modis.make_trixels()
        #modis.set_trixels(trixels, inplace=True)


def test_find_sidecar():
    fpath = starepandas.datasets.get_path('MOD05_L2.A2019336.0000.061.2019336211522.hdf')
    mod05 = starepandas.io.granules.Mod05(fpath)
    sidecar_path = mod05.guess_sidecar_path()
    assert os.path.exists(sidecar_path)

