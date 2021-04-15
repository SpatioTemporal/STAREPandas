import unittest 
import starepandas
 
 
class MainTest(unittest.TestCase): 
    
    def test_makecatalog1(self):
        catalog = starepandas.folder2catalog('tests/data/catalog/', granule_extension='hdf')        
        self.assertEqual(15, catalog.size)


if __name__ == '__main__':
    unittest.main()
