import time
import starepandas

start = time.time()

for i in range(0, 10):
    fname = '/home/griessbaum/MOD09.A2021001.1855.061.2021012044801.hdf'
    mod05 = starepandas.read_granule(fname, sidecar=False, latlon=True, xy=True)

print(time.time() - start)
