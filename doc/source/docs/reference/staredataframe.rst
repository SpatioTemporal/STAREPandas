STAREDataFrame
====================================
.. currentmodule:: starepandas


Constructor
-----------


.. autoclass:: STAREDataFrame


Bootstrapping
---------------

.. autosummary::
    :toctree: api/

    STAREDataFrame.make_stare
    STAREDataFrame.set_stare
    STAREDataFrame.make_trixels
    STAREDataFrame.set_trixels


Geoprocessing
---------------

.. autosummary::
    :toctree: api/

    STAREDataFrame.stare_intersects
    STAREDataFrame.stare_disjoint
    STAREDataFrame.stare_intersection
    STAREDataFrame.stare_dissolve

STARE manipulation
--------------------
.. autosummary::
    :toctree: api/

    STAREDataFrame.to_stare_resolution
    STAREDataFrame.clear_to_resolution
    STAREDataFrame.to_stare_singleres
    STAREDataFrame.hex

IO
------------
.. autosummary::
    :toctree: api/

    STAREDataFrame.write_pods
    STAREDataFrame.to_scidb

Plotting
-----------
.. autosummary::
    :toctree: api/

    STAREDataFrame.plot
