STAREDataFrame
====================================
.. currentmodule:: starepandas


Constructor
-----------


.. autosummary::
   :toctree: api/

   STAREDataFrame

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
    STAREDataFrame.to_stare_singleres

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
