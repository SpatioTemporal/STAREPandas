STAREDataFrame
====================================
.. currentmodule:: starepandas


Constructor
-----------
.. autosummary::
    :toctree: api/

    STAREDataFrame.__init__


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

Trixel Functions
--------------------
.. autosummary::
    :toctree: api/

    STAREDataFrame.trixel_vertices
    STAREDataFrame.trixel_centers
    STAREDataFrame.trixel_centers_ecef
    STAREDataFrame.trixel_centerpoints
    STAREDataFrame.trixel_corners
    STAREDataFrame.trixel_corners_ecef
    STAREDataFrame.trixel_grings
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
