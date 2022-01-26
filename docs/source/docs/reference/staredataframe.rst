starepandas.STAREDataFrame
====================================
.. currentmodule:: starepandas

A STAREDataFrame object is a pandas.DataFrame that has a special column
with STARE indices and optionally a special column holding the trixel representation.

Constructor
-----------
.. autosummary::
    :toctree: api/

    STAREDataFrame


Bootstrapping
---------------

.. autosummary::
    :toctree: api/

    STAREDataFrame.make_sids
    STAREDataFrame.set_sids
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
    STAREDataFrame.split_antimeridian

IO
------------
.. autosummary::
    :toctree: api/

    STAREDataFrame.write_pods
    STAREDataFrame.to_scidb
    STAREDataFrame.to_array
    STAREDataFrame.to_arrays

Plotting
-----------
.. autosummary::
    :toctree: api/

    STAREDataFrame.plot
