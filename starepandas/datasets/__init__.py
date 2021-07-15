import os

__all__ = ["available", "get_path"]

_module_path = os.path.dirname(__file__)
_available_dir = [p for p in next(os.walk(_module_path))[-1] if not p.startswith("__")]
available = _available_dir


def get_path(dataset):
    """
    Get the path to the data file.

    Parameters
    ----------
    dataset : str
        The name of the dataset. See ``geopandas.datasets.available`` for
        all options.

    Examples
    --------
    >>> starepandas.datasets.get_path("MOD05_L2.A2019336.0000.061.2019336211522.hdf")  # doctest: +SKIP
    """

    if dataset in _available_dir:
        return os.path.abspath(os.path.join(_module_path, dataset))    
    else:
        msg = "The dataset '{data}' is not available. ".format(data=dataset)
        msg += "Available datasets are {}".format(", ".join(available))
        raise ValueError(msg)
 
