"""Tests for task-12 S3 layout alignment + default_s3_prefix.

Covers:

* ``STAREDataFrame.to_s3``'s ``granule_name`` validation (early failure
  before any S3 work, so we can test without a live bucket).
* ``_DEFAULT_S3_PREFIX`` loading from ``.config``.
* Path-construction shape — that with ``granule_name`` set the path
  pattern matches the to_local layout (HTM → granule → dataset).
"""

import os

import pytest


# ----- granule_name validation --------------------------------------------


def test_granule_name_with_slash_raises():
    """A granule_name containing '/' would corrupt the HTM path; reject early."""
    from starepandas import STAREDataFrame
    sdf = STAREDataFrame()
    with pytest.raises(ValueError, match="must not contain '/'"):
        sdf.to_s3("s3://x", level=4, granule_name="foo/bar")


def test_granule_name_none_is_accepted():
    """granule_name=None is the backward-compat path. Validation passes,
    even if downstream s3 write would fail for other reasons."""
    from starepandas import STAREDataFrame
    sdf = STAREDataFrame()
    # We expect a downstream error (empty dataframe / no config), NOT the
    # granule_name validation error.
    try:
        sdf.to_s3("s3://x", level=4)  # granule_name omitted → None
    except ValueError as e:
        assert "granule_name" not in str(e), \
            "granule_name=None should not trigger the validation branch"
    except Exception:
        pass  # other errors are fine


# ----- _DEFAULT_S3_PREFIX loading -----------------------------------------


def test_default_s3_prefix_loads_from_config(monkeypatch):
    """When .config contains default_s3_prefix=..., the loader populates
    the module constant."""
    import starepandas.staredataframe as sdf_module

    # Point the loader at a temp config file with the new field.
    tmp = "/tmp/_starepods_task12_test.config"
    with open(tmp, "w") as f:
        f.write("key=AKIA_FAKE\n")
        f.write("secret=fakesecret\n")
        f.write("region_name=us-west-2\n")
        f.write("rds_host=localhost\n")
        f.write("port=5432\n")
        f.write("username=u\n")
        f.write("password=p\n")
        f.write("database=d\n")
        f.write("default_s3_prefix=s3://zarrpods/storage\n")

    monkeypatch.setenv("STAREPANDAS_AWS_CONFIG", tmp)
    # Reset state so the load is observable.
    monkeypatch.setattr(sdf_module, "_DEFAULT_S3_PREFIX", "")
    sdf_module._load_config_from_default_locations()
    assert sdf_module._DEFAULT_S3_PREFIX == "s3://zarrpods/storage"

    os.remove(tmp)


def test_default_s3_prefix_strips_trailing_slash(monkeypatch):
    """Common config typo: trailing slash. Loader should normalise it
    so callers don't end up with double slashes in S3 keys."""
    import starepandas.staredataframe as sdf_module

    tmp = "/tmp/_starepods_task12_test_slash.config"
    with open(tmp, "w") as f:
        f.write("key=AKIA_FAKE\nsecret=s\nregion_name=us-west-2\n")
        f.write("rds_host=h\nport=5432\nusername=u\npassword=p\ndatabase=d\n")
        f.write("default_s3_prefix=s3://zarrpods/storage/\n")

    monkeypatch.setenv("STAREPANDAS_AWS_CONFIG", tmp)
    monkeypatch.setattr(sdf_module, "_DEFAULT_S3_PREFIX", "")
    sdf_module._load_config_from_default_locations()
    assert sdf_module._DEFAULT_S3_PREFIX == "s3://zarrpods/storage"

    os.remove(tmp)


def test_config_without_default_s3_prefix_leaves_constant_empty(monkeypatch):
    """When .config omits default_s3_prefix, the constant stays empty —
    callers that pass s3_path=None will then fail loudly."""
    import starepandas.staredataframe as sdf_module

    tmp = "/tmp/_starepods_task12_test_nopref.config"
    with open(tmp, "w") as f:
        f.write("key=k\nsecret=s\nregion_name=us-west-2\n")
        f.write("rds_host=h\nport=5432\nusername=u\npassword=p\ndatabase=d\n")
        # deliberately no default_s3_prefix line

    monkeypatch.setenv("STAREPANDAS_AWS_CONFIG", tmp)
    monkeypatch.setattr(sdf_module, "_DEFAULT_S3_PREFIX", "")
    sdf_module._load_config_from_default_locations()
    assert sdf_module._DEFAULT_S3_PREFIX == ""

    os.remove(tmp)


# ----- Path-construction shape (via to_local, which uses the same splice) -


def test_to_local_layout_is_podcode_hierarchy_with_granule_name(tmp_path):
    """to_local writes the cumulative pod-code dir tree with a self-describing
    ``<podcode>-<granule>-<dataset>.parquet`` leaf (quaternary layout §2).
    The granule basename may itself contain '-' (the filename grammar recovers
    it as the span between the first and last '-')."""
    import numpy as np
    import pystare
    from starepandas import STAREDataFrame
    from starepandas.staredataframe import (
        parse_chunk_filename, podcode_to_local_dirs,
    )

    # Tiny synthetic STAREDataFrame.
    rng = np.random.default_rng(0)
    lats = rng.uniform(33, 41, 20)
    lons = rng.uniform(-122, -118, 20)
    sids = pystare.from_latlon(lats, lons, level=8)
    sdf = STAREDataFrame({'sids': sids, 'Latitude': lats, 'Longitude': lons})
    sdf.set_sids('sids', inplace=True)

    granule = '1A.GPM.GMI.X.20240115-S000000-E001234.000.V07A'
    root = str(tmp_path)
    sdf.to_local(root, level=6, pixel_width=5, dataset='GMI_S1',
                 granule_name=granule)

    # Walk the tree and assert every parquet leaf matches the expected layout.
    found = []
    for r, _, files in os.walk(root):
        for f in files:
            if f.endswith('.parquet'):
                rel = os.path.relpath(os.path.join(r, f), root)
                found.append(rel)
    assert found, "no parquet files written"

    for rel in found:
        parts = rel.split(os.sep)
        # Every path segment is a pod-code dir; the leaf is the chunk file.
        dir_segs, leaf = parts[:-1], parts[-1]
        assert dir_segs, f"no pod-code dirs before leaf: {rel}"
        assert all(s.startswith('q') for s in dir_segs), \
            f"non-pod-code segment in {rel}: {dir_segs}"

        podcode, g, ds = parse_chunk_filename(leaf)
        # Filename grammar recovers the three components, '-' in granule and all.
        assert g == granule, f"granule mismatch in {rel}: {g}"
        assert ds == 'GMI_S1', f"dataset mismatch in {rel}: {ds}"
        # Leaf dir name == filename pod-code prefix (intentional redundancy).
        assert dir_segs[-1] == podcode, f"leaf dir != pod code in {rel}"
        # Dir chain is the cumulative pod-code chain for this code.
        assert dir_segs == podcode_to_local_dirs(podcode), \
            f"dir chain not cumulative pod-code chain in {rel}: {dir_segs}"
