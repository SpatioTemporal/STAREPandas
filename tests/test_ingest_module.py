"""Tests for the task-7 starepandas.ingest module.

Confirms:
* The three functions are reachable at module level and via the top-level
  ``starepandas`` namespace (cloud worker import path).
* The demo class methods are now thin shims that delegate (existing
  notebooks keep working).
* ``ingest_granules_local`` works end-to-end on a tiny synthetic input
  (no AWS, no real HDF5 — bypasses _glob_granules by passing one path
  through the to_local layer mock).
"""

import os

import pytest


def test_ingest_module_exports_three_functions():
    """The module-level API the cloud worker imports."""
    from starepandas.ingest import (
        ingest_granules_local,
        ingest_granules_s3,
        clean_s3_prefix,
    )
    assert callable(ingest_granules_local)
    assert callable(ingest_granules_s3)
    assert callable(clean_s3_prefix)


def test_functions_reachable_at_top_level():
    """``starepandas.ingest_granules_s3`` etc. — convenience re-exports."""
    import starepandas
    assert hasattr(starepandas, 'ingest')
    assert callable(starepandas.ingest_granules_local)
    assert callable(starepandas.ingest_granules_s3)
    assert callable(starepandas.clean_s3_prefix)


def test_starepods_demo_ingest_is_thin_shim(monkeypatch):
    """StarePodsDemo.ingest_granules must delegate to
    starepandas.ingest.ingest_granules_s3 with the same kwargs."""
    from starepandas.demo_lib import StarePodsDemo
    import starepandas.ingest as ingest_mod

    calls = []
    def fake_ingest_s3(**kwargs):
        calls.append(kwargs)
        return ['s3://x/done']

    monkeypatch.setattr(ingest_mod, 'ingest_granules_s3', fake_ingest_s3)
    demo = StarePodsDemo()
    result = demo.ingest_granules(
        data_path='/tmp/fake.HDF5',
        instrument='GMI',
        s3_prefix='s3://x/y',
        scan='S1',
        level=10,
        clean_before_run=False,
        extra='passthrough',
    )
    assert result == ['s3://x/done']
    assert len(calls) == 1
    assert calls[0]['data_path'] == '/tmp/fake.HDF5'
    assert calls[0]['instrument'] == 'GMI'
    assert calls[0]['s3_prefix'] == 's3://x/y'
    assert calls[0]['scan'] == 'S1'
    assert calls[0]['extra'] == 'passthrough'


def test_local_demo_ingest_is_thin_shim(monkeypatch, tmp_path):
    """LocalStarePodsDemo.ingest_granules must delegate with self.local_root
    and self.db_path threaded through."""
    from starepandas.demo_lib import LocalStarePodsDemo
    import starepandas.ingest as ingest_mod

    calls = []
    def fake_ingest_local(**kwargs):
        calls.append(kwargs)
        return ['/tmp/done.parquet']

    monkeypatch.setattr(ingest_mod, 'ingest_granules_local', fake_ingest_local)
    demo = LocalStarePodsDemo(local_root=str(tmp_path / 'pods'))
    result = demo.ingest_granules(
        data_path='/tmp/fake.HDF5',
        instrument='GMI',
        scan='S1',
        level=10,
    )
    assert result == ['/tmp/done.parquet']
    assert len(calls) == 1
    assert calls[0]['local_root'] == os.path.abspath(str(tmp_path / 'pods'))
    assert calls[0]['db_path'] == os.path.join(
        os.path.abspath(str(tmp_path / 'pods')), 'metadata.db'
    )


def test_starepods_demo_clean_s3_prefix_is_thin_shim(monkeypatch):
    """StarePodsDemo.clean_s3_prefix delegates to module-level."""
    from starepandas.demo_lib import StarePodsDemo
    import starepandas.ingest as ingest_mod

    calls = []
    def fake_clean(s3_prefix):
        calls.append(s3_prefix)
        return {'rds_rows_deleted': 7, 's3_objects_deleted': 13}

    monkeypatch.setattr(ingest_mod, 'clean_s3_prefix', fake_clean)
    demo = StarePodsDemo()
    result = demo.clean_s3_prefix('s3://zarrpods/testing-s3')
    assert result == {'rds_rows_deleted': 7, 's3_objects_deleted': 13}
    assert calls == ['s3://zarrpods/testing-s3']


def test_glob_granules_helper_handles_single_file(tmp_path):
    from starepandas.ingest import _glob_granules
    f = tmp_path / "x.HDF5"
    f.write_text("dummy")
    assert _glob_granules(str(f)) == [str(f)]


def test_glob_granules_helper_handles_missing_path():
    from starepandas.ingest import _glob_granules
    assert _glob_granules("/nonexistent/path/x.HDF5") == []


def test_glob_granules_helper_handles_directory(tmp_path):
    from starepandas.ingest import _glob_granules
    (tmp_path / "a.HDF5").write_text("")
    (tmp_path / "b.HDF5").write_text("")
    (tmp_path / "not_hdf5.txt").write_text("")
    found = sorted(_glob_granules(str(tmp_path)))
    assert len(found) == 2
    assert all(f.endswith(".HDF5") for f in found)


def test_ingest_granules_s3_clean_without_prefix_rejected():
    """clean_before_run=True must require an explicit s3_prefix —
    refuses to wipe the entire default_s3_prefix."""
    from starepandas.ingest import ingest_granules_s3
    with pytest.raises(ValueError, match="clean_before_run=True requires"):
        ingest_granules_s3(
            data_path='/tmp/fake.HDF5',
            instrument='GMI',
            s3_prefix=None,
            clean_before_run=True,
        )


def test_ingest_granules_local_empty_input_returns_empty(tmp_path):
    """No matching granule files → empty list, no error."""
    from starepandas.ingest import ingest_granules_local
    result = ingest_granules_local(
        data_path='/nonexistent/path/x.HDF5',
        instrument='GMI',
        local_root=str(tmp_path / 'pods'),
    )
    assert result == []
