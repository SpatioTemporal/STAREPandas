"""Shared local write → SQLite round-trip helpers for the temporal tests.

Lifted out of ``test_temporal_query.py`` (flagged in the issue-03 review) so
the issue-04 VCF roll-up tests reuse the same seams instead of re-declaring
them. Plain importable helpers, not pytest fixtures, so callers can
parameterize them (pod location, temporal range, dataset) per test.
"""

import datetime
import os

import numpy as np
import pandas as pd
import pystare

from starepandas import STAREDataFrame
from starepandas.demo_lib import LocalStarePodsDemo


T0 = pd.Timestamp('2020-06-15 12:00:00')

# One fixed point → one pod; timestamps span exactly [T0, T0 + 30 min].
CHUNK_START = T0
CHUNK_END = T0 + pd.Timedelta(minutes=30)


def single_pod_sdf(lat=37.0, lon=-120.0, n=7,
                   t_start=CHUNK_START, t_end=CHUNK_END,
                   with_timestamp=True):
    """STAREDataFrame whose points all land in one pod with a known,
    exact temporal range [t_start, t_end]."""
    lats = np.full(n, lat)
    lons = np.full(n, lon)
    data = {
        'lat': lats,
        'lon': lons,
        'sids': pystare.from_latlon(lats, lons, level=6),
    }
    if with_timestamp:
        data['timestamp'] = list(pd.date_range(t_start, t_end, periods=n))
    sdf = STAREDataFrame(data)
    sdf._sid_column_name = 'sids'
    return sdf


def write_local(tmp_path, sdf, dataset='GMI_S1', granule_name='G1',
                raw_collected_time=datetime.datetime(2020, 6, 15)):
    """Pod ``sdf`` into a local root + SQLite catalog under ``tmp_path``."""
    root = os.path.join(str(tmp_path), 'local_root')
    db_path = os.path.join(str(tmp_path), 'metadata.db')
    sdf.to_local(root, level=6, db_path=db_path, dataset=dataset,
                 granule_name=granule_name, raw_collected_time=raw_collected_time)
    return root, db_path


def two_pod_demo(tmp_path):
    """Two granules in far-apart pods with disjoint time ranges:
    California chunk over [T0, T0+30m], Australia chunk a day later."""
    root = os.path.join(str(tmp_path), 'local_root')
    os.makedirs(root, exist_ok=True)
    db_path = os.path.join(root, 'metadata.db')
    ca = single_pod_sdf(lat=37.0, lon=-120.0)
    au = single_pod_sdf(lat=-25.0, lon=134.0,
                        t_start=T0 + pd.Timedelta(days=1),
                        t_end=T0 + pd.Timedelta(days=1, minutes=30))
    ca.to_local(root, level=6, db_path=db_path, dataset='GMI_S1',
                granule_name='G_CA', raw_collected_time=datetime.datetime(2020, 6, 15))
    au.to_local(root, level=6, db_path=db_path, dataset='GMI_S1',
                granule_name='G_AU',
                raw_collected_time=datetime.datetime(2020, 6, 16))
    demo = LocalStarePodsDemo(local_root=root)
    ca_sids = demo.get_sids_for_bbox(-125, 32, -115, 42)
    return demo, ca_sids
