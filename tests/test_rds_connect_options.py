"""2026-09-07: every catalog connection carries TCP keepalives, a connect
timeout and a statement timeout (Q1-2025 bulk-run follow-up #2).

psycopg2's defaults are none of those, which is how six of eight workers
hung on dead sockets through the 2026-09-05 RDS crash. ``psycopg2.connect``
is stubbed — no database is touched.
"""

from unittest.mock import MagicMock

import pytest

import starepandas.staredataframe as sdf


def test_defaults_are_keepalives_plus_timeouts():
    kw = sdf._rds_connect_options({})
    assert kw == {
        'keepalives': 1,
        'keepalives_idle': 30,
        'keepalives_interval': 10,
        'keepalives_count': 3,
        'connect_timeout': 15,
        'options': '-c statement_timeout=1800000',
    }


def test_rds_block_overrides_by_key_and_can_disable_statement_timeout():
    kw = sdf._rds_connect_options({
        'host': 'h', 'keepalives_idle': '60', 'connect_timeout': 5,
        'statement_timeout_ms': 0,
    })
    assert kw['keepalives_idle'] == 60 and kw['connect_timeout'] == 5
    assert kw['keepalives'] == 1
    assert 'options' not in kw
    kw = sdf._rds_connect_options({'statement_timeout_ms': 600000})
    assert kw['options'] == '-c statement_timeout=600000'


def test_ensure_rds_db_and_table_passes_options_to_both_connects(monkeypatch):
    psycopg2 = pytest.importorskip('psycopg2')
    calls = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return MagicMock()

    monkeypatch.setattr(psycopg2, 'connect', fake_connect)
    monkeypatch.setattr(sdf, '_AWS_RDS_OPTIONS', {
        'host': 'db.example', 'port': '5432', 'username': 'u',
        'password': 'p', 'database': 'postgres', 'keepalives_idle': 45,
    })

    conn = sdf._ensure_rds_db_and_table('StarePodsMetadata')
    assert conn is calls[-1] or conn is not None
    assert [c['dbname'] for c in calls] == ['postgres', 'StarePodsMetadata']
    for c in calls:
        assert (c['host'], c['port'], c['user'], c['password']) == ('db.example', 5432, 'u', 'p')
        assert c['keepalives'] == 1 and c['keepalives_idle'] == 45
        assert c['keepalives_interval'] == 10 and c['keepalives_count'] == 3
        assert c['connect_timeout'] == 15
        assert c['options'] == '-c statement_timeout=1800000'
