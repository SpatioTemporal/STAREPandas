"""Multi-instrument overlap analytics (design slides 8 & 9).

Answers "which instruments observed the same pod at the same time" over an
already-loaded, period-filtered temporal-catalog frame (the shape returned by
:func:`starepandas.io.granules.load_s3_temporal_catalog` /
:func:`~starepandas.io.granules.load_local_temporal_catalog`). Everything in
this module is a **pure function** — no database or object-store access — so
changing the Δt window re-runs only the sweep on the loaded frame, and the
aggregations / drill-downs only ever touch the events frame the sweep
produced (ADR-0002 Decision 2).

Semantics (ADR-0001): a *pass* is one chunk's temporal range
``[t_start, t_end]``; an *n*-instrument overlap is a **simultaneous
rendezvous** — *n* passes, one per distinct instrument, all mutually within
Δt at once. ADR-0001's pairwise expanded-interval rule reduces (Helly, 1-D)
to ``max(starts) <= min(ends) + Δt``, so :func:`rendezvous_events` is an
event sweep, never a combinatorial subset search (ADR-0002 Decision 1): per
pod, ``+instrument`` at ``t_start`` and ``-instrument`` at ``t_end + Δt``,
sorted once; every instrument set active at a ``+`` event — and every subset
of it — rendezvoused. A "fake triangle" (each pair coincided at different
times, never together) never yields a common active moment and falls out for
free.

Headline cells count **distinct instrument-combinations** per pod (repeats
deduped); rendezvous frequency and timestamps live in the drill-downs.
Scan-group datasets fold to their instrument (:func:`fold_instrument`);
folding is labeling only — the Δt test always runs pass-by-pass.

Pod-set aggregation uses one bitmap per instrument pair over the bounded pod
universe (8 × 4^``MAX_PARTITION_LEVEL`` = 2,048 level-4 pods, so a pod set
is a 2,048-bit integer): matrix cells are popcounts and a region re-scope is
one AND per cell, with no re-sweep. This depends on the pod universe staying
small — if ``MAX_PARTITION_LEVEL`` ever rises past ~8, switch these
plain-int bitmaps to compressed (roaring) bitmaps rather than abandoning the
design (ADR-0002 Decision 2).
"""

import itertools
import re

import numpy as np
import pandas as pd


#: Schema of the canonical rendezvous-events frame — load-bearing: both the
#: populated and the empty result derive from it.
_EVENT_SCHEMA = {'podcode': object, 'time': 'datetime64[ns]',
                 'added': object, 'instruments': object}

#: Columns of the canonical rendezvous-events frame :func:`rendezvous_events`
#: returns (ADR-0002's "events table"). One row per ``+`` sweep event at
#: which >= 2 distinct instruments were simultaneously active in the pod:
#: ``time`` is the rendezvous moment (the arriving pass's ``t_start`` — the
#: latest start among the co-active passes), ``added`` the instrument whose
#: pass arrived, and ``instruments`` the full set then active (sorted tuple).
EVENT_COLUMNS = list(_EVENT_SCHEMA)

_PAIR_DRILLDOWN_SCHEMA = {'podcode': object, 'frequency': 'int64',
                          'times': object}
_POD_DRILLDOWN_SCHEMA = {'instruments': object, 'n_instruments': 'int64',
                         'frequency': 'int64', 'times': object}

# The 'INSTRUMENT_Sn' dataset-name grammar; the reconstitute readers in
# starepandas.io.granules parse the same suffix ("Expected a suffix like
# '_S1'") — keep the two in step.
_SCAN_GROUP_SUFFIX = re.compile(r'_S\d+$')


def _empty_frame(schema):
    return pd.DataFrame({column: pd.Series(dtype=dtype)
                         for column, dtype in schema.items()})


def fold_instrument(dataset):
    """Fold a dataset name to its instrument identity.

    Datasets are one scan group of an instrument's granule, named
    ``<INSTRUMENT>_S<n>`` (``GMI_S1``, ``SSMIS_S3``, …); the overlap
    analytics are per-instrument, so the scan-group suffix is stripped. A
    name without the suffix is already an instrument and passes through.
    Folding affects labels only, never the pass intervals (ADR-0001).
    """
    if not isinstance(dataset, str):
        raise ValueError(f"dataset name must be a string, got {dataset!r}")
    return _SCAN_GROUP_SUFFIX.sub('', dataset)


def _validate_dt(dt):
    if isinstance(dt, (int, float, np.integer, np.floating)):
        raise ValueError(
            f"dt={dt!r} is ambiguous — pass a pandas.Timedelta / "
            f"datetime.timedelta (e.g. pd.Timedelta(minutes=30))")
    dt = pd.Timedelta(dt)
    if pd.isna(dt) or dt < pd.Timedelta(0):
        raise ValueError(f"dt must be a non-negative timedelta, got {dt!r}")
    return dt


def _validate_instrument(name):
    """An instrument argument must be a folded instrument name — a raw
    dataset (scan-group) name would silently match nothing in the events."""
    if not isinstance(name, str) or not name:
        raise ValueError(f"instrument must be a non-empty string, got {name!r}")
    folded = fold_instrument(name)
    if folded != name:
        raise ValueError(
            f"{name!r} is a dataset (scan-group) name — the analytics label "
            f"by instrument; pass {folded!r} instead (see fold_instrument)")
    return name


def _validate_pod_prefix(podcode):
    """A pod-code argument must be grammar-valid and no finer than the
    partition level: chunks are cataloged at pods of at most
    ``MAX_PARTITION_LEVEL``, so a finer code would silently select nothing
    (ADR-0002: presentation rolls up coarser, never finer)."""
    from starepandas.staredataframe import (MAX_PARTITION_LEVEL,
                                            podcode_prefix_length,
                                            podcode_to_sid)

    podcode_to_sid(podcode)                  # grammar check (raises ValueError)
    if len(podcode) > podcode_prefix_length(MAX_PARTITION_LEVEL):
        raise ValueError(
            f"pod code {podcode!r} (level {len(podcode) - 2}) is finer than "
            f"the partition level {MAX_PARTITION_LEVEL} — chunks are "
            f"cataloged at pods of at most that level, so it can never match "
            f"one; pass the covering level-{MAX_PARTITION_LEVEL} (or coarser) "
            f"pod code instead")
    return podcode


def rendezvous_events(catalog, dt, period=None, include_passes=False):
    """
    Sweep a temporal catalog for simultaneous rendezvous — the one kernel
    every headline view and drill-down aggregates (ADR-0002 Decision 2).

    Pure function over an already-loaded frame: re-running with a different
    ``dt`` (or ``period`` sub-window) never touches the catalog.

    Parameters
    ----------
    catalog : pandas.DataFrame
        Temporal-catalog frame with ``podcode``, ``Dataset`` and *parsed*
        (datetime) ``t_start`` / ``t_end`` columns, as returned by
        :func:`~starepandas.io.granules.load_s3_temporal_catalog` /
        :func:`~starepandas.io.granules.load_local_temporal_catalog`.
        tz-aware timestamps are normalized to the catalog's naive-UTC
        convention. Rows with a null temporal range are not passes and
        never enter the sweep; rows with a null ``podcode`` raise (the
        loaders exclude them in SQL — the same contract as
        :func:`~starepandas.io.granules.vcf_rollup`).
    dt : pandas.Timedelta or datetime.timedelta
        The Δt window: passes rendezvous when all mutually within ``dt``
        at once (closed bounds — a gap of exactly ``dt`` still counts).
    period : tuple, optional
        ``(period_start, period_end)`` sub-filter applied client-side to
        the loaded frame. Selects **passes** by closed overlap — the same
        semantics as the loaders' ``period`` — so a rendezvous among
        selected passes can itself fall outside the period when a pass
        merely straddles the boundary. Use the loaders' ``period`` for the
        initial fetch; this parameter narrows *within* it without
        re-querying.
    include_passes : bool, optional
        Also emit a ``passes`` column: per event, the tuple of ``catalog``
        index labels of every pass active at that moment (the rendezvous'
        participants). Requires a unique catalog index (the labels must
        identify passes). Costs a Python replay of the sweep — leave off
        for headline-view workloads.

    Returns
    -------
    pandas.DataFrame
        :data:`EVENT_COLUMNS` (plus ``passes`` when requested), sorted by
        ``(podcode, time)``. Every distinct instrument combination that
        rendezvoused in a pod appears as (a subset of) some event's
        ``instruments`` there.
    """
    from starepandas.io.granules import _period_mask, _require_podcodes

    dt = _validate_dt(dt)
    missing = [c for c in ('podcode', 'Dataset', 't_start', 't_end')
               if c not in catalog.columns]
    if missing:
        raise ValueError(f"catalog is missing column(s) {missing} — expected "
                         f"a temporal-catalog frame")
    for col in ('t_start', 't_end'):
        if not pd.api.types.is_datetime64_any_dtype(catalog[col]):
            raise ValueError(
                f"catalog[{col!r}] is not datetime — load it via the "
                f"temporal-catalog loaders or parse it first")
    if include_passes and not catalog.index.is_unique:
        raise ValueError(
            "include_passes labels participants by catalog index, but the "
            "index has duplicate labels — reset or de-duplicate it first")

    df = catalog.loc[:, ['podcode', 'Dataset', 't_start', 't_end']]
    for col in ('t_start', 't_end'):
        if isinstance(df[col].dtype, pd.DatetimeTZDtype):
            # Catalog timestamps are naive UTC (same as _validate_period).
            df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None)
    _require_podcodes(df)

    # Null-range chunks (either end missing) are not passes.
    df = df[df['t_start'].notna() & df['t_end'].notna()]
    if (df['t_end'] < df['t_start']).any():
        raise ValueError("catalog contains passes with t_end < t_start")
    if period is not None:
        df = df[_period_mask(df, period)]

    columns = EVENT_COLUMNS + (['passes'] if include_passes else [])
    if df.empty:
        schema = dict(_EVENT_SCHEMA, **({'passes': object}
                                        if include_passes else {}))
        return _empty_frame(schema)

    inst_codes, inst_names = pd.factorize(df['Dataset'].map(fold_instrument),
                                          sort=True)
    n_instruments = len(inst_names)
    if n_instruments > 62:
        raise NotImplementedError(
            f"{n_instruments} distinct instruments exceed the int64 "
            f"instrument-mask width")
    pod_codes, pod_names = pd.factorize(df['podcode'], sort=True)

    # The sweep: +pass at t_start, -pass at t_end + dt, sorted per pod with
    # '+' before '-' at ties (closed intervals: touching expanded ranges
    # still rendezvous).
    n = len(df)
    starts = df['t_start'].to_numpy(dtype='datetime64[ns]').view('int64')
    ends = (df['t_end'] + dt).to_numpy(dtype='datetime64[ns]').view('int64')
    times = np.concatenate([starts, ends])
    kind = np.concatenate([np.zeros(n, np.int8), np.ones(n, np.int8)])
    pods2 = np.concatenate([pod_codes, pod_codes])
    inst2 = np.concatenate([inst_codes, inst_codes])
    order = np.lexsort((kind, times, pods2))
    pods_sorted = pods2[order]
    kind_sorted = kind[order]

    # Per-(pod, instrument) active-pass counts at every event: a cumulative
    # sum of ±1 restarted at each pod (events are pod-contiguous after the
    # sort).
    deltas = np.zeros((2 * n, n_instruments), np.int32)
    deltas[np.arange(2 * n), inst2[order]] = np.where(kind_sorted == 0, 1, -1)
    counts = (pd.DataFrame(deltas).groupby(pods_sorted, sort=False)
              .cumsum().to_numpy())

    active = counts > 0
    is_event = (kind_sorted == 0) & (active.sum(axis=1) >= 2)
    masks = active[is_event] @ (np.int64(1) << np.arange(n_instruments))
    combo_of_mask = {
        m: tuple(inst_names[k] for k in range(n_instruments) if m >> k & 1)
        for m in np.unique(masks)}

    events = pd.DataFrame({
        'podcode': np.asarray(pod_names)[pods_sorted[is_event]],
        'time': pd.DatetimeIndex(times[order][is_event].view('datetime64[ns]')),
        'added': np.asarray(inst_names)[inst2[order][is_event]],
        'instruments': [combo_of_mask[m] for m in masks],
    })
    if include_passes:
        events['passes'] = _replay_passes(df, order, pods_sorted, kind_sorted,
                                          is_event, n)
    return events[columns]


def _replay_passes(df, order, pods_sorted, kind_sorted, is_event, n):
    """Per qualifying event, the catalog index labels of the active passes."""
    labels = df.index.to_numpy()
    pass2 = np.concatenate([np.arange(n), np.arange(n)])[order]
    active, passes, previous_pod = set(), [], None
    for i in range(2 * n):
        if pods_sorted[i] != previous_pod:
            previous_pod, active = pods_sorted[i], set()
        if kind_sorted[i] == 0:
            active.add(pass2[i])
            if is_event[i]:
                passes.append(tuple(labels[sorted(active)]))
        else:
            active.discard(pass2[i])
    return passes


# ----- aggregations over the events frame -------------------------------------


def _require_events(events):
    missing = [c for c in EVENT_COLUMNS if c not in events.columns]
    if missing:
        raise ValueError(f"events frame is missing column(s) {missing} — "
                         f"pass the frame rendezvous_events() returned")


def _subset_combos(instruments, containing=None):
    """Every >=2-instrument subset of a recorded active set (each one
    rendezvoused — Helly); optionally only those containing one instrument."""
    for size in range(2, len(instruments) + 1):
        for combo in itertools.combinations(instruments, size):
            if containing is None or containing in combo:
                yield combo


def _distinct_pod_combos(events):
    """The deduped headline unit both slide views count: distinct
    (pod, instrument-combination) incidences, however often each recurred."""
    distinct = events[['podcode', 'instruments']].drop_duplicates()
    return {(pod, combo)
            for pod, mask in distinct.itertuples(index=False)
            for combo in _subset_combos(mask)}


def _cover_bitmap(pods, pod_bit):
    """A region cover as a pod bitmap. Each entry is a pod code (validated —
    grammar plus not finer than the partition level); a coarser code covers
    its whole subtree, since a pod code prefixes its descendants'. Covers
    select cataloged pods by code: a chunk cataloged at a pod *coarser* than
    a cover code is not selected by it."""
    cover = 0
    for prefix in pods:
        _validate_pod_prefix(prefix)
        for pod, bit in pod_bit.items():
            if pod.startswith(prefix):
                cover |= bit
    return cover


def overlap_matrix(events, pods=None, instruments=None):
    """
    The slide-8 instrument×instrument matrix: cell (A, B) is the number of
    pods in which instruments A and B rendezvous.

    Pure aggregation over the events frame — per-pair pod bitmaps make each
    cell a popcount, and a region re-scope (``pods``) is one AND per cell:
    no re-sweep, no catalog access (ADR-0002 Decision 2).

    Parameters
    ----------
    events : pandas.DataFrame
        The frame :func:`rendezvous_events` returned.
    pods : iterable of str, optional
        Region re-scope: count only pods covered by these pod codes (a
        coarser code covers its subtree; a code finer than the partition
        level raises). Codes outside the loaded frame simply cover nothing.
    instruments : iterable of str, optional
        The matrix universe (row/column labels), as folded instrument
        names. Defaults to the instruments observed in ``events``; pass
        the full instrument list for a fixed presentation shape.

    Returns
    -------
    pandas.DataFrame
        Square, symmetric, zero-diagonal integer frame indexed by
        instrument.
    """
    _require_events(events)
    if instruments is None:
        universe = sorted(set().union(*events['instruments']))
    else:
        universe = sorted({_validate_instrument(name) for name in instruments})

    pod_bit = {pod: 1 << i
               for i, pod in enumerate(sorted(set(events['podcode'])))}
    bitmaps = {}
    for pod, combo in _distinct_pod_combos(events):
        if len(combo) == 2:
            bitmaps[combo] = bitmaps.get(combo, 0) | pod_bit[pod]
    if pods is None:
        cover = (1 << len(pod_bit)) - 1
    else:
        cover = _cover_bitmap(pods, pod_bit)

    matrix = pd.DataFrame(0, index=universe, columns=universe, dtype='int64')
    for a, b in itertools.combinations(universe, 2):
        n_pods = (bitmaps.get((a, b), 0) & cover).bit_count()
        matrix.loc[a, b] = matrix.loc[b, a] = n_pods
    return matrix


def overlap_pod_table(events):
    """
    The slide-9 pod×(number-of-instruments) table: cell (pod, n) is the
    number of distinct n-instrument combinations that rendezvous in that
    pod (repeats deduped — frequency lives in :func:`pod_drilldown`).

    A genuine n-way rendezvous contributes all its sub-combinations (one
    A+B+C rendezvous adds its three pairs to n=2 and the triple to n=3); a
    fake triangle never recorded a 3-instrument active set, so it counts
    only at n=2 (ADR-0001).

    Parameters
    ----------
    events : pandas.DataFrame
        The frame :func:`rendezvous_events` returned.

    Returns
    -------
    pandas.DataFrame
        Indexed by ``podcode`` (pods with at least one rendezvous), one
        integer column per combination size from 2 up to the largest
        observed (no rows, and just the ``2`` column, when there are no
        events).
    """
    _require_events(events)
    combos = _distinct_pod_combos(events)
    if not combos:
        return (pd.DataFrame({2: pd.Series(dtype='int64')})
                .rename_axis('podcode'))
    counted = pd.DataFrame(
        [(pod, len(combo)) for pod, combo in combos],
        columns=['podcode', 'n_instruments'])
    table = (counted.value_counts()
             .unstack('n_instruments', fill_value=0)
             .sort_index())
    table.columns = table.columns.astype(int)
    return table.reindex(columns=range(2, table.columns.max() + 1),
                         fill_value=0)


def pair_drilldown(events, instrument_a, instrument_b):
    """
    Slide-8 drill-down: for one instrument pair, the pods containing their
    overlaps plus the per-pod rendezvous frequency and times.

    Frequency counts distinct rendezvous of the pair — the sweep events at
    which one of the two arrived while the other was active (a pair
    co-active only inside a larger rendezvous counts there too). Pure
    filter over the events frame — no catalog access.

    Parameters
    ----------
    events : pandas.DataFrame
        The frame :func:`rendezvous_events` returned.
    instrument_a, instrument_b : str
        The instrument pair, in either order, as folded instrument names
        (``'GMI'``, not ``'GMI_S1'`` — raw dataset names raise).

    Returns
    -------
    pandas.DataFrame
        Columns ``podcode``, ``frequency``, ``times`` (the rendezvous
        moments, ascending), one row per pod, sorted by ``podcode``.
    """
    _require_events(events)
    pair = tuple(sorted((_validate_instrument(instrument_a),
                         _validate_instrument(instrument_b))))
    if pair[0] == pair[1]:
        raise ValueError(f"a pair needs two distinct instruments, got "
                         f"{instrument_a!r} twice")
    if events.empty:
        return _empty_frame(_PAIR_DRILLDOWN_SCHEMA)

    both_active = events['instruments'].map(
        lambda mask: pair[0] in mask and pair[1] in mask)
    hits = events[both_active & events['added'].isin(pair)]
    if hits.empty:
        return _empty_frame(_PAIR_DRILLDOWN_SCHEMA)
    return (hits.groupby('podcode', sort=True)['time']
            .agg(frequency='size', times=list)
            .reset_index())


def pod_drilldown(events, podcode):
    """
    Slide-9 drill-down: for one pod, the instrument combinations that
    rendezvous there plus each combination's frequency and times.

    Frequency counts distinct rendezvous of the combination — the sweep
    events at which one of its instruments arrived while the rest were
    active. A coarser pod code rolls up its whole subtree (the same cover
    semantics as :func:`overlap_matrix`'s ``pods``). Pure filter over the
    events frame — no catalog access.

    Parameters
    ----------
    events : pandas.DataFrame
        The frame :func:`rendezvous_events` returned.
    podcode : str
        The pod to inspect — or a coarser pod code, which aggregates every
        loaded pod beneath it. Validated (grammar plus not finer than the
        partition level); a pod without events yields an empty frame.

    Returns
    -------
    pandas.DataFrame
        Columns ``instruments`` (sorted tuple), ``n_instruments``,
        ``frequency``, ``times`` (the rendezvous moments, ascending), one
        row per combination, sorted by size then name.
    """
    _require_events(events)
    _validate_pod_prefix(podcode)

    times_of = {}
    here = events[events['podcode'].str.startswith(podcode)]
    for row in here.itertuples(index=False):
        for combo in _subset_combos(row.instruments, containing=row.added):
            times_of.setdefault(combo, []).append(row.time)
    if not times_of:
        return _empty_frame(_POD_DRILLDOWN_SCHEMA)
    return pd.DataFrame(
        [(combo, len(combo), len(times), sorted(times))
         for combo, times in sorted(times_of.items(),
                                    key=lambda kv: (len(kv[0]), kv[0]))],
        columns=list(_POD_DRILLDOWN_SCHEMA))
