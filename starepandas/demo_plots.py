"""Plot helpers for the STARE-PODS example demos.

The overlap analytics (:mod:`starepandas.overlap`) answer *which* pods hold a
rendezvous and *when*, but a matrix of counts hides the geometry that produces
it. These two figures show it:

* :func:`plot_pod_coverage` — the level-4 pods each instrument's chunks
  occupy, on a world map. Makes the orbital geometry obvious: GMI's 65°
  inclination confines it to a ±67° band, while the sun-synchronous
  instruments run pole to pole, so the two only meet at certain latitudes.
* :func:`plot_rendezvous` — one pod, zoomed: the swaths that meet there
  (space) beside their pass windows (time). A rendezvous needs *both*, and a
  single map cannot show the temporal half.

Everything here takes already-loaded frames — the same
``podcode``/``Dataset``/``t_start``/``t_end`` catalog and the ordinary chunk
frames — so it works identically against the local SQLite/Parquet store and
against S3 + RDS. Each function returns a Matplotlib ``Figure``; callers
decide whether to ``savefig`` (scripts) or let the notebook display it.
"""

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import geopandas
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

import starepandas
from starepandas.overlap import fold_instrument
from starepandas.staredataframe import podcode_to_sid

#: Stable per-instrument colors, so the two figures (and the two demos) agree.
INSTRUMENT_COLORS = {
    'GMI': '#e6194b',
    'SSMIS': '#3cb44b',
    'AMSR2': '#4363d8',
    'ATMS': '#f58231',
}

_DEFAULT_COLOR = '#808080'


def _color(instrument):
    return INSTRUMENT_COLORS.get(instrument, _DEFAULT_COLOR)


def pod_trixels(podcodes):
    """Trixel polygons for pod codes, ready to hand to cartopy.

    Built with ``wrap_lon=False`` and then split at the antimeridian. The
    default ``wrap_lon=True`` normalises every longitude into [-180, 180],
    which silently ruins any trixel straddling the antimeridian: a triangle
    with corners at 178° and -179° becomes a polygon spanning ~357° the wrong
    way round, and fills a band clean across the map at that latitude. Keeping
    longitudes continuous and splitting into a MultiPolygon draws both halves
    where they belong.

    Parameters
    ----------
    podcodes : iterable of str
        Pod codes (e.g. ``"q03200"``).

    Returns
    -------
    geopandas.GeoSeries
        One (multi)polygon per pod code, in lon/lat degrees.
    """
    sids = [podcode_to_sid(podcode) for podcode in podcodes]
    frame = starepandas.STAREDataFrame({'sids': sids})
    frame.set_sids('sids', inplace=True)
    frame.set_trixels(frame.make_trixels(wrap_lon=False), inplace=True)
    split = frame.split_antimeridian()
    return geopandas.GeoSeries(split[split._trixel_column_name])


def _pod_widths(events):
    """Per pod, how many instruments its *widest* rendezvous involves."""
    return events.groupby('podcode')['instruments'].apply(lambda s: s.map(len).max())


def widest_rendezvous(events):
    """The pod holding the widest rendezvous, and the instant it completes.

    Parameters
    ----------
    events : pandas.DataFrame
        Events frame from :func:`starepandas.overlap.rendezvous_events`.

    Returns
    -------
    tuple
        ``(podcode, meeting_time, n_instruments)``. ``meeting_time`` is the
        arrival of the *last* of the co-active passes — the moment the
        rendezvous is complete.

    Raises
    ------
    ValueError
        If ``events`` is empty.
    """
    if events.empty:
        raise ValueError("no rendezvous events to plot")
    widest = events['instruments'].map(len).max()
    return rendezvous_of_size(events, int(widest))


def _pod_footprints(metadata, meetings=None, window=None):
    """Pixels per (pod, instrument), from the catalog's per-chunk ``num_rows``.

    With ``meetings`` (pod → rendezvous instant) and ``window``, each chunk
    counts only for the share of its span lying inside
    ``[meeting − window, meeting + window]``. That is the difference between
    "how much of this pod did the instrument ever see" and "how much did it
    see *at the rendezvous*" — an instrument can own hundreds of pixels in a
    pod and still contribute a 21-pixel sliver to the meeting being drawn.

    Prorating rather than counting an overlapping chunk in full matters
    because ``[t_start, t_end]`` is an *envelope*: a granule that crosses one
    pod twice stores both passes in a single chunk spanning the hour and a
    half between them. Such a chunk is down-weighted, which is right — only
    part of it is ever present at one meeting. Pixels are near-uniform in
    time within a pass, so the estimate is good enough to rank examples by;
    it is never used to decide whether a rendezvous happened.
    """
    frame = metadata[['podcode', 'Dataset', 'num_rows']].copy()
    frame['instrument'] = metadata['Dataset'].map(fold_instrument)

    if meetings is not None and window is not None \
            and {'t_start', 't_end'} <= set(metadata.columns):
        # Drop chunks of pods with no meeting before clipping: a NaT in the
        # clip bound is filled with ``inf``, which turns the comparison into
        # an object-dtype one and raises.
        centre = pd.to_datetime(frame['podcode'].map(meetings), errors='coerce')
        frame = frame[centre.notna()]
        centre = centre[centre.notna()]
        t_start = pd.to_datetime(metadata['t_start'], errors='coerce')[frame.index]
        t_end = pd.to_datetime(metadata['t_end'], errors='coerce')[frame.index]
        inside = (t_end.clip(upper=centre + window)
                  - t_start.clip(lower=centre - window)).dt.total_seconds()
        span = (t_end - t_start).dt.total_seconds()
        # An instantaneous chunk has no span to take a share of: it is either
        # in the window or out of it.
        share = (inside.clip(lower=0) / span).where(span > 0, (inside >= 0) * 1.0)
        frame['num_rows'] = (frame['num_rows'] * share.fillna(0)).round()

    return frame.groupby(['podcode', 'instrument'])['num_rows'].sum()


#: A participant contributing fewer pixels than this only clipped a corner of
#: the pod. True, but illegible: a swath genuinely crossing a level-4 pod
#: (~500 km) leaves thousands of pixels behind, so a few hundred means a
#: sliver. Used to rank examples, never to reject a rendezvous outright.
LEGIBLE_PIXELS = 250


def _exact_width_candidates(events, n_instruments, metadata, min_pixels, window):
    """Pods whose widest rendezvous is exactly ``n``, best example first.

    Returns ``[(podcode, combination, meeting, legible)]`` sorted by how well
    the pod illustrates an ``n``-way — its *least* represented participant's
    pixel count, since a pod is only as legible as its thinnest swath — with
    pod code breaking ties so a re-run tells the same story.
    """
    if events.empty:
        return []
    widths = _pod_widths(events)
    candidates = list(widths.index[widths == n_instruments])
    if not candidates:
        return []

    scoped = events[events['podcode'].isin(candidates)
                    & (events['instruments'].map(len) == n_instruments)].copy()
    scoped['combination'] = scoped['instruments'].map(tuple)
    combos = scoped.groupby('podcode')['combination'].first()

    # Time each pod by the *same* combination it is scored on. A pod can hold
    # several distinct pairs at different hours; timing it by one and judging
    # it by another would centre the picture on a window the swaths never
    # share.
    on_combination = scoped[scoped['combination'] == scoped['podcode'].map(combos)]
    meetings = on_combination.groupby('podcode')['time'].min()

    if metadata is None:
        # Nothing to measure a footprint with: fall back to the busiest pod,
        # and call every candidate legible since the sliver test needs pixels.
        counts = events[events['podcode'].isin(candidates)].groupby('podcode').size()
        rank = {p: int(counts[p]) for p in meetings.index}
        legible = dict.fromkeys(rank, True)
    else:
        footprints = _pod_footprints(metadata, meetings, window)
        rank = {p: min(int(footprints.get((p, instrument), 0))
                       for instrument in combos[p])
                for p in meetings.index}
        legible = {p: pixels >= min_pixels for p, pixels in rank.items()}

    ranked = sorted(rank, key=lambda p: (-rank[p], p))
    return [(p, combos[p], meetings[p], legible[p]) for p in ranked]


def rendezvous_examples(events, n_instruments, count=1, metadata=None,
                        window=None, min_pixels=LEGIBLE_PIXELS):
    """Representative pods whose widest rendezvous is *exactly* ``n``.

    "Exactly" matters: every pod holding a 4-way also holds 2- and 3-way
    combinations, so a pod that merely *contains* an n-way one would
    illustrate a "2-way" with a picture of four instruments. Only pods whose
    maximum is ``n`` are considered.

    With ``count`` above 1 the examples are spread across *different*
    instrument combinations wherever the data allows — three GMI+SSMIS pods
    tell one story three times, whereas GMI+SSMIS, AMSR2+ATMS and ATMS+SSMIS
    show that a rendezvous is not a property of one pair of satellites. Pods
    whose thinnest participant is a sliver (< ``min_pixels``) are held back
    and used only if there are not otherwise enough examples — so a width
    with just two legible-or-not pods still yields two.

    Selection is deterministic (ties broken by pod code) so a re-run tells the
    same story.

    Parameters
    ----------
    events : pandas.DataFrame
        Events frame from :func:`starepandas.overlap.rendezvous_events`.
    n_instruments : int
        Rendezvous width to look for (2 = a pair, 3 = a trio, ...).
    count : int, optional
        How many examples to return. Fewer are returned when the catalog
        holds fewer — no error, so a demo degrades rather than breaks.
    metadata : pandas.DataFrame, optional
        Full metadata rows (needs ``podcode``, ``Dataset``, ``num_rows``).
        Falls back to ranking by event count when omitted.
    window : pandas.Timedelta, optional
        Half-width of the window the caller will draw, i.e. the sweep's Δt.
        Given it (and ``t_start``/``t_end`` on ``metadata``), footprints count
        only the chunks present *at the rendezvous* rather than every chunk
        the instrument ever wrote to that pod — the difference between a real
        swath and a sliver that merely clips the pod on some other orbit.
    min_pixels : int, optional
        Sliver threshold; see :data:`LEGIBLE_PIXELS`.

    Returns
    -------
    list of tuple
        ``[(podcode, meeting_time, n_instruments)]``, best example first.
        ``meeting_time`` is the completion of an ``n``-instrument rendezvous
        in that pod.

    Raises
    ------
    ValueError
        If no pod's widest rendezvous is exactly ``n_instruments``.
    """
    ranked = _exact_width_candidates(events, n_instruments, metadata,
                                     min_pixels, window)
    if not ranked:
        raise ValueError(
            f"no pod has a rendezvous of exactly {n_instruments} instruments"
        )

    # Round-robin over combinations: best pod of the best combination, then
    # the best of the next, and only then a second pod from a combination
    # already shown.
    by_combination = {}
    for podcode, combination, _meeting, legible in ranked:
        if legible:
            by_combination.setdefault(combination, []).append(podcode)

    selected = []
    while len(selected) < count and any(by_combination.values()):
        for pods in by_combination.values():
            if pods:
                selected.append(pods.pop(0))
                if len(selected) == count:
                    break

    # Short on legible pods — top up with the slivers rather than returning
    # fewer examples than the catalog can actually show.
    for podcode, _combination, _meeting, legible in ranked:
        if len(selected) == count:
            break
        if not legible:
            selected.append(podcode)

    meetings = {podcode: meeting for podcode, _combination, meeting, _ in ranked}
    return [(podcode, meetings[podcode], int(n_instruments))
            for podcode in selected]


def rendezvous_of_size(events, n_instruments, metadata=None, window=None):
    """The single best pod whose widest rendezvous is *exactly* ``n``.

    Thin wrapper over :func:`rendezvous_examples`; see it for the selection
    rules.

    Returns
    -------
    tuple
        ``(podcode, meeting_time, n_instruments)``.

    Raises
    ------
    ValueError
        If no pod's widest rendezvous is exactly ``n_instruments``.
    """
    return rendezvous_examples(events, n_instruments, metadata=metadata,
                               window=window)[0]


def plot_pod_coverage(catalog, highlight=(), instruments=None, figsize=(14, 7)):
    """World map, one panel per instrument, of the pods its chunks occupy.

    Parameters
    ----------
    catalog : pandas.DataFrame
        Temporal catalog (``podcode`` + ``Dataset`` columns). Scan groups are
        folded, so ``GMI_S1``/``GMI_S2`` share the ``GMI`` panel.
    highlight : iterable of str, optional
        Pod codes to outline in every panel — typically the pods holding the
        widest rendezvous.
    instruments : list of str, optional
        Panel order. Defaults to every instrument in ``catalog``, sorted.
    figsize : tuple, optional
        Figure size in inches.

    Returns
    -------
    matplotlib.figure.Figure
    """
    folded = catalog.assign(instrument=catalog['Dataset'].map(fold_instrument))
    if instruments is None:
        instruments = sorted(folded['instrument'].unique())

    highlight = list(highlight)
    highlight_geoms = pod_trixels(highlight) if highlight else None

    columns = 2 if len(instruments) > 1 else 1
    rows = -(-len(instruments) // columns)
    fig, axes = plt.subplots(rows, columns, figsize=figsize,
                             subplot_kw={'projection': ccrs.PlateCarree()},
                             squeeze=False)
    for ax, instrument in zip(axes.ravel(), instruments):
        pods = sorted(folded.loc[folded['instrument'] == instrument, 'podcode'].unique())
        ax.add_geometries(pod_trixels(pods), crs=ccrs.PlateCarree(),
                          facecolor=_color(instrument), edgecolor='none', alpha=0.45)
        if highlight_geoms is not None:
            ax.add_geometries(highlight_geoms, crs=ccrs.PlateCarree(),
                              facecolor='none', edgecolor='black', linewidth=1.6)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.4)
        ax.set_global()
        ax.set_title(f"{instrument} — {len(pods)} level-4 pods", fontsize=10)

    for ax in axes.ravel()[len(instruments):]:
        ax.set_visible(False)

    subtitle = ("Pods each instrument's chunks occupy"
                + (f"  (black outline = {', '.join(highlight)})" if highlight else ""))
    fig.suptitle(subtitle)
    fig.tight_layout()
    return fig


def pod_pixels(demo, metadata, podcode, window):
    """Load the pixels of one pod's chunks, grouped by instrument.

    Works for either backend: the pod code prefixes the chunk *filename* in
    both layouts (hierarchical on local disk, flat on S3), so one substring
    test on ``group_path`` selects a pod's chunks either way.

    Parameters
    ----------
    demo : StarePodsDemo or LocalStarePodsDemo
        Used only for its ``download_and_analyze``.
    metadata : pandas.DataFrame
        Metadata rows to select from — pre-filter by ``period`` so this stays
        small.
    podcode : str
        The pod to load.
    window : tuple of pandas.Timestamp
        ``(start, end)``; pixels outside it are dropped. Keeps a *different*
        pass of the same instrument over the same pod from being drawn as if
        it were part of the rendezvous.

    Returns
    -------
    dict
        ``{instrument: DataFrame}`` with ``lat``/``lon``/``timestamp``.
    """
    rows = metadata[metadata['group_path'].str.contains(f"/{podcode}-", regex=False)]
    if rows.empty:
        return {}

    frames = {}
    for dataset, chunk in demo.download_and_analyze(rows).items():
        selected = chunk[chunk['timestamp'].between(*window)]
        if not selected.empty:
            frames.setdefault(fold_instrument(dataset), []).append(
                selected[['lat', 'lon', 'timestamp']]
            )
    return {instrument: pd.concat(parts) for instrument, parts in frames.items()}


def plot_rendezvous(podcode, passes, meeting, dt, figsize=(15, 6.5)):
    """One pod, zoomed: the swaths that meet there (left) and when (right).

    Parameters
    ----------
    podcode : str
        The pod being drawn; its trixel is outlined on the map.
    passes : dict
        ``{instrument: DataFrame}`` from :func:`pod_pixels`.
    meeting : pandas.Timestamp
        When the rendezvous completes (the last arrival).
    dt : pandas.Timedelta
        The coincidence window the sweep used.
    figsize : tuple, optional
        Figure size in inches.

    Returns
    -------
    matplotlib.figure.Figure

    Raises
    ------
    ValueError
        If ``passes`` is empty.
    """
    if not passes:
        raise ValueError(f"no pixels to plot for pod {podcode}")

    minutes = int(dt.total_seconds() // 60)
    fig = plt.figure(figsize=figsize)

    # ---- left: space -------------------------------------------------------
    ax = fig.add_subplot(1, 2, 1, projection=ccrs.PlateCarree())
    # Densest swath first so sparser ones stay visible on top, but keep the
    # markers small and semi-transparent throughout: a 2,500-pixel sliver drawn
    # boldly will otherwise hide the 35,000-pixel swath underneath it.
    by_density = sorted(passes, key=lambda k: -len(passes[k]))
    for rank, instrument in enumerate(by_density):
        pixels = passes[instrument]
        ax.scatter(pixels['lon'], pixels['lat'],
                   s=(1.2, 2.5, 4.0, 5.5)[min(rank, 3)],
                   alpha=(0.45, 0.55, 0.65, 0.75)[min(rank, 3)],
                   color=_color(instrument), label=instrument, zorder=2 + rank,
                   transform=ccrs.PlateCarree())
    # Pods are triangles of wildly varying shape — a polar one spans tens of
    # degrees of longitude but only a few of latitude. Let the panel fill its
    # box rather than preserving 1:1 degrees, or such a pod renders as an
    # unreadable sliver.
    ax.set_aspect('auto')
    ax.add_geometries(pod_trixels([podcode]), crs=ccrs.PlateCarree(),
                      facecolor='none', edgecolor='black', linewidth=2, zorder=10)
    ax.add_feature(cfeature.LAND, facecolor='#f2f2f2')
    ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
    ax.gridlines(draw_labels=True, linewidth=0.3)
    ax.legend(markerscale=4, loc='upper right', fontsize=9)
    ax.set_title(f"Where — pod {podcode} is one level-4 trixel")

    # ---- right: time -------------------------------------------------------
    ax2 = fig.add_subplot(1, 2, 2)
    by_arrival = sorted(passes, key=lambda k: passes[k]['timestamp'].min())
    for row, instrument in enumerate(by_arrival):
        stamps = passes[instrument]['timestamp']
        start, end = mdates.date2num(stamps.min()), mdates.date2num(stamps.max())
        ax2.barh(row, max(end - start, 1e-4), left=start, height=0.45,
                 color=_color(instrument))
        ax2.text(start, row + 0.42,
                 f"{stamps.min().strftime('%H:%M:%S')}–{stamps.max().strftime('%H:%M:%S')}",
                 fontsize=9, va='bottom')
        ax2.text(start, row, f"{instrument} ", ha='right', va='center',
                 fontsize=10, fontweight='bold')
    ax2.axvspan(mdates.date2num(meeting - dt), mdates.date2num(meeting),
                color='0.87', zorder=0, label=f"Δt = {minutes} min before last arrival")
    ax2.axvline(mdates.date2num(meeting), color='black', linestyle='--', linewidth=1.2)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax2.set_xlim(mdates.date2num(meeting - dt * 1.15),
                 mdates.date2num(meeting + dt * 0.35))
    ax2.set_ylim(-0.8, len(passes) - 0.1)
    ax2.set_yticks([])
    ax2.set_xlabel(f"{meeting.date()} UTC")
    ax2.legend(loc='lower right', fontsize=9)
    ax2.set_title(f"When — all {len(passes)} inside one Δt window\n"
                  f"last arrival {meeting.strftime('%H:%M:%S')} (dashed)")

    fig.suptitle(f"A {len(passes)}-way rendezvous in pod {podcode}: "
                 f"same pod, and within Δt of each other", fontsize=13)
    fig.tight_layout()
    return fig
