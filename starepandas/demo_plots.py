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
    widest_events = events[events['instruments'].map(len) == widest]
    meeting = widest_events['time'].min()
    podcode = widest_events.loc[widest_events['time'] == meeting, 'podcode'].iloc[0]
    return podcode, meeting, int(widest)


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
    # Widest swath first, so the sparse ones stay visible on top of it.
    for rank, instrument in enumerate(sorted(passes, key=lambda k: -len(passes[k]))):
        pixels = passes[instrument]
        ax.scatter(pixels['lon'], pixels['lat'],
                   s=1.0 if rank == 0 else 6.0,
                   alpha=0.30 if rank == 0 else 0.85,
                   color=_color(instrument), label=instrument, zorder=2 + rank,
                   transform=ccrs.PlateCarree())
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
