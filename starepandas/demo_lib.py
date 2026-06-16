#!/usr/bin/env python3
"""
STARE-PODS Demonstration API

High-level API for demonstrating STARE-PODS workflow:
1. Ingest granules into Parquet partitions stored in S3
2. Find intersecting data across different instruments using STARE SIDs
3. Download and analyze only intersecting partitions
4. Compare and visualize data from multiple instruments
"""

import os
import starepandas
import pystare
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StarePodsDemo:
    """
    High-level STARE-PODS demonstration API.
    
    Provides simple interface for:
    - Ingesting granules into S3 Parquet partitions
    - Finding intersecting data using STARE spatial indexing
    - Downloading and analyzing intersecting partitions
    - Comparing multiple instruments at same location/time
    """
    
    def __init__(self, aws_config_path: Optional[str] = None):
        """
        Initialize STARE-PODS demonstration.
        
        Parameters
        ----------
        aws_config_path : str, optional
            Path to AWS configuration file
        """
        if aws_config_path:
            import os
            os.environ['STAREPANDAS_AWS_CONFIG'] = aws_config_path
        
        # Load AWS configuration
        starepandas.staredataframe._load_config_from_default_locations()
        
    def clean_s3_prefix(self, s3_prefix: str) -> Dict[str, int]:
        """Thin shim — delegates to :func:`starepandas.ingest.clean_s3_prefix`.

        See that function for full docs. Preserved here so existing
        notebooks calling ``StarePodsDemo().clean_s3_prefix(...)`` keep
        working post-task-7 extraction.
        """
        from starepandas.ingest import clean_s3_prefix as _clean
        return _clean(s3_prefix)

    def ingest_granules(self, data_path: str, instrument: str,
                     s3_prefix: Optional[str] = None,
                     scan: Optional[str] = None, level: int = 10,
                     clean_before_run: bool = False, **kwargs) -> List[str]:
        """Thin shim — delegates to :func:`starepandas.ingest.ingest_granules_s3`.

        Preserved so existing notebooks calling
        ``StarePodsDemo().ingest_granules(...)`` keep working unchanged
        after the task-7 extraction.
        """
        from starepandas.ingest import ingest_granules_s3
        return ingest_granules_s3(
            data_path=data_path,
            instrument=instrument,
            s3_prefix=s3_prefix,
            scan=scan,
            level=level,
            clean_before_run=clean_before_run,
            **kwargs,
        )
    
    def find_intersecting_data(self, location_sids: List[int], instruments: List[str],
                           time_range: Optional[Tuple[str, str]] = None,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           **kwargs) -> pd.DataFrame:
        """
        Find intersecting data across multiple instruments using STARE SIDs.
        
        Parameters
        ----------
        location_sids : List[int]
            STARE SIDs for location of interest
        instruments : List[str]
            List of instruments to search (GMI, AMSR2, SSMIS, ATMS)
        time_range : Tuple[str, str], optional
            Time range filter (start_date, end_date) in 'YYYY-MM-DD' format
        **kwargs
            Additional filters for metadata search
            
        Returns
        -------
        pd.DataFrame
            Metadata for intersecting datasets
        """
        from starepandas.staredataframe import MAX_PARTITION_LEVEL

        logger.info(f"Finding intersections for {len(location_sids)} SIDs across {instruments}")

        # Handle time range parameters
        if time_range is None and start_date is not None and end_date is not None:
            time_range = (start_date, end_date)

        # Coerce query SIDs down to partition level for coarse matching against grouped_id in RDS.
        # A level-10 query SID maps to its ancestor level-6 trixel, which is what's stored as grouped_id.
        sids_array = np.array(location_sids, dtype=np.int64)
        coerced_sids = pystare.spatial_coerce_resolution(sids_array, MAX_PARTITION_LEVEL)
        coerced_sids = pystare.spatial_clear_to_resolution(coerced_sids)
        query_grouped_ids = set(int(s) for s in coerced_sids)

        logger.info(f"Coerced {len(location_sids)} query SIDs to {len(query_grouped_ids)} "
                     f"partition-level (level {MAX_PARTITION_LEVEL}) grouped IDs")

        # Search metadata for each instrument
        all_results = []
        for instrument in instruments:
            try:
                logger.info(f"Searching {instrument} metadata...")

                # Query metadata — try exact match first, then LIKE for scan-based
                # datasets stored as e.g. "GMI_S1", "GMI_S2" when queried as "GMI".
                metadata = starepandas.io.granules.load_s3_metadata(
                    dataset=instrument,
                    **kwargs
                )
                if metadata.empty:
                    metadata = starepandas.io.granules.load_s3_metadata(
                        dataset_prefix=instrument,
                        **kwargs
                    )

                if metadata.empty:
                    logger.warning(f"No metadata found for {instrument}")
                    continue

                # Filter by coerced grouped SIDs — stored grouped_ids are already
                # at partition level since to_s3() partitions at that level
                intersecting_results = []
                for _, row in metadata.iterrows():
                    row_grouped_id = row.get('grouped_id')
                    if pd.isna(row_grouped_id):
                        continue

                    try:
                        if int(row_grouped_id) in query_grouped_ids:
                            intersecting_results.append(row)
                    except (ValueError, TypeError):
                        continue

                logger.info(f"Found {len(intersecting_results)} intersecting partitions for {instrument}")
                all_results.extend(intersecting_results)

            except Exception as e:
                logger.error(f"Error searching {instrument} metadata: {e}")
                continue

        if not all_results:
            logger.warning("No intersecting data found")
            return pd.DataFrame()

        result_df = pd.DataFrame(all_results)
        logger.info(f"Found {len(result_df)} total intersecting partitions")
        return result_df
    
    def download_and_analyze(self, intersecting_metadata: pd.DataFrame,
                           instruments: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Download intersecting Parquet partitions and create STAREDataFrames.

        Parameters
        ----------
        intersecting_metadata : pd.DataFrame
            Metadata from find_intersecting_data()
        instruments : List[str], optional
            Specific instruments to analyze (if None, analyze all)

        Returns
        -------
        Dict[str, pd.DataFrame]
            Dictionary mapping instrument to STAREDataFrames
        """
        import pyarrow.parquet as pq
        import s3fs

        if intersecting_metadata.empty:
            logger.warning("No metadata to download")
            return {}

        if instruments is None:
            instruments = intersecting_metadata['Dataset'].unique().tolist()

        logger.info(f"Downloading intersecting partitions for {instruments}")

        merged_opts = dict(starepandas.staredataframe._AWS_S3_STORAGE_OPTIONS)
        parquet_fs = s3fs.S3FileSystem(**merged_opts) if merged_opts else s3fs.S3FileSystem()

        data_results = {}

        for instrument in instruments:
            instrument_data = intersecting_metadata[
                intersecting_metadata['Dataset'] == instrument
            ]

            if instrument_data.empty:
                logger.warning(f"No data found for {instrument}")
                continue

            total_chunks = len(instrument_data)
            logger.info(f"Processing {total_chunks} partitions for {instrument}")

            dfs_by_chunk = []
            for chunk_i, (_, chunk_meta) in enumerate(instrument_data.iterrows()):
                grouped_id = chunk_meta.get('grouped_id', '')
                try:
                    group_path = chunk_meta.get('group_path', '')
                    if not group_path:
                        continue

                    # group_path is the full s3://bucket/.../<dataset>.parquet path
                    read_path = group_path[len('s3://'):] if group_path.startswith('s3://') else group_path
                    logger.debug(f"Downloading partition {grouped_id} from {group_path}")

                    df = pq.read_table(read_path, filesystem=parquet_fs).to_pandas()

                    if not df.empty:
                        dfs_by_chunk.append(df)
                        logger.debug(f"✓ Downloaded {len(df)} rows for partition {grouped_id}")
                        if (chunk_i + 1) % 500 == 0:
                            logger.info(f"  {instrument}: downloaded {chunk_i + 1}/{total_chunks} partitions ...")
                    else:
                        logger.warning(f"Empty result for partition {grouped_id}")

                except Exception as e:
                    logger.error(f"Error downloading partition {grouped_id}: {e}")
                    continue

            if dfs_by_chunk:
                combined_df = pd.concat(dfs_by_chunk, ignore_index=True)
                if '__row_positions__' in combined_df.columns:
                    combined_df = combined_df.sort_values('__row_positions__').drop(
                        columns=['__row_positions__']
                    ).reset_index(drop=True)
                data_results[instrument] = starepandas.STAREDataFrame(combined_df)
                logger.info(f"✓ Combined {len(combined_df)} rows for {instrument}")
            else:
                logger.warning(f"No successful downloads for {instrument}")

        logger.info(f"Downloaded data for {len(data_results)} instruments")
        return data_results
    
    def plot_comparison(self, data_dict: Dict[str, pd.DataFrame], 
                    location: str, variables: Optional[List[str]] = None):
        """
        Plot comparison of different instruments at same location.
        
        Parameters
        ----------
        data_dict : Dict[str, pd.DataFrame]
            Dictionary mapping instrument to STAREDataFrames
        location : str
            Description of location being analyzed
        variables : List[str], optional
            Specific variables to plot (if None, plot available brightness temps)
        """
        if not data_dict:
            logger.warning("No data to plot")
            return
        
        try:
            import matplotlib.pyplot as plt
            
            # Determine variables to plot
            if variables is None:
                variables = []
                for instrument, df in data_dict.items():
                    # Find temperature variables (Tc*)
                    temp_vars = [col for col in df.columns if col.startswith('Tc')]
                    variables.extend(temp_vars)
                variables = list(set(variables))[:4]  # Limit to 4 variables
            
            if not variables:
                logger.warning("No temperature variables found for plotting")
                return
            
            # Create comparison plots
            n_instruments = len(data_dict)
            n_vars = len(variables)
            
            fig, axes = plt.subplots(n_vars, n_instruments, 
                                   figsize=(4*n_instruments, 3*n_vars))
            fig.suptitle(f'Instrument Comparison at {location}', fontsize=14, fontweight='bold')
            
            instrument_names = list(data_dict.keys())
            
            for i, var in enumerate(variables):
                for j, (instrument, df) in enumerate(data_dict.items()):
                    ax = axes[i, j] if n_vars > 1 else axes[j]
                    
                    if var in df.columns:
                        # Plot histogram of temperature values
                        values = df[var].dropna()
                        if not values.empty:
                            ax.hist(values, bins=50, alpha=0.7, edgecolor='black')
                            ax.set_title(f'{instrument}\n{var}')
                            ax.set_xlabel('Temperature (K)')
                            ax.set_ylabel('Frequency')
                            ax.grid(True, alpha=0.3)
                    else:
                        ax.text(0.5, 0.5, 'No Data', ha='center', va='center', 
                                transform=ax.transAxes, fontsize=12)
                        ax.set_title(f'{instrument}\n{var}')
            
            plt.tight_layout()
            plt.show()
            
            logger.info(f"✓ Plotted comparison for {location}")
            
        except ImportError:
            logger.warning("Matplotlib not available for plotting")
        except Exception as e:
            logger.error(f"Error plotting comparison: {e}")
    
    def get_sids_for_bbox(self, lon_min: float, lat_min: float, 
                         lon_max: float, lat_max: float, level: int = 10) -> List[int]:
        """
        Get STARE SIDs for a bounding box.
        
        Parameters
        ----------
        lon_min, lat_min, lon_max, lat_max : float
            Bounding box coordinates
        level : int
            STARE level (default: 10)
            
        Returns
        -------
        List[int]
            STARE SIDs within the bounding box
        """
        # Create bounding box coordinates for hull
        # cover_from_hull expects arrays of lat/lon coordinates for polygon vertices
        # Counter-clockwise order: bottom-left, bottom-right, top-right, top-left
        lats = [lat_min, lat_min, lat_max, lat_max]
        lons = [lon_min, lon_max, lon_max, lon_min]
        
        # Convert to STARE SIDs using hull cover function
        sids = pystare.cover_from_hull(lats, lons, level)
        return sids.tolist()
    
    def reconstitute_hdf5(
        self,
        dataset,
        output_hdf5_path: str,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        area_sids: Optional[List[int]] = None,
        s3_root: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        pixel_width: Optional[int] = None,
        compression: str = 'gzip',
        compression_opts: int = 4,
    ) -> str:
        """
        Reconstitute an HDF5 granule from Parquet partitions stored in S3.

        Queries the RDS metadata to find Parquet partitions whose STARE
        partition SIDs intersect the requested area, downloads only those
        files, and writes an HDF5 file that matches the original granule
        structure.

        Parameters
        ----------
        dataset : str or list of str
            Dataset / scan identifier(s) (e.g. ``"GMI_S1"`` or
            ``["GMI_S1", "GMI_S2"]``).  When a list is given, all scans are
            written into the same output HDF5 file as separate top-level
            groups (e.g. ``/S1``, ``/S2``).
        output_hdf5_path : str
            Destination path for the reconstituted HDF5 file.  Parent
            directories are created automatically.
        bbox : tuple of float, optional
            Bounding box ``(lon_min, lat_min, lon_max, lat_max)``.  Exactly
            one of ``bbox`` or ``area_sids`` must be given.
        area_sids : list of int, optional
            STARE SIDs covering the area of interest.  Exactly one of
            ``bbox`` or ``area_sids`` must be given.
        s3_root : str, optional
            Root S3 path used as a fallback when the RDS metadata does not
            contain a ``group_path`` for a matching SID.
        s3_prefix : str, optional
            Filter RDS metadata to group_paths starting with this prefix
            (e.g. a single granule's S3 path) to avoid mixing data from
            different ingestion runs.
        pixel_width : int, optional
            Explicit pixel_width override.
        compression : str, optional
            HDF5 compression filter (default ``'gzip'``).
        compression_opts : int, optional
            Compression level 0–9 (default ``4``).

        Returns
        -------
        str
            ``output_hdf5_path`` — path of the written HDF5 file.
        """
        # Relaxed (2026-05-25, task 13 — match LocalStarePodsDemo behaviour):
        # both None means "no spatial filter — reconstitute the full granule".
        # Either set, but not both.
        if bbox is not None and area_sids is not None:
            raise ValueError(
                "Provide at most one of 'bbox' or 'area_sids', not both."
            )

        datasets = [dataset] if isinstance(dataset, str) else list(dataset)
        resolved_root = s3_root or "s3://"

        for i, ds in enumerate(datasets):
            if bbox is not None:
                area_desc = f"bbox={bbox}"
            elif area_sids is not None:
                area_desc = f"{len(area_sids)} area SIDs"
            else:
                area_desc = "full granule (no spatial filter)"
            logger.info(f"Reconstituting HDF5 for dataset='{ds}' over {area_desc}")

            # First scan creates the file ('w'), subsequent scans append ('a')
            hdf5_mode = 'w' if i == 0 else 'a'

            starepandas.io.granules.reconstitute_hdf5_from_s3(
                s3_root=resolved_root,
                dataset=ds,
                output_hdf5_path=output_hdf5_path,
                area_sids=area_sids,
                bbox=bbox,
                s3_prefix=s3_prefix,
                pixel_width=pixel_width,
                compression=compression,
                compression_opts=compression_opts,
                mode=hdf5_mode,
            )

        logger.info(f"✓ Reconstituted HDF5 written to {output_hdf5_path}")
        return output_hdf5_path

    def run_full_demo(self, data_root: str, location_bbox: Tuple[float, float, float, float],
                    location_name: str = "Study Area",
                    reconstitute_output_dir: Optional[str] = None,
                    reconstitute_datasets: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run complete STARE-PODS demonstration workflow.

        Parameters
        ----------
        data_root : str
            Root path containing instrument data folders.
        location_bbox : Tuple[float, float, float, float]
            Bounding box ``(lon_min, lat_min, lon_max, lat_max)``.
        location_name : str
            Name of the location for visualization labels.
        reconstitute_output_dir : str, optional
            If provided, reconstitute HDF5 files for each dataset into this
            directory after downloading the intersecting data.  One ``.h5``
            file is written per dataset (e.g. ``GMI_S1.h5``).  When ``None``
            the reconstitution step is skipped.
        reconstitute_datasets : list of str, optional
            Dataset / scan identifiers to reconstitute
            (e.g. ``["GMI_S1", "AMSR2_S5"]``).  Defaults to ``["GMI_S1"]``
            when ``reconstitute_output_dir`` is set but this argument is not.

        Returns
        -------
        Dict[str, Any]
            ``"data"`` → ``Dict[str, STAREDataFrame]`` (downloaded chunks),
            ``"hdf5_paths"`` → ``Dict[str, str]`` (reconstituted HDF5 paths,
            only present when ``reconstitute_output_dir`` is given).
        """
        logger.info(f"Starting STARE-PODS demo for {location_name}")

        # Step 1: Get location SIDs
        lon_min, lat_min, lon_max, lat_max = location_bbox
        location_sids = self.get_sids_for_bbox(lon_min, lat_min, lon_max, lat_max)
        logger.info(f"Generated {len(location_sids)} SIDs for {location_name}")

        # Step 2: Define instruments and data paths
        instruments = ['GMI', 'AMSR2', 'SSMIS', 'ATMS']
        data_paths = {
            'GMI': f"{data_root}/GPM",
            'AMSR2': f"{data_root}/AMSR2",
            'SSMIS': f"{data_root}/SSMIS",
            'ATMS': f"{data_root}/ATMS"
        }

        # Step 3: Ingest granules (only if not already done)
        logger.info("Starting granule ingestion...")
        for instrument in instruments:
            data_path = data_paths[instrument]
            if os.path.exists(data_path):
                s3_prefix = f"s3://zarrpods/{instrument.lower()}-demo"
                self.ingest_granules(data_path, instrument, s3_prefix)
            else:
                logger.warning(f"Data path not found: {data_path}")

        # Step 4: Find intersecting data
        logger.info("Finding intersecting data...")
        intersecting_metadata = self.find_intersecting_data(location_sids, instruments)

        if intersecting_metadata.empty:
            logger.warning("No intersecting data found")
            return {"data": {}}

        # Step 5: Download and analyze
        logger.info("Downloading intersecting partitions...")
        data_dict = self.download_and_analyze(intersecting_metadata, instruments)

        # Step 6: Plot comparison
        if data_dict:
            logger.info("Creating comparison plots...")
            self.plot_comparison(data_dict, location_name)

        result: Dict[str, Any] = {"data": data_dict}

        # Step 7 (optional): Reconstitute HDF5 files from S3 Parquet
        if reconstitute_output_dir is not None:
            os.makedirs(reconstitute_output_dir, exist_ok=True)
            datasets_to_reconstitute = reconstitute_datasets or ["GMI_S1"]
            hdf5_paths: Dict[str, str] = {}

            logger.info(
                f"Reconstituting HDF5 for {datasets_to_reconstitute} → {reconstitute_output_dir}"
            )
            for ds in datasets_to_reconstitute:
                out_path = os.path.join(reconstitute_output_dir, f"{ds}.h5")
                try:
                    self.reconstitute_hdf5(
                        dataset=ds,
                        output_hdf5_path=out_path,
                        bbox=location_bbox,
                    )
                    hdf5_paths[ds] = out_path
                except Exception as e:
                    logger.error(f"✗ HDF5 reconstitution failed for {ds}: {e}")

            result["hdf5_paths"] = hdf5_paths
            logger.info(f"✓ Reconstituted {len(hdf5_paths)} HDF5 file(s)")

        logger.info(f"✓ STARE-PODS demo completed for {location_name}")
        return result


class LocalStarePodsDemo:
    """
    Local STARE-PODS pipeline — no AWS or RDS required.

    Mirrors :class:`StarePodsDemo` but writes Parquet partitions to the
    local filesystem and stores metadata in a SQLite database.  Useful for
    development, offline work, or environments without cloud access.

    Directory layout (hierarchical quaternary pod-code tree, Parquet leaves)::

        <local_root>/
        ├── metadata.db                          # SQLite — PodsMetadata table
        └── q13/q132/q1321/q13211/
            └── q13211-<granule_basename>-<dataset>.parquet  # one chunk per pod

    Parameters
    ----------
    local_root : str
        Root directory for Parquet storage and the SQLite database file.
        Created automatically if it does not exist.
    """

    def __init__(self, local_root: str = "/tmp/stare_pods_local"):
        self.local_root = os.path.abspath(local_root)
        self.db_path = os.path.join(self.local_root, "metadata.db")
        os.makedirs(self.local_root, exist_ok=True)

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def ingest_granules(
        self,
        data_path: str,
        instrument: str,
        scan: Optional[str] = None,
        level: int = 10,
        **kwargs,
    ) -> List[str]:
        """Thin shim — delegates to :func:`starepandas.ingest.ingest_granules_local`.

        Forwards ``self.local_root`` and ``self.db_path`` so the function-
        level callable knows where the SQLite catalog lives. Preserved so
        existing notebooks calling ``LocalStarePodsDemo().ingest_granules(...)``
        keep working unchanged after the task-7 extraction.
        """
        from starepandas.ingest import ingest_granules_local
        return ingest_granules_local(
            data_path=data_path,
            instrument=instrument,
            local_root=self.local_root,
            scan=scan,
            level=level,
            db_path=self.db_path,
            **kwargs,
        )

    # ── Spatial query ─────────────────────────────────────────────────────────

    def get_sids_for_bbox(
        self,
        lon_min: float,
        lat_min: float,
        lon_max: float,
        lat_max: float,
        level: int = 10,
    ) -> List[int]:
        """Convert a bounding box to STARE SIDs (identical to StarePodsDemo)."""
        lats = [lat_min, lat_min, lat_max, lat_max]
        lons = [lon_min, lon_max, lon_max, lon_min]
        sids = pystare.cover_from_hull(lats, lons, level)
        return sids.tolist()

    def find_intersecting_data(
        self,
        location_sids: List[int],
        instruments: List[str],
        **kwargs,
    ) -> pd.DataFrame:
        """
        Find metadata rows whose STARE partition SIDs intersect ``location_sids``.

        Parameters
        ----------
        location_sids : list of int
            STARE SIDs for the area of interest.
        instruments : list of str
            Instrument / dataset names to search (e.g. ``["GMI"]``).
        **kwargs
            Forwarded to :func:`load_local_metadata`.

        Returns
        -------
        pandas.DataFrame
            Matching metadata rows.
        """
        # No spatial filter — return all partitions for the requested instruments
        if location_sids is None:
            all_meta = []
            for instrument in instruments:
                try:
                    meta = starepandas.io.granules.load_local_metadata(
                        self.db_path, dataset=instrument, **kwargs
                    )
                    if meta is None or meta.empty:
                        meta = starepandas.io.granules.load_local_metadata(
                            self.db_path, dataset_prefix=instrument, **kwargs
                        )
                    if meta is not None and not meta.empty:
                        logger.info(f"Loaded all {len(meta)} partitions for {instrument}")
                        all_meta.append(meta)
                    else:
                        logger.warning(f"No metadata found for {instrument}")
                except Exception as e:
                    logger.error(f"Error loading {instrument} metadata: {e}")
            return pd.concat(all_meta, ignore_index=True) if all_meta else pd.DataFrame()

        from starepandas.staredataframe import MAX_PARTITION_LEVEL

        sids_array = np.array(location_sids, dtype=np.int64)
        coerced = pystare.spatial_coerce_resolution(sids_array, MAX_PARTITION_LEVEL)
        coerced = pystare.spatial_clear_to_resolution(coerced)
        query_ids = set(int(s) for s in coerced)

        logger.info(
            f"Coerced {len(location_sids)} query SIDs → {len(query_ids)} partition-level IDs"
        )

        all_results = []
        for instrument in instruments:
            try:
                meta = starepandas.io.granules.load_local_metadata(
                    self.db_path, dataset=instrument, **kwargs
                )
                if meta.empty:
                    meta = starepandas.io.granules.load_local_metadata(
                        self.db_path, dataset_prefix=instrument, **kwargs
                    )

                if meta.empty:
                    logger.warning(f"No metadata found for {instrument}")
                    continue

                for _, row in meta.iterrows():
                    gid = row.get('grouped_id')
                    if pd.isna(gid):
                        continue
                    try:
                        if int(gid) in query_ids:
                            all_results.append(row)
                    except (ValueError, TypeError):
                        continue

                logger.info(
                    f"Found {sum(1 for r in all_results if r.get('Dataset', '').startswith(instrument))} "
                    f"intersecting chunks for {instrument}"
                )

            except Exception as e:
                logger.error(f"Error searching {instrument} metadata: {e}")
                continue

        if not all_results:
            logger.warning("No intersecting data found")
            return pd.DataFrame()

        return pd.DataFrame(all_results)

    # ── Download / analyse ────────────────────────────────────────────────────

    def download_and_analyze(
        self,
        intersecting_metadata: pd.DataFrame,
        instruments: Optional[List[str]] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Load intersecting Parquet partitions from local disk into STAREDataFrames.

        Parameters
        ----------
        intersecting_metadata : pandas.DataFrame
            Output of :meth:`find_intersecting_data`.
        instruments : list of str, optional
            Restrict to these datasets.  ``None`` uses all found in metadata.

        Returns
        -------
        dict
            Mapping ``dataset_name → STAREDataFrame``.
        """
        import pyarrow.parquet as pq

        if intersecting_metadata.empty:
            logger.warning("No metadata to load")
            return {}

        if instruments is None:
            instruments = intersecting_metadata['Dataset'].unique().tolist()

        data_results = {}
        for instrument in instruments:
            rows = intersecting_metadata[intersecting_metadata['Dataset'] == instrument]
            if rows.empty:
                logger.warning(f"No data found for {instrument}")
                continue

            frames = []
            total = len(rows)
            for i, (_, chunk) in enumerate(rows.iterrows()):
                gpath = chunk.get('group_path', '')
                if not gpath or not os.path.isfile(gpath):
                    continue
                try:
                    df_chunk = pq.read_table(gpath).to_pandas()
                    if df_chunk.empty:
                        continue
                    frames.append(df_chunk)
                    logger.debug(f"✓ Loaded {len(df_chunk)} rows from {gpath}")
                    if (i + 1) % 500 == 0:
                        logger.info(f"  {instrument}: loaded {i + 1}/{total} partitions ...")
                except Exception as e:
                    logger.error(f"Error loading {gpath}: {e}")

            if frames:
                combined = pd.concat(frames, ignore_index=True)
                if '__row_positions__' in combined.columns:
                    combined = combined.sort_values('__row_positions__').drop(
                        columns=['__row_positions__']
                    ).reset_index(drop=True)
                data_results[instrument] = starepandas.STAREDataFrame(combined)
                logger.info(f"✓ Combined {len(combined)} rows for {instrument}")

        return data_results

    # ── HDF5 reconstitution ───────────────────────────────────────────────────

    def reconstitute_hdf5(
        self,
        dataset,
        output_hdf5_path: str,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        area_sids: Optional[List[int]] = None,
        local_prefix: Optional[str] = None,
        granule_name: Optional[str] = None,
        pixel_width: Optional[int] = None,
        compression: str = 'gzip',
        compression_opts: int = 4,
    ) -> str:
        """
        Reconstitute an HDF5 granule from local Parquet partitions.

        Parameters
        ----------
        dataset : str or list of str
            Dataset / scan identifier(s) (e.g. ``"GMI_S1"`` or
            ``["GMI_S1", "GMI_S2"]``).
        output_hdf5_path : str
            Destination HDF5 file path.
        bbox : tuple of float, optional
            ``(lon_min, lat_min, lon_max, lat_max)``.  Exactly one of ``bbox``
            or ``area_sids`` must be given.
        area_sids : list of int, optional
            STARE SIDs for the area of interest.
        local_prefix : str, optional
            Filter metadata to group_paths starting with this prefix (scopes to
            a particular ``local_root``).  Note: under the HTM-first layout the
            granule basename is mid-path, so this no longer scopes to a single
            granule — use ``granule_name`` for that.
        granule_name : str, optional
            Filter to rows whose recorded granule_name matches.  Preferred
            per-granule filter under the HTM-first layout.
        pixel_width : int, optional
            Explicit pixel_width override.
        compression : str, optional
            HDF5 compression filter (default ``'gzip'``).
        compression_opts : int, optional
            Compression level (default ``4``).

        Returns
        -------
        str
            ``output_hdf5_path``
        """
        if bbox is not None and area_sids is not None:
            raise ValueError("Provide at most one of 'bbox' or 'area_sids', not both.")

        datasets = [dataset] if isinstance(dataset, str) else list(dataset)

        for i, ds in enumerate(datasets):
            logger.info(f"Reconstituting HDF5 for dataset='{ds}' over bbox={bbox}")
            hdf5_mode = 'w' if i == 0 else 'a'

            starepandas.io.granules.reconstitute_hdf5_from_local(
                db_path=self.db_path,
                dataset=ds,
                output_hdf5_path=output_hdf5_path,
                area_sids=area_sids,
                bbox=bbox,
                local_prefix=local_prefix,
                granule_name=granule_name,
                pixel_width=pixel_width,
                compression=compression,
                compression_opts=compression_opts,
                mode=hdf5_mode,
            )

        logger.info(f"✓ Reconstituted HDF5 written to {output_hdf5_path}")
        return output_hdf5_path


def get_sids_for_region(region_name: str, level: int = 10) -> List[int]:
    """Get STARE SIDs for a predefined region."""
    regions = {
        'california': (-125, 32, -115, 42),
        'europe': (-10, 35, 30, 70),
        'asia': (60, 5, 150, 50),
        'global': (-180, -90, 180, 90),
    }
    if region_name.lower() not in regions:
        raise ValueError(f"Unknown region: {region_name}")
    bbox = regions[region_name.lower()]
    return StarePodsDemo().get_sids_for_bbox(*bbox, level)