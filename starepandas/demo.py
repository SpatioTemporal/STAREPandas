#!/usr/bin/env python3
"""
STARE-PODS Demonstration API

High-level API for demonstrating STARE-PODS workflow:
1. Ingest granules into zarr chunks stored in S3
2. Find intersecting data across different instruments using STARE SIDs
3. Download and analyze only intersecting chunks
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
    - Ingesting granules into S3 zarr storage
    - Finding intersecting data using STARE spatial indexing
    - Downloading and analyzing intersecting chunks
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
        
    def ingest_granules(self, data_path: str, instrument: str, s3_prefix: str,
                     scan: Optional[str] = None, level: int = 10, **kwargs) -> List[str]:
        """
        Partition granules into zarr chunks and store in S3.
        
        Parameters
        ----------
        data_path : str
            Path to granule files (supports glob patterns)
        instrument : str
            Instrument name (GMI, AMSR2, SSMIS, ATMS)
        s3_prefix : str
            S3 prefix for storage (e.g., "s3://zarrpods/instrument-data")
        scan : str, optional
            Specific scan to process (e.g., "S1", "S2")
        **kwargs
            Additional arguments for to_zarr_s3()
            
        Returns
        -------
        List[str]
            List of S3 paths where data was stored
        """
        logger.info(f"Ingesting {instrument} granules from {data_path}")
        
        # Find granule files
        import glob
        import os
        
        if os.path.isdir(data_path):
            pattern = f"{data_path}/**/*.HDF5"
            granule_files = glob.glob(pattern, recursive=True)
        elif '*' in data_path or '?' in data_path:
            granule_files = glob.glob(data_path)
        else:
            granule_files = [data_path] if os.path.exists(data_path) else []
        
        if not granule_files:
            logger.warning(f"No granule files found in {data_path}")
            return []
        
        logger.info(f"Found {len(granule_files)} {instrument} files")
        
        # Process each granule
        s3_paths = []
        for granule_file in granule_files:
            try:
                logger.info(f"Processing {os.path.basename(granule_file)}")
                
                # Generate S3 path
                base_name = os.path.splitext(os.path.basename(granule_file))[0]
                s3_path = f"{s3_prefix}/{base_name}"
                
                kwargs.setdefault('read_timestamp', True)
                s3_result = starepandas.io.granules.to_zarr_s3(
                    file_path=granule_file,
                    s3_path=s3_path,
                    level=level,
                    dataset=instrument,
                    scan=scan,
                    **kwargs
                )
                
                if isinstance(s3_result, list):
                    s3_paths.extend(s3_result)
                else:
                    s3_paths.append(s3_result)
                    
                logger.info(f"✓ Stored {os.path.basename(granule_file)} to {s3_result}")
                
            except Exception as e:
                logger.error(f"✗ Failed to process {granule_file}: {e}")
                continue
        
        logger.info(f"Ingested {len(s3_paths)} zarr datasets")
        return s3_paths
    
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
                metadata = starepandas.io.granules.load_zarr_metadata(
                    dataset=instrument,
                    **kwargs
                )
                if metadata.empty:
                    metadata = starepandas.io.granules.load_zarr_metadata(
                        dataset_prefix=instrument,
                        **kwargs
                    )

                if metadata.empty:
                    logger.warning(f"No metadata found for {instrument}")
                    continue

                # Filter by coerced grouped SIDs — stored grouped_ids are already
                # at partition level since to_zarr_s3() partitions at that level
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

                logger.info(f"Found {len(intersecting_results)} intersecting chunks for {instrument}")
                all_results.extend(intersecting_results)

            except Exception as e:
                logger.error(f"Error searching {instrument} metadata: {e}")
                continue

        if not all_results:
            logger.warning("No intersecting data found")
            return pd.DataFrame()

        result_df = pd.DataFrame(all_results)
        logger.info(f"Found {len(result_df)} total intersecting chunks")
        return result_df
    
    def download_and_analyze(self, intersecting_metadata: pd.DataFrame, 
                           instruments: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Download intersecting chunks and create STAREDataFrames.
        
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
        if intersecting_metadata.empty:
            logger.warning("No metadata to download")
            return {}
        
        if instruments is None:
            instruments = intersecting_metadata['Dataset'].unique().tolist()
        
        logger.info(f"Downloading intersecting chunks for {instruments}")
        
        # Group by instrument and S3 path
        data_results = {}
        
        for instrument in instruments:
            instrument_data = intersecting_metadata[
                intersecting_metadata['Dataset'] == instrument
            ]
            
            if instrument_data.empty:
                logger.warning(f"No data found for {instrument}")
                continue
            
            total_chunks = len(instrument_data)
            logger.info(f"Processing {total_chunks} chunks for {instrument}")

            dfs_by_chunk = []
            for chunk_i, (_, chunk_meta) in enumerate(instrument_data.iterrows()):
                try:
                    # Extract S3 path and chunk information
                    s3_bucket = chunk_meta['S3 bucket']
                    group_path = chunk_meta.get('group_path', '')
                    grouped_id = chunk_meta.get('grouped_id', '')
                    
                    if not group_path:
                        continue
                    
                    # Construct full S3 path
                    s3_path = f"s3://{s3_bucket}/{group_path}"
                    
                    logger.debug(f"Downloading chunk {grouped_id} from {s3_path}")

                    # Use existing function to download specific groups
                    if grouped_id:
                        df = starepandas.io.granules.from_zarr_s3_chunked_groups(
                            s3_path=s3_path,
                            group_sid_ids=[grouped_id]
                        )
                    else:
                        # Fallback to full chunked reading
                        df = starepandas.io.granules.from_zarr_s3_chunked(s3_path)

                    if not df.empty:
                        dfs_by_chunk.append(df)
                        logger.debug(f"✓ Downloaded {len(df)} rows for chunk {grouped_id}")
                        if (chunk_i + 1) % 500 == 0:
                            logger.info(f"  {instrument}: downloaded {chunk_i + 1}/{total_chunks} chunks ...")
                    else:
                        logger.warning(f"Empty result for chunk {grouped_id}")
                        
                except Exception as e:
                    logger.error(f"Error downloading chunk {grouped_id}: {e}")
                    continue
            
            if dfs_by_chunk:
                # Combine all chunks for this instrument
                combined_df = pd.concat(dfs_by_chunk, ignore_index=True)
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
        s3_zarr_path: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        pixel_width: Optional[int] = None,
        compression: str = 'gzip',
        compression_opts: int = 4,
    ) -> str:
        """
        Reconstitute an HDF5 granule from zarr chunks stored in S3.

        Queries the RDS metadata to find zarr groups whose STARE partition
        SIDs intersect the requested area, downloads only those groups, and
        writes an HDF5 file that matches the original granule structure.

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
        s3_zarr_path : str, optional
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
        if (bbox is None) == (area_sids is None):
            raise ValueError(
                "Provide exactly one of 'bbox' or 'area_sids', not both or neither."
            )

        datasets = [dataset] if isinstance(dataset, str) else list(dataset)
        zarr_path = s3_zarr_path or "s3://"

        for i, ds in enumerate(datasets):
            area_desc = f"bbox={bbox}" if bbox is not None else f"{len(area_sids)} area SIDs"
            logger.info(f"Reconstituting HDF5 for dataset='{ds}' over {area_desc}")

            # First scan creates the file ('w'), subsequent scans append ('a')
            hdf5_mode = 'w' if i == 0 else 'a'

            starepandas.io.granules.reconstitute_hdf5_from_zarr(
                zarr_path=zarr_path,
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
        logger.info("Downloading intersecting chunks...")
        data_dict = self.download_and_analyze(intersecting_metadata, instruments)

        # Step 6: Plot comparison
        if data_dict:
            logger.info("Creating comparison plots...")
            self.plot_comparison(data_dict, location_name)

        result: Dict[str, Any] = {"data": data_dict}

        # Step 7 (optional): Reconstitute HDF5 files from S3 zarr
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

    Mirrors :class:`StarePodsDemo` but writes zarr groups to the local
    filesystem and stores metadata in a SQLite database.  Useful for
    development, offline work, or environments without cloud access.

    Directory layout::

        <local_root>/
        ├── metadata.db              # SQLite — PodsMetadata table
        └── <granule_basename>/      # one directory per granule
            ├── <grouped_id_0>/      # flat zarr group dirs
            │   ├── lat/
            │   ├── lon/
            │   ├── Tc1/ …
            │   └── __row_positions__/
            └── <grouped_id_N>/

    Parameters
    ----------
    local_root : str
        Root directory for zarr storage and the SQLite database file.
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
        """
        Ingest granule files into local zarr storage and record metadata.

        Parameters
        ----------
        data_path : str
            Path to a single granule file, a directory, or a glob pattern.
        instrument : str
            Instrument / dataset name (e.g. ``"GMI"``).
        scan : str, optional
            Specific scan to process (e.g. ``"S1"``); ``None`` processes all.
        level : int, optional
            STARE partitioning level (default 10).
        **kwargs
            Forwarded to :func:`~starepandas.io.granules.to_zarr_local_meta`.

        Returns
        -------
        list of str
            Local paths written for each processed granule.
        """
        import glob

        if os.path.isdir(data_path):
            granule_files = glob.glob(f"{data_path}/**/*.HDF5", recursive=True)
        elif '*' in data_path or '?' in data_path:
            granule_files = glob.glob(data_path)
        else:
            granule_files = [data_path] if os.path.exists(data_path) else []

        if not granule_files:
            logger.warning(f"No granule files found in {data_path}")
            return []

        logger.info(f"Found {len(granule_files)} {instrument} file(s)")
        local_paths = []

        for granule_file in granule_files:
            try:
                logger.info(f"Processing {os.path.basename(granule_file)}")
                base_name = os.path.splitext(os.path.basename(granule_file))[0]
                granule_local_path = os.path.join(self.local_root, base_name)

                kwargs.setdefault('read_timestamp', True)
                result = starepandas.io.granules.to_zarr_local_meta(
                    file_path=granule_file,
                    local_path=granule_local_path,
                    level=level,
                    db_path=self.db_path,
                    dataset=instrument,
                    scan=scan,
                    **kwargs,
                )

                if isinstance(result, list):
                    local_paths.extend(result)
                else:
                    local_paths.append(result)

                logger.info(f"✓ Stored {os.path.basename(granule_file)} → {granule_local_path}")

            except Exception as e:
                logger.error(f"✗ Failed to process {granule_file}: {e}")
                continue

        logger.info(f"Ingested {len(local_paths)} zarr dataset(s)")
        return local_paths

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
            Forwarded to :func:`load_local_zarr_metadata`.

        Returns
        -------
        pandas.DataFrame
            Matching metadata rows.
        """
        # No spatial filter — return all groups for the requested instruments
        if location_sids is None:
            all_meta = []
            for instrument in instruments:
                try:
                    meta = starepandas.io.granules.load_local_zarr_metadata(
                        self.db_path, dataset=instrument, **kwargs
                    )
                    if meta is None or meta.empty:
                        meta = starepandas.io.granules.load_local_zarr_metadata(
                            self.db_path, dataset_prefix=instrument, **kwargs
                        )
                    if meta is not None and not meta.empty:
                        logger.info(f"Loaded all {len(meta)} groups for {instrument}")
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
                meta = starepandas.io.granules.load_local_zarr_metadata(
                    self.db_path, dataset=instrument, **kwargs
                )
                if meta.empty:
                    meta = starepandas.io.granules.load_local_zarr_metadata(
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
        Load intersecting zarr chunks from local disk into STAREDataFrames.

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
        import zarr as _zarr

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
                if not gpath or not os.path.isdir(gpath):
                    continue
                try:
                    zg = _zarr.open_group(gpath, mode='r')
                    col_data = {k: zg[k][:] for k in zg.array_keys() if k != '__row_positions__'}
                    if not col_data:
                        continue
                    df_chunk = pd.DataFrame(col_data)
                    if '__row_positions__' in zg:
                        df_chunk['__row_positions__'] = zg['__row_positions__'][:]
                    frames.append(df_chunk)
                    logger.debug(f"✓ Loaded {len(df_chunk)} rows from {gpath}")
                    if (i + 1) % 500 == 0:
                        logger.info(f"  {instrument}: loaded {i + 1}/{total} groups ...")
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
        pixel_width: Optional[int] = None,
        compression: str = 'gzip',
        compression_opts: int = 4,
    ) -> str:
        """
        Reconstitute an HDF5 granule from local zarr chunks.

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
            Filter metadata to group_paths starting with this prefix (e.g. the
            granule directory) to avoid mixing data from multiple ingestions.
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

            starepandas.io.granules.reconstitute_hdf5_from_local_zarr(
                db_path=self.db_path,
                dataset=ds,
                output_hdf5_path=output_hdf5_path,
                area_sids=area_sids,
                bbox=bbox,
                local_prefix=local_prefix,
                pixel_width=pixel_width,
                compression=compression,
                compression_opts=compression_opts,
                mode=hdf5_mode,
            )

        logger.info(f"✓ Reconstituted HDF5 written to {output_hdf5_path}")
        return output_hdf5_path


# Convenience functions for easier usage
def get_sids_for_point(lon: float, lat: float, level: int = 10, radius_km: float = 10) -> List[int]:
    """Get STARE SIDs for a point with radius."""
    from shapely.geometry import Point
    point = Point(lon, lat)
    
    # Create circular area (approximate with buffer)
    point_buffered = point.buffer(radius_km/111)  # Rough km to degrees
    
    sids = pystare.sid_from_polygon(point_buffered, level)
    return sids.tolist()


def get_sids_for_region(region_name: str, level: int = 10) -> List[int]:
    """Get STARE SIDs for a predefined region."""
    # Example regions - could be expanded
    regions = {
        'california': (-125, 32, -115, 42),
        'europe': (-10, 35, 30, 70),
        'asia': (60, 5, 150, 50),
        'global': (-180, -90, 180, 90)
    }
    
    if region_name.lower() in regions:
        bbox = regions[region_name.lower()]
        demo = StarePodsDemo()
        return demo.get_sids_for_bbox(*bbox, level)
    else:
        raise ValueError(f"Unknown region: {region_name}")


if __name__ == "__main__":
    import h5py

    CONFIG_PATH = "/Users/tonhai/workspace/Bayesics/StarePandas_par/stare_demo/starepandas/.config"
    GRANULE_FILE = (
        "/Users/tonhai/workspace/Bayesics/L1C_Data_Samples/GPM/2025/Jan_1_2/"
        "1C.GPM.GMI.XCAL2016-C.20250101-S034347-E051659.061567.V07B.HDF5"
    )
    S3_PREFIX    = "s3://zarrpods/gmi-demo"
    BBOX         = (115, -30, 120, -25)   # SW Australia / Perth area (lon_min, lat_min, lon_max, lat_max)
    OUTPUT_HDF5  = "/tmp/gmi_s1_australia.h5"
    ORIGINAL_HDF5 = GRANULE_FILE

    demo = StarePodsDemo(aws_config_path=CONFIG_PATH)

    print("=" * 60)
    print("STARE-PODS Full Demo")
    print("=" * 60)
    print(f"Granule : {os.path.basename(GRANULE_FILE)}")
    print(f"BBox    : {BBOX}  (SE Australia)")
    print(f"S3      : {S3_PREFIX}")
    print()

    # ── Step 1: Ingest the single granule into S3 zarr ──────────────────
    print("Step 1: Ingesting granule into S3 zarr ...")
    s3_paths = demo.ingest_granules(
        data_path=GRANULE_FILE,
        instrument='GMI',
        s3_prefix=S3_PREFIX,
    )
    print(f"  Stored {len(s3_paths)} zarr dataset(s) to S3")
    print()

    # ── Step 2: Find intersecting data via STARE SIDs ───────────────────
    print("Step 2: Finding intersecting data ...")
    location_sids = demo.get_sids_for_bbox(*BBOX, level=10)
    print(f"  Generated {len(location_sids)} SIDs for bbox")
    intersecting = demo.find_intersecting_data(location_sids, instruments=['GMI'])
    print(f"  Found {len(intersecting)} intersecting zarr group(s)")
    print()

    # ── Step 3: Download intersecting chunks ────────────────────────────
    print("Step 3: Downloading intersecting chunks ...")
    data_dict = demo.download_and_analyze(intersecting, instruments=['GMI'])
    for inst, df in data_dict.items():
        print(f"  {inst}: {len(df)} rows, columns: {list(df.columns)}")
    print()

    # ── Step 4: Reconstitute HDF5 from the freshly ingested zarr ─────────
    # Derive the granule's S3 prefix from the ingested paths so reconstitution
    # reads only data from this specific granule (not older ingestions).
    granule_basename = os.path.splitext(os.path.basename(GRANULE_FILE))[0]
    granule_s3_prefix = f"{S3_PREFIX}/{granule_basename}"
    print("Step 4: Reconstituting HDF5 from S3 zarr ...")
    recon_path = demo.reconstitute_hdf5(
        dataset='GMI_S1',
        output_hdf5_path=OUTPUT_HDF5,
        bbox=BBOX,
        s3_prefix=granule_s3_prefix,
    )
    print(f"  Written to: {recon_path}")
    print()

    # ── Step 5: Structure comparison ─────────────────────────────────────
    print("=" * 60)
    print("Structure comparison")
    print("=" * 60)

    def dump_structure(path, label):
        print(f"\n--- {label} ---")
        with h5py.File(path, 'r') as f:
            def visit(name, obj):
                if isinstance(obj, h5py.Dataset):
                    print(f"  /{name:50s} {str(obj.shape):20s} {obj.dtype}")
                elif isinstance(obj, h5py.Group) and name != '/':
                    print(f"  /{name:50s} Group")
            f.visititems(visit)

    dump_structure(OUTPUT_HDF5,  f"RECONSTITUTED  ({os.path.basename(OUTPUT_HDF5)})")
    dump_structure(ORIGINAL_HDF5, f"ORIGINAL       ({os.path.basename(ORIGINAL_HDF5)})")