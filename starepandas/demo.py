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
                     scan: Optional[str] = None, **kwargs) -> List[str]:
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
                
                # Use existing to_zarr_s3 function
                # Use level from kwargs if provided, otherwise default to 10
                level = kwargs.pop('level', 10)
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
        logger.info(f"Finding intersections for {len(location_sids)} SIDs across {instruments}")
        
        # Handle time range parameters
        if time_range is None and start_date is not None and end_date is not None:
            time_range = (start_date, end_date)
        
        # Convert location SIDs to grouped SIDs for hierarchical lookup
        grouped_sids = {}
        for sid in location_sids:
            # Group STARE SIDs to appropriate level (level 10 -> group level)
            grouped_sid = pystare.sid_from_int64_to_int32(sid) // 1000
            grouped_sids[grouped_sid] = sid
        
        logger.info(f"Searching for {len(grouped_sids)} grouped SIDs")
        
        # Search metadata for each instrument
        all_results = []
        for instrument in instruments:
            try:
                logger.info(f"Searching {instrument} metadata...")
                
                # Query metadata using existing function
                metadata = starepandas.io.granules.load_zarr_metadata(
                    dataset=instrument,
                    **kwargs
                )
                
                if metadata.empty:
                    logger.warning(f"No metadata found for {instrument}")
                    continue
                
                # Filter by grouped SIDs
                intersecting_results = []
                for _, row in metadata.iterrows():
                    # Check if this chunk contains any of our location SIDs
                    row_group_sids = row.get('grouped_id', row.get('group_path', '').split('/')[-1])
                    if pd.isna(row_group_sids):
                        continue
                    
                    try:
                        row_group_sids_int = int(row_group_sids)
                        if row_group_sids_int in grouped_sids:
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
            
            logger.info(f"Processing {len(instrument_data)} chunks for {instrument}")
            
            dfs_by_chunk = []
            for _, chunk_meta in instrument_data.iterrows():
                try:
                    # Extract S3 path and chunk information
                    s3_bucket = chunk_meta['S3 bucket']
                    group_path = chunk_meta.get('group_path', '')
                    grouped_id = chunk_meta.get('grouped_id', '')
                    
                    if not group_path:
                        continue
                    
                    # Construct full S3 path
                    s3_path = f"s3://{s3_bucket}/{group_path}"
                    
                    logger.info(f"Downloading chunk {grouped_id} from {s3_path}")
                    
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
                        logger.info(f"✓ Downloaded {len(df)} rows for chunk {grouped_id}")
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
    
    def run_full_demo(self, data_root: str, location_bbox: Tuple[float, float, float, float],
                    location_name: str = "Study Area") -> Dict[str, pd.DataFrame]:
        """
        Run complete STARE-PODS demonstration workflow.
        
        Parameters
        ----------
        data_root : str
            Root path containing instrument data folders
        location_bbox : Tuple[float, float, float, float]
            Bounding box (lon_min, lat_min, lon_max, lat_max)
        location_name : str
            Name of the location for visualization
            
        Returns
        -------
        Dict[str, pd.DataFrame]
            Downloaded and analyzed data
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
            return {}
        
        # Step 5: Download and analyze
        logger.info("Downloading intersecting chunks...")
        data_dict = self.download_and_analyze(intersecting_metadata, instruments)
        
        # Step 6: Plot comparison
        if data_dict:
            logger.info("Creating comparison plots...")
            self.plot_comparison(data_dict, location_name)
        
        logger.info(f"✓ STARE-PODS demo completed for {location_name}")
        return data_dict


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
    # Example usage
    demo = StarePodsDemo()
    
    # Example: California coast
    data_root = "/Users/tonhai/workspace/Bayesics/L1C_Data_Samples"
    ca_bbox = (-125, 32, -115, 42)
    
    print("=== STARE-PODS Demo ===")
    print("Demonstrating workflow with sample data...")
    print("This will:")
    print("1. Ingest granules into S3 zarr storage")
    print("2. Find intersections using STARE spatial indexing") 
    print("3. Download only intersecting chunks")
    print("4. Compare instruments at same location")
    print()
    
    data = demo.run_full_demo(data_root, ca_bbox, "California Coast")