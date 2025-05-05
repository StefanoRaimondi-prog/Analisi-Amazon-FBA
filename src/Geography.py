import pandas as pd
import logging
import os
from typing import Dict, Optional

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def define_regions(mapping: Dict[str, str]) -> Dict[str, str]:
    """
    Validate and return a country-to-region mapping.

    Args:
        mapping: Dict where keys are country codes/names and values are region names.

    Returns:
        The validated mapping dictionary.

    Raises:
        ValueError: If mapping is empty.
    """
    if not mapping:
        logger.error("Empty mapping provided to define_regions.")
        raise ValueError("Region mapping cannot be empty.")
    logger.info(f"Defined {len(mapping)} region mappings.")
    return mapping


def map_to_region(
    df: pd.DataFrame,
    geo_col: str,
    mapping: Dict[str, str],
    default_region: Optional[str] = 'Unknown'
) -> pd.DataFrame:
    """
    Map geographic column values to regions.

    Args:
        df: Input DataFrame with geographic information.
        geo_col: Column name containing country or state.
        mapping: Dict for mapping values in geo_col to region names.
        default_region: Name to assign if no mapping found.

    Returns:
        DataFrame with new column 'region'.

    Raises:
        ValueError: If geo_col not in df.
    """
    if geo_col not in df.columns:
        logger.error(f"Geo column '{geo_col}' not found.")
        raise ValueError(f"Column '{geo_col}' not found in DataFrame.")

    df = df.copy()
    df['region'] = df[geo_col].map(mapping).fillna(default_region)
    n_unmapped = (df['region'] == default_region).sum()
    logger.info(f"Mapped geo column '{geo_col}' to regions. Unmapped entries: {n_unmapped}")
    return df


def popularity_by_region(
    df: pd.DataFrame,
    region_col: str,
    product_col: str = 'ASIN',
    metric: str = 'Qty'
) -> pd.DataFrame:
    """
    Compute popularity metric aggregated by region and product.

    Args:
        df: DataFrame with 'region' column and sales metrics.
        region_col: Column name for region.
        product_col: Column name for product identifier.
        metric: Numeric column name to aggregate (e.g., 'Qty' or 'Amount').

    Returns:
        DataFrame with columns [region_col, product_col, 'metric', 'popularity'],
        sorted by region and descending popularity.

    Raises:
        ValueError: If required columns missing or metric not numeric.
    """
    # Validate columns
    required = [region_col, product_col, metric]
    missing = [col for col in required if col not in df.columns]
    if missing:
        logger.error(f"Missing columns for popularity_by_region: {missing}")
        raise ValueError(f"Columns missing: {missing}")

    if not pd.api.types.is_numeric_dtype(df[metric]):
        logger.error(f"Metric column '{metric}' is not numeric.")
        raise ValueError(f"Metric column '{metric}' must be numeric.")

    # Group and aggregate
    agg_df = (
        df.groupby([region_col, product_col])[metric]
        .sum()
        .reset_index(name='popularity')
    )

    # Sort within each region
    agg_df = agg_df.sort_values([region_col, 'popularity'], ascending=[True, False])
    logger.info(f"Computed popularity by region for {agg_df[product_col].nunique()} products across {agg_df[region_col].nunique()} regions.")
    return agg_df


def save_region_popularity(
    df: pd.DataFrame,
    region_pop_df: pd.DataFrame,
    path: str
) -> None:
    """
    Save the regional popularity DataFrame to CSV.

    Args:
        df: Original DataFrame (unused, for CLI consistency).
        region_pop_df: DataFrame from popularity_by_region.
        path: Output file path.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    region_pop_df.to_csv(path, index=False)
    logger.info(f"Saved regional popularity to {path}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Map to regions and compute popularity by region.')
    parser.add_argument('input', help='Path to preprocessed CSV file')
    parser.add_argument('--geo_col', default='ship-country', help='Geographic column to map')
    parser.add_argument('--mapping_file', help='Path to CSV with two columns: geo_value, region_name')
    parser.add_argument('--metric', default='Qty', help='Metric column to aggregate')
    parser.add_argument('--product_col', default='ASIN', help='Product column to group by')
    parser.add_argument('--output', help='Path for saving regional popularity CSV')
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    # Load mapping
    mapping = {}
    if args.mapping_file:
        map_df = pd.read_csv(args.mapping_file)
        mapping = dict(zip(map_df.iloc[:,0], map_df.iloc[:,1]))
    regions = define_regions(mapping)
    df_mapped = map_to_region(df, args.geo_col, regions)
    region_pop = popularity_by_region(df_mapped, 'region', args.product_col, args.metric)
    if args.output:
        save_region_popularity(df, region_pop, args.output)
    else:
        print(region_pop.head())
    logger.info("Geographical analysis complete.")
