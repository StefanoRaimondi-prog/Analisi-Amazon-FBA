import pandas as pd
import logging
import os
from typing import List, Tuple, Union

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def summary_stats(
    df: pd.DataFrame,
    groupby_col: str,
    agg_cols: List[str]
) -> pd.DataFrame:
    """
    Compute summary statistics for specified numeric columns grouped by a key.

    Args:
        df: Input DataFrame.
        groupby_col: Column name to group by.
        agg_cols: List of numeric columns to summarize.

    Returns:
        DataFrame with multi-index columns containing count, mean, median, std, min, 25%, 75%, max.

    Raises:
        ValueError: If grouping column or any agg column is missing.
    """
    # Validate columns
    missing_cols = [c for c in [groupby_col] + agg_cols if c not in df.columns]
    if missing_cols:
        logger.error(f"Columns not found for summary_stats: {missing_cols}")
        raise ValueError(f"Missing columns: {missing_cols}")

    # Aggregate functions
    agg_funcs = [
        ('count', 'count'),
        ('mean', 'mean'),
        ('median', 'median'),
        ('std', 'std'),
        ('min', 'min'),
        ('25%', lambda x: x.quantile(0.25)),
        ('75%', lambda x: x.quantile(0.75)),
        ('max', 'max')
    ]

    # Perform groupby and aggregation
    grouped = df.groupby(groupby_col)[agg_cols].agg({name: func for name, func in agg_funcs})

    # Flatten MultiIndex columns
    grouped.columns = [f"{col}_{stat}" for col, stat in grouped.columns]
    logger.info(f"Computed summary statistics for {len(grouped)} groups on columns {agg_cols}.")
    return grouped.reset_index()


def detect_outliers(
    df: pd.DataFrame,
    col: str,
    method: str = 'iqr',
    factor: float = 1.5
) -> pd.Series:
    """
    Detect outliers in a numeric column using the specified method.

    Args:
        df: Input DataFrame.
        col: Column name on which to detect outliers.
        method: Outlier detection method. Currently only 'iqr' supported.
        factor: Multiplier for the IQR to define fences (default 1.5).

    Returns:
        Boolean Series where True indicates an outlier.

    Raises:
        ValueError: If column missing or method unsupported.
    """
    if col not in df.columns:
        logger.error(f"Column '{col}' not found for outlier detection.")
        raise ValueError(f"Column '{col}' not found.")
    if method.lower() != 'iqr':
        logger.error(f"Unsupported outlier detection method: {method}")
        raise ValueError("Currently only 'iqr' method is supported.")

    series = df[col].dropna()
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower_fence = q1 - factor * iqr
    upper_fence = q3 + factor * iqr

    outliers = (df[col] < lower_fence) | (df[col] > upper_fence)
    n_outliers = outliers.sum()
    logger.info(f"Detected {n_outliers} outliers in '{col}' using IQR method.")
    return outliers


def long_tail_analysis(
    df: pd.DataFrame,
    groupby_col: str,
    metric_col: str,
    threshold: float = 0.8
) -> pd.DataFrame:
    """
    Analyze head vs. long-tail distribution of a metric across groups.

    Args:
        df: Input DataFrame.
        groupby_col: Column to group (e.g., product ID).
        metric_col: Numeric column to aggregate (e.g., 'Qty' or 'Amount').
        threshold: Cumulative proportion threshold for head (default 0.8).

    Returns:
        DataFrame with columns [groupby_col, metric_sum, cum_pct, segment]
        where segment is 'head' if cum_pct <= threshold else 'tail'.

    Raises:
        ValueError: If columns missing or invalid threshold.
    """
    # Validate inputs
    missing = [c for c in [groupby_col, metric_col] if c not in df.columns]
    if missing:
        logger.error(f"Columns not found for long_tail_analysis: {missing}")
        raise ValueError(f"Missing columns: {missing}")
    if not 0 < threshold < 1:
        logger.error(f"Invalid threshold: {threshold}. Must be between 0 and 1.")
        raise ValueError("Threshold must be between 0 and 1.")

    # Aggregate metric
    agg = (
        df.groupby(groupby_col)[metric_col]
        .sum()
        .reset_index(name='metric_sum')
        .sort_values(by='metric_sum', ascending=False)
    )
    total = agg['metric_sum'].sum()

    # Cumulative percentage
    agg['cum_pct'] = agg['metric_sum'].cumsum() / total
    agg['segment'] = agg['cum_pct'].apply(lambda x: 'head' if x <= threshold else 'tail')

    n_head = (agg['segment'] == 'head').sum()
    logger.info(f"Long-tail analysis: {n_head} groups in head ({threshold*100}%).")
    return agg


def save_stats(
    df: pd.DataFrame,
    path: str,
    stats_df: pd.DataFrame
) -> None:
    """
    Save a statistics DataFrame to CSV.

    Args:
        df: Original DataFrame (unused, for CLI consistency).
        path: Output file path for CSV.
        stats_df: DataFrame with statistics to save.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    stats_df.to_csv(path, index=False)
    logger.info(f"Saved statistics DataFrame to {path}.")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Compute statistical summaries and outlier detection.')
    parser.add_argument('input', help='Path to preprocessed CSV file')
    parser.add_argument('--groupby', required=True, help='Column to group by')
    parser.add_argument('--metrics', nargs='+', required=True, help='Numeric columns to summarize')
    parser.add_argument('--output', help='Path for saving summary CSV')
    parser.add_argument('--detect_outlier_col', help='Column to detect outliers')
    parser.add_argument('--long_tail_col', help='Metric column for long-tail analysis')
    parser.add_argument('--threshold', type=float, default=0.8, help='Threshold for long-tail head')
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    summary_df = summary_stats(df, args.groupby, args.metrics)
    if args.output:
        save_stats(df, args.output, summary_df)
    if args.detect_outlier_col:
        outliers = detect_outliers(df, args.detect_outlier_col)
        print(f"Outliers detected: {outliers.sum()}")
    if args.long_tail_col:
        lt_df = long_tail_analysis(df, args.groupby, args.long_tail_col, args.threshold)
        lt_path = args.output.replace('.csv', '_longtail.csv') if args.output else 'longtail.csv'
        save_stats(df, lt_path, lt_df)
    logger.info("Statistical analysis complete.")
