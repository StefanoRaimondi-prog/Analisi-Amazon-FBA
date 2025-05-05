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
    Compute summary statistics (count, mean, median, std, min, 25%, 75%, max)
    for specified numeric columns grouped by a key.

    Args:
        df: Input DataFrame.
        groupby_col: Column name to group by.
        agg_cols: List of numeric columns to summarize.

    Returns:
        DataFrame with columns <groupby_col>, and for each col in agg_cols, col_count, col_mean, etc.

    Raises:
        ValueError: If grouping column or any agg column is missing.
    """
    # Validate input columns
    missing = [col for col in [groupby_col] + agg_cols if col not in df.columns]
    if missing:
        logger.error(f"Missing columns for summary_stats: {missing}")
        raise ValueError(f"Missing columns: {missing}")

    results = []
    # For each metric column, compute stats
    for col in agg_cols:
        group = df.groupby(groupby_col)[col]
        stats = pd.DataFrame({
            f"{col}_count": group.count(),
            f"{col}_mean": group.mean(),
            f"{col}_median": group.median(),
            f"{col}_std": group.std(),
            f"{col}_min": group.min(),
            f"{col}_25%": group.quantile(0.25),
            f"{col}_75%": group.quantile(0.75),
            f"{col}_max": group.max(),
        })
        results.append(stats)
        logger.info(f"Computed stats for column '{col}'")

    # Merge all stats on index (groupby_col)
    summary_df = pd.concat(results, axis=1).reset_index()
    logger.info(f"Summary statistics computed for {len(summary_df)} groups.")
    return summary_df


def detect_outliers(
    df: pd.DataFrame,
    col: str,
    method: str = 'iqr',
    factor: float = 1.5
) -> pd.Series:
    """
    Detect outliers in a numeric column using IQR method.
    """
    if col not in df.columns:
        logger.error(f"Column '{col}' not found for outlier detection.")
        raise ValueError(f"Column '{col}' not found.")
    if method.lower() != 'iqr':
        logger.error(f"Unsupported method: {method}")
        raise ValueError("Only 'iqr' method is supported.")

    series = df[col].dropna()
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    outliers = (df[col] < lower) | (df[col] > upper)
    logger.info(f"Detected {outliers.sum()} outliers in '{col}' using IQR.")
    return outliers


def long_tail_analysis(
    df: pd.DataFrame,
    groupby_col: str,
    metric_col: str,
    threshold: float = 0.8
) -> pd.DataFrame:
    """
    Analyze head vs long-tail of metric distribution across groups.
    """
    if groupby_col not in df.columns or metric_col not in df.columns:
        logger.error(f"Missing columns for long_tail: {groupby_col}, {metric_col}")
        raise ValueError("Required columns missing.")
    if not 0 < threshold < 1:
        raise ValueError("Threshold must be between 0 and 1.")

    agg = df.groupby(groupby_col)[metric_col].sum().reset_index(name='metric_sum')
    agg = agg.sort_values('metric_sum', ascending=False)
    total = agg['metric_sum'].sum()
    agg['cum_pct'] = agg['metric_sum'].cumsum() / total
    agg['segment'] = agg['cum_pct'].apply(lambda x: 'head' if x <= threshold else 'tail')
    logger.info(f"Long-tail: {sum(agg['segment']=='head')} head groups out of {len(agg)}.")
    return agg


def save_stats(
    df: pd.DataFrame,
    path: str,
    stats_df: pd.DataFrame
) -> None:
    """
    Save stats DataFrame to CSV.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    stats_df.to_csv(path, index=False)
    logger.info(f"Statistics saved to {path}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--groupby', required=True)
    parser.add_argument('--metrics', nargs='+', required=True)
    parser.add_argument('--output')
    parser.add_argument('--detect_outlier_col')
    parser.add_argument('--long_tail_col')
    parser.add_argument('--threshold', type=float, default=0.8)
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    summary = summary_stats(df, args.groupby, args.metrics)
    if args.output:
        save_stats(df, args.output, summary)
    if args.detect_outlier_col:
        print(detect_outliers(df, args.detect_outlier_col).sum())
    if args.long_tail_col:
        lt = long_tail_analysis(df, args.groupby, args.long_tail_col, args.threshold)
        lt_path = args.output.replace('.csv','_longtail.csv') if args.output else 'longtail.csv'
        save_stats(df, lt_path, lt)
    logger.info("Statistical analysis done.")
