import pandas as pd
import matplotlib.pyplot as plt
import logging
import os
from typing import List, Optional

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def aggregate_time(
    df: pd.DataFrame,
    date_col: str,
    freq: str = 'M',
    metrics: Optional[List[str]] = None,
    groupby_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Aggregate sales data over time with optional grouping.

    Args:
        df: Preprocessed DataFrame with sales data.
        date_col: Name of datetime column.
        freq: Resampling frequency (e.g., 'D', 'W', 'M').
        metrics: List of numeric columns to aggregate (default all numeric).
        groupby_col: Optional column to further group by (e.g., 'ASIN').

    Returns:
        DataFrame with aggregated metrics. If groupby_col=None, columns: [date_col] + metrics.
        If groupby_col provided, columns: [date_col, groupby_col] + metrics.

    Raises:
        ValueError: If date_col or any metrics/groupby_col missing.
    """
    # Validate date column
    if date_col not in df.columns:
        logger.error(f"Date column '{date_col}' not found.")
        raise ValueError(f"Missing date column: {date_col}")

    # Set default metrics
    if metrics is None:
        metrics = df.select_dtypes(include='number').columns.tolist()
        logger.info(f"No metrics specified. Using numeric columns: {metrics}")
    else:
        missing_metrics = [c for c in metrics if c not in df.columns]
        if missing_metrics:
            logger.error(f"Metrics columns not found: {missing_metrics}")
            raise ValueError(f"Missing metrics: {missing_metrics}")

    # Check groupby column
    if groupby_col and groupby_col not in df.columns:
        logger.error(f"Group-by column '{groupby_col}' not found.")
        raise ValueError(f"Missing groupby column: {groupby_col}")

    # Ensure datetime dtype
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        logger.info(f"Converted '{date_col}' to datetime.")

    # Drop rows with missing date
    missing_dates = df[date_col].isna().sum()
    if missing_dates > 0:
        df = df.dropna(subset=[date_col])
        logger.warning(f"Dropped {missing_dates} rows with invalid dates.")

    # Perform aggregation
    if groupby_col:
        grouped = (
            df
            .groupby([pd.Grouper(key=date_col, freq=freq), groupby_col])[metrics]
            .sum()
            .reset_index()
        )
    else:
        grouped = (
            df
            .groupby(pd.Grouper(key=date_col, freq=freq))[metrics]
            .sum()
            .reset_index()
        )
    logger.info(f"Aggregated data by freq='{freq}'{' and group=' + groupby_col if groupby_col else ''}.")
    return grouped


def plot_time_series(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    group_col: Optional[str] = None,
    top_n: Optional[int] = None,
    title: Optional[str] = None
) -> plt.Figure:
    """
    Plot time series of a metric, optionally for top N groups.

    Args:
        df: Aggregated DataFrame from aggregate_time, with columns [date_col, (group_col?), value_col].
        date_col: Name of date column.
        value_col: Name of metric to plot.
        group_col: Optional column for grouping (e.g., 'ASIN').
        top_n: If group_col provided, number of top groups to plot based on total value.
        title: Optional chart title.

    Returns:
        Matplotlib Figure object.

    Raises:
        ValueError: If required columns missing or invalid top_n.
    """
    # Validate columns
    for col in [date_col, value_col] + ([group_col] if group_col else []):
        if col and col not in df.columns:
            logger.error(f"Column '{col}' not found in DataFrame for plotting.")
            raise ValueError(f"Missing column: {col}")

    # Prepare plot
    fig, ax = plt.subplots()

    if group_col:
        # Determine groups to plot
        grouped_totals = df.groupby(group_col)[value_col].sum().nlargest(top_n or 10)
        groups_to_plot = grouped_totals.index.tolist()
        logger.info(f"Plotting top groups: {groups_to_plot}")

        for grp in groups_to_plot:
            subset = df[df[group_col] == grp]
            ax.plot(subset[date_col], subset[value_col], label=str(grp))
        ax.legend(title=group_col)
    else:
        ax.plot(df[date_col], df[value_col], label=value_col)

    # Formatting
    ax.set_xlabel('Date')
    ax.set_ylabel(value_col)
    if title:
        ax.set_title(title)
    ax.grid(True)

    fig.autofmt_xdate()
    plt.tight_layout()
    logger.info("Time series plot generated.")
    return fig


def save_plot(
    fig: plt.Figure,
    path: str,
    fmt: str = 'png'
) -> None:
    """
    Save a Matplotlib figure to a file.

    Args:
        fig: Figure object to save.
        path: Output file path (without extension).
        fmt: File format (e.g., 'png', 'pdf').
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    full_path = f"{path}.{fmt}"
    fig.savefig(full_path)
    logger.info(f"Saved plot to {full_path}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Aggregate and plot sales trends over time.')
    parser.add_argument('input', help='Path to preprocessed CSV file')
    parser.add_argument('--date_col', default='Date', help='Date column name')
    parser.add_argument('--freq', default='M', help="Resampling frequency (e.g., 'D','W','M')")
    parser.add_argument('--metrics', nargs='+', default=['Qty'], help='Metric columns to aggregate')
    parser.add_argument('--groupby', help='Optional column to group by (e.g., ASIN)')
    parser.add_argument('--plot_metric', default='Qty', help='Which metric to plot')
    parser.add_argument('--top_n', type=int, default=10, help='Top N groups to plot')
    parser.add_argument('--output', help='Path prefix for saving plot')
    args = parser.parse_args()

    df = pd.read_csv(args.input, parse_dates=[args.date_col])
    df_agg = aggregate_time(df, args.date_col, args.freq, args.metrics, args.groupby)
    if args.output:
        for metric in args.metrics:
            fig = plot_time_series(
                df_agg, args.date_col, metric,
                group_col=args.groupby, top_n=args.top_n,
                title=f"Trend of {metric}"
            )
            save_plot(fig, f"{args.output}_{metric}")
    else:
        # Just show one plot
        fig = plot_time_series(
            df_agg, args.date_col, args.plot_metric,
            group_col=args.groupby, top_n=args.top_n
        )
        plt.show()
