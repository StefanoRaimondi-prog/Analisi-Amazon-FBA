import pandas as pd
import logging
import os
from typing import Optional

# Configure logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def compute_popularity(
    df: pd.DataFrame,
    product_col: str = 'ASIN',
    metric: str = 'quantity'
) -> pd.DataFrame:
    """
    Compute popularity of products based on quantity sold or revenue.

    Args:
        df: Preprocessed DataFrame containing sales data.
        product_col: Column name to group products (e.g., 'ASIN' or 'SKU').
        metric: 'quantity' to sum 'Qty', 'revenue' to sum 'Amount'.

    Returns:
        DataFrame with columns [product_col, 'popularity'], sorted descending.

    Raises:
        ValueError: If product_col not in df or metric invalid or required column missing.
    """
    if product_col not in df.columns:
        logger.error(f"Product column '{product_col}' not found in DataFrame.")
        raise ValueError(f"Column '{product_col}' not found.")

    metric = metric.lower()
    if metric == 'quantity':
        qty_col = 'Qty'
        if qty_col not in df.columns:
            logger.error(f"Quantity column '{qty_col}' not found in DataFrame.")
            raise ValueError(f"Column '{qty_col}' not found for quantity metric.")
        agg_series = df.groupby(product_col)[qty_col].sum()
    elif metric == 'revenue':
        rev_col = 'Amount'
        if rev_col not in df.columns:
            logger.error(f"Revenue column '{rev_col}' not found in DataFrame.")
            raise ValueError(f"Column '{rev_col}' not found for revenue metric.")
        agg_series = df.groupby(product_col)[rev_col].sum()
    else:
        logger.error(f"Invalid metric '{metric}'. Choose 'quantity' or 'revenue'.")
        raise ValueError("Metric must be either 'quantity' or 'revenue'.")

    popularity_df = (
        agg_series
        .reset_index(name='popularity')
        .sort_values(by='popularity', ascending=False)
    )
    logger.info(f"Computed popularity by '{metric}' for {len(popularity_df)} products.")
    return popularity_df


def top_n_products(
    popularity_df: pd.DataFrame,
    n: int = 10,
    product_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Select the top-N products from a popularity DataFrame.

    Args:
        popularity_df: DataFrame returned by compute_popularity.
        n: Number of top products to return.
        product_col: Name of the product column if not inferred (first col).

    Returns:
        DataFrame containing the top-N rows.

    Raises:
        ValueError: If popularity column missing or n <= 0.
    """
    if 'popularity' not in popularity_df.columns:
        logger.error("Column 'popularity' not found in DataFrame.")
        raise ValueError("DataFrame must contain 'popularity' column.")
    if n <= 0:
        logger.error(f"Invalid n={n}. Must be > 0.")
        raise ValueError("n must be a positive integer.")

    top_df = popularity_df.head(n).copy()
    logger.info(f"Selected top {n} products.")
    return top_df


def save_top_n(
    df: pd.DataFrame,
    path: str,
    n: int = 10,
    product_col: Optional[str] = None
) -> None:
    """
    Compute and save the top-N products to CSV.

    Args:
        df: Preprocessed sales DataFrame.
        path: Output path for CSV.
        n: Number of top products to save.
        product_col: Column to group by.
    """
    pop_df = compute_popularity(df, product_col or 'ASIN', 'quantity')
    top_df = top_n_products(pop_df, n, product_col)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    top_df.to_csv(path, index=False)
    logger.info(f"Saved top {n} products to {path}.")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Compute and save top-N popular products.')
    parser.add_argument('input', help='Path to preprocessed CSV file')
    parser.add_argument('output', help='Path for saving top-N CSV')
    parser.add_argument('--product_col', default='ASIN', help="Column to group products by")
    parser.add_argument('--metric', default='quantity', choices=['quantity','revenue'],
                        help="How to measure popularity")
    parser.add_argument('--top_n', type=int, default=10, help="Number of top products to select")
    args = parser.parse_args()

    df_processed = pd.read_csv(args.input)
    pop = compute_popularity(df_processed, args.product_col, args.metric)
    top = top_n_products(pop, args.top_n, args.product_col)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    top.to_csv(args.output, index=False)
    logger.info("Popularity analysis complete.")
