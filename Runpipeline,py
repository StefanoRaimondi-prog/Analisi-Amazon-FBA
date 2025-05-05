#!/usr/bin/env python3
"""
Orchestrazione end-to-end della pipeline di analisi vendite Amazon.
"""
import os
import logging
import pandas as pd

from src.config import (
    RAW_DATA_PATH,
    CLEANED_DATA_PATH,
    TOP_N_PRODUCTS_PATH,
    SUMMARY_STATS_PATH,
    LONG_TAIL_PATH,
    REGION_POPULARITY_PATH,
    TREND_PLOTS_DIR,
    REGION_MAPPING_FILE,
    TOP_N,
    LONG_TAIL_THRESHOLD,
)
from src.data_preprocessing import (
    load_raw,
    drop_unused_columns,
    parse_dates,
    handle_missing,
    standardize_text,
    save_processed,
)
from src.popularity import compute_popularity, top_n_products
from src.statistics import summary_stats, long_tail_analysis
from src.trend import aggregate_time, plot_time_series, save_plot
from src.geography import define_regions, map_to_region, popularity_by_region


def main():
    # Logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    # 1. Preprocessing
    df_raw = load_raw(RAW_DATA_PATH)
    df_clean = drop_unused_columns(df_raw, ['Unnamed: 22'])
    df_clean = parse_dates(df_clean, ['Date'])
    df_clean = handle_missing(df_clean)
    df_clean = standardize_text(df_clean, ['Status', 'Courier Status', 'Fulfilment'])
    save_processed(df_clean, CLEANED_DATA_PATH)

    # 2. Popularity
    pop_df = compute_popularity(df_clean, product_col='ASIN', metric='quantity')
    top_df = top_n_products(pop_df, TOP_N)
    os.makedirs(os.path.dirname(TOP_N_PRODUCTS_PATH), exist_ok=True)
    top_df.to_csv(TOP_N_PRODUCTS_PATH, index=False)
    logger.info(f"Top {TOP_N} products saved to {TOP_N_PRODUCTS_PATH}")

    # 3. Statistical Analysis
    stats_df = summary_stats(df_clean, groupby_col='ASIN', agg_cols=['Qty', 'Amount'])
    os.makedirs(os.path.dirname(SUMMARY_STATS_PATH), exist_ok=True)
    stats_df.to_csv(SUMMARY_STATS_PATH, index=False)
    logger.info(f"Summary statistics saved to {SUMMARY_STATS_PATH}")

    lt_df = long_tail_analysis(
        df_clean,
        groupby_col='ASIN',
        metric_col='Qty',
        threshold=LONG_TAIL_THRESHOLD,
    )
    os.makedirs(os.path.dirname(LONG_TAIL_PATH), exist_ok=True)
    lt_df.to_csv(LONG_TAIL_PATH, index=False)
    logger.info(f"Long-tail analysis saved to {LONG_TAIL_PATH}")

    # 4. Trend Analysis (Monthly Quantity)
    df_trend = aggregate_time(
        df_clean,
        date_col='Date',
        freq='M',
        metrics=['Qty'],
        groupby_col=None,
    )
    fig = plot_time_series(
        df_trend,
        date_col='Date',
        value_col='Qty',
        title='Monthly Sales Quantity'
    )
    os.makedirs(TREND_PLOTS_DIR, exist_ok=True)
    save_plot(fig, os.path.join(TREND_PLOTS_DIR, 'monthly_qty_trend'))

    # 5. Geographical Analysis
    # Load region mapping CSV (geo_value, region_name)
    map_df = pd.read_csv(REGION_MAPPING_FILE)
    mapping = define_regions(dict(zip(map_df.iloc[:, 0], map_df.iloc[:, 1])))
    df_geo = map_to_region(df_clean, geo_col='ship-country', mapping=mapping)
    region_pop_df = popularity_by_region(
        df_geo,
        region_col='region',
        product_col='ASIN',
        metric='Qty',
    )
    os.makedirs(os.path.dirname(REGION_POPULARITY_PATH), exist_ok=True)
    region_pop_df.to_csv(REGION_POPULARITY_PATH, index=False)
    logger.info(f"Regional popularity saved to {REGION_POPULARITY_PATH}")

    logger.info("Pipeline completata con successo.")


if __name__ == '__main__':
    main()
