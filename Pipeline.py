#!/usr/bin/env python3
"""
Orchestrazione end-to-end della pipeline di analisi vendite Amazon
adattata alla struttura di progetto con moduli PascalCase.
"""
import os
import sys
import logging
import pandas as pd

# Aggiunge dinamicamente 'src/' al PYTHONPATH
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Import dei moduli interni (PascalCase)
import Config
import Preprocessing
import Popularity
import Statistic
import Trend
import Geography
import Visualization


def main():
    # Configura logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    # 1. Preprocessing
    logger.info("Caricamento dati grezzi...")
    df_raw = Preprocessing.load_raw(Config.RAW_DATA_PATH)
    df_clean = Preprocessing.drop_unused_columns(df_raw, ['Unnamed: 22'])
    df_clean = Preprocessing.parse_dates(df_clean, ['Date'])
    df_clean = Preprocessing.handle_missing(df_clean)
    df_clean = Preprocessing.standardize_text(df_clean, ['Status', 'Courier Status', 'Fulfilment'])
    Preprocessing.save_processed(df_clean, Config.CLEANED_DATA_PATH)
    logger.info(f"Dati puliti salvati in {Config.CLEANED_DATA_PATH}")

    # 2. Popolarità
    logger.info("Calcolo top-N prodotti...")
    pop_df = Popularity.compute_popularity(df_clean, product_col='ASIN', metric='quantity')
    top_df = Popularity.top_n_products(pop_df, Config.TOP_N)
    os.makedirs(os.path.dirname(Config.TOP_N_PRODUCTS_PATH), exist_ok=True)
    top_df.to_csv(Config.TOP_N_PRODUCTS_PATH, index=False)
    logger.info(f"Top {Config.TOP_N} prodotti salvati in {Config.TOP_N_PRODUCTS_PATH}")

    # 3. Statistiche descrittive
    logger.info("Calcolo statistiche descrittive...")
    stats_df = Statistic.summary_stats(df_clean, groupby_col='ASIN', agg_cols=['Qty', 'Amount'])
    os.makedirs(os.path.dirname(Config.SUMMARY_STATS_PATH), exist_ok=True)
    stats_df.to_csv(Config.SUMMARY_STATS_PATH, index=False)
    logger.info(f"Statistiche salvate in {Config.SUMMARY_STATS_PATH}")

    # 4. Long-tail analysis
    logger.info("Analisi long-tail...")
    lt_df = Statistic.long_tail_analysis(
        df_clean,
        groupby_col='ASIN',
        metric_col='Qty',
        threshold=Config.LONG_TAIL_THRESHOLD
    )
    os.makedirs(os.path.dirname(Config.LONG_TAIL_PATH), exist_ok=True)
    lt_df.to_csv(Config.LONG_TAIL_PATH, index=False)
    logger.info(f"Long-tail analysis salvata in {Config.LONG_TAIL_PATH}")

    # 5. Trend mensile
    logger.info("Analisi trend mensile...")
    df_trend = Trend.aggregate_time(
        df_clean,
        date_col='Date',
        freq='M',
        metrics=['Qty'],
        groupby_col=None
    )
    fig1 = Trend.plot_time_series(
        df_trend,
        date_col='Date',
        value_col='Qty',
        title='Andamento Mensile Qty'
    )
    os.makedirs(Config.TREND_PLOTS_DIR, exist_ok=True)
    Trend.save_plot(fig1, os.path.join(Config.TREND_PLOTS_DIR, 'monthly_qty_trend'))
    logger.info(f"Grafico trend salvato in {Config.TREND_PLOTS_DIR}")

    # 6. Analisi geografica
    logger.info("Analisi geografica...")
    map_df = pd.read_csv(Config.REGION_MAPPING_FILE)
    regions = Geography.define_regions(dict(zip(map_df.iloc[:, 0], map_df.iloc[:, 1])))
    df_geo = Geography.map_to_region(df_clean, geo_col='ship-country', mapping=regions)
    region_pop_df = Geography.popularity_by_region(
        df_geo,
        region_col='region',
        product_col='ASIN',
        metric='Qty'
    )
    os.makedirs(os.path.dirname(Config.REGION_POPULARITY_PATH), exist_ok=True)
    region_pop_df.to_csv(Config.REGION_POPULARITY_PATH, index=False)
    logger.info(f"Popolarità per regione salvata in {Config.REGION_POPULARITY_PATH}")

    # 7. Heatmap top-5 ASIN
    logger.info("Heatmap popolarità top-5 ASIN per regione...")
    top5_asin = top_df['ASIN'].head(Config.TOP_N).tolist()
    subset = region_pop_df[region_pop_df['ASIN'].isin(top5_asin)]
    fig2 = Visualization.heatmap(
        subset,
        index='region',
        columns='ASIN',
        values='popularity',
        title='Popolarità Top-5 ASIN per Regione'
    )
    Visualization.save_figure(fig2, os.path.join(Config.TREND_PLOTS_DIR, 'heatmap_top5_asin'))
    logger.info("Heatmap salvata")

    logger.info("Pipeline completata con successo.")


if __name__ == '__main__':
    main()