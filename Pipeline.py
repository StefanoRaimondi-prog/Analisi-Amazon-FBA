#!/usr/bin/env python3
"""
Script interattivo per gestire le varie fasi della pipeline di analisi vendite Amazon.
"""
import os
import sys
import logging
import pandas as pd
import matplotlib.pyplot as plt

# Inserisce 'src/' nel sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Import moduli interni
import Config
import Preprocessing
import Popularity
import Statistic
import Trend
import Geography
import Visualization

# Configura logging di base
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def menu():
    print("\n=== Amazon Sales Analysis Menu ===")
    print("1) Preprocessing dati")
    print("2) Calcolo top-N prodotti")
    print("3) Statistiche descrittive")
    print("4) Analisi long-tail")
    print("5) Trend temporale")
    print("6) Analisi geografica")
    print("7) Esegui pipeline completa")
    print("0) Esci")


def run_preprocessing():
    df_raw = Preprocessing.load_raw(Config.RAW_DATA_PATH)
    df = Preprocessing.drop_unused_columns(df_raw, ['Unnamed: 22'])
    df = Preprocessing.parse_dates(df, ['Date'])
    df = Preprocessing.handle_missing(df)
    df = Preprocessing.standardize_text(df, ['Status', 'Courier Status', 'Fulfilment'])
    Preprocessing.save_processed(df, Config.CLEANED_DATA_PATH)
    print(f"Dati preprocessati salvati in {Config.CLEANED_DATA_PATH}")
    return df


def run_popularity(df=None):
    if df is None:
        df = pd.read_csv(Config.CLEANED_DATA_PATH, parse_dates=['Date'])
    pop_df = Popularity.compute_popularity(df, product_col='ASIN', metric='quantity')
    top_df = Popularity.top_n_products(pop_df, Config.TOP_N)
    top_df.to_csv(Config.TOP_N_PRODUCTS_PATH, index=False)
    print(f"Top {Config.TOP_N} prodotti salvati in {Config.TOP_N_PRODUCTS_PATH}")
    print(top_df)
    return top_df


def run_statistics(df=None):
    if df is None:
        df = pd.read_csv(Config.CLEANED_DATA_PATH, parse_dates=['Date'])
    stats_df = Statistic.summary_stats(df, groupby_col='ASIN', agg_cols=['Qty', 'Amount'])
    stats_df.to_csv(Config.SUMMARY_STATS_PATH, index=False)
    print(f"Statistiche salvate in {Config.SUMMARY_STATS_PATH}")
    print(stats_df.head())
    return stats_df


def run_long_tail(df=None):
    if df is None:
        df = pd.read_csv(Config.CLEANED_DATA_PATH, parse_dates=['Date'])
    lt_df = Statistic.long_tail_analysis(df, groupby_col='ASIN', metric_col='Qty', threshold=Config.LONG_TAIL_THRESHOLD)
    lt_df.to_csv(Config.LONG_TAIL_PATH, index=False)
    print(f"Long-tail analysis salvata in {Config.LONG_TAIL_PATH}")
    print(lt_df.head())
    return lt_df


def run_trend(df=None):
    if df is None:
        df = pd.read_csv(Config.CLEANED_DATA_PATH, parse_dates=['Date'])
    df_trend = Trend.aggregate_time(df, date_col='Date', freq='M', metrics=['Qty'], groupby_col=None)
    fig = Trend.plot_time_series(df_trend, date_col='Date', value_col='Qty', title='Andamento Mensile Qty')
    os.makedirs(Config.TREND_PLOTS_DIR, exist_ok=True)
    Trend.save_plot(fig, os.path.join(Config.TREND_PLOTS_DIR, 'monthly_qty_trend'))
    print(f"Grafico trend salvato in {Config.TREND_PLOTS_DIR}")
    plt.show()
    return df_trend


def run_geography(df=None):
    if df is None:
        df = pd.read_csv(Config.CLEANED_DATA_PATH, parse_dates=['Date'])
    map_df = pd.read_csv(Config.REGION_MAPPING_FILE)
    mapping = Geography.define_regions(dict(zip(map_df.iloc[:,0], map_df.iloc[:,1])))
    df_geo = Geography.map_to_region(df, geo_col='ship-country', mapping=mapping)
    region_pop_df = Geography.popularity_by_region(df_geo, region_col='region', product_col='ASIN', metric='Qty')
    region_pop_df.to_csv(Config.REGION_POPULARITY_PATH, index=False)
    print(f"Popolarità per regione salvata in {Config.REGION_POPULARITY_PATH}")
    print(region_pop_df.head())
    # Heatmap
    top_df = pd.read_csv(Config.TOP_N_PRODUCTS_PATH)
    top5 = top_df['ASIN'].head(Config.TOP_N).tolist()
    subset = region_pop_df[region_pop_df['ASIN'].isin(top5)]
    fig = Visualization.heatmap(subset, index='region', columns='ASIN', values='popularity', title='Popolarità Top-5 ASIN per Regione')
    Visualization.save_figure(fig, os.path.join(Config.TREND_PLOTS_DIR, 'heatmap_top5_asin'))
    print(f"Heatmap salvata in {Config.TREND_PLOTS_DIR}")
    plt.show()
    return region_pop_df


if __name__ == '__main__':
    df = None
    top = None
    stats = None
    lt = None
    trend_df = None
    geo = None
    while True:
        menu()
        choice = input("Scegli un'opzione: ").strip()
        if choice == '1':
            df = run_preprocessing()
        elif choice == '2':
            top = run_popularity(df)
        elif choice == '3':
            stats = run_statistics(df)
        elif choice == '4':
            lt = run_long_tail(df)
        elif choice == '5':
            trend_df = run_trend(df)
        elif choice == '6':
            geo = run_geography(df)
        elif choice == '7':
            df = run_preprocessing()
            top = run_popularity(df)
            stats = run_statistics(df)
            lt = run_long_tail(df)
            trend_df = run_trend(df)
            geo = run_geography(df)
        elif choice == '0':
            print("Esco...")
            break
        else:
            print("Opzione non valida, riprova.")
