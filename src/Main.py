#!/usr/bin/env python3
"""
Script principale che esegue l'intera pipeline in modo interattivo:
- Carica e pulisce i dati
- Stampa head dei dati puliti
- Calcola e stampa i top-N prodotti
- Calcola e stampa statistiche descrittive e long-tail
- Visualizza il trend mensile\ n- Esegue l'analisi geografica e stampa i risultati
- Mostra grafico heatmap di popolarità per le top 5 ASIN
"""
import os
import sys
import logging
import pandas as pd
import matplotlib.pyplot as plt

# Inserisce dinamicamente 'src/' nel PYTHONPATH
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Import dei moduli interni
import Config
import Preprocessing
import Popularity
import Statistic
import Trend
import Geography
import Visualization


def main():
    # Logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    logger = logging.getLogger(__name__)

    # 1. Preprocessing
    logger.info("Caricamento dati grezzi...")
    df_raw = Preprocessing.load_raw(Config.RAW_DATA_PATH)
    df = Preprocessing.drop_unused_columns(df_raw, ['Unnamed: 22'])
    df = Preprocessing.parse_dates(df, ['Date'])
    df = Preprocessing.handle_missing(df)
    df = Preprocessing.standardize_text(df, ['Status', 'Courier Status', 'Fulfilment'])

    print("\n=== Dati puliti (5 righe) ===")
    print(df.head())

    # 2. Top-N prodotti
    logger.info("Calcolo top-N prodotti...")
    pop_df = Popularity.compute_popularity(df, product_col='ASIN', metric='quantity')
    top_df = Popularity.top_n_products(pop_df, Config.TOP_N)

    print(f"\n=== Top {Config.TOP_N} prodotti per quantità ===")
    print(top_df)

    # 3. Statistiche descrittive
    logger.info("Calcolo statistiche descrittive...")
    stats_df = Statistic.summary_stats(df, groupby_col='ASIN', agg_cols=['Qty', 'Amount'])

    print("\n=== Prime 5 righe delle statistiche descrittive ===")
    print(stats_df.head())

    # 4. Long-tail
    logger.info("Analisi long-tail...")
    lt_df = Statistic.long_tail_analysis(df, groupby_col='ASIN', metric_col='Qty', threshold=Config.LONG_TAIL_THRESHOLD)

    print("\n=== Prime 5 righe dell'analisi long-tail ===")
    print(lt_df.head())

    # 5. Trend mensile
    logger.info("Visualizzazione trend mensile...")
    df_trend = Trend.aggregate_time(df, date_col='Date', freq='M', metrics=['Qty'], groupby_col=None)
    fig1 = Trend.plot_time_series(df_trend, date_col='Date', value_col='Qty', title='Andamento Mensile Qty')
    plt.show()

    # 6. Analisi geografica
    logger.info("Analisi geografica...")
    map_df = pd.read_csv(Config.REGION_MAPPING_FILE)
    regions = Geography.define_regions(dict(zip(map_df.iloc[:,0], map_df.iloc[:,1])))
    df_geo = Geography.map_to_region(df, geo_col='ship-country', mapping=regions)
    region_pop_df = Geography.popularity_by_region(df_geo, region_col='region', product_col='ASIN', metric='Qty')

    print("\n=== Prime 5 righe popolarità per regione ===")
    print(region_pop_df.head())

    # 7. Heatmap per top 5 ASIN
    logger.info("Heatmap popolarità top-5 ASIN per regione...")
    top5 = top_df['ASIN'].head(5).tolist()
    subset = region_pop_df[region_pop_df['ASIN'].isin(top5)]
    fig2 = Visualization.heatmap(subset, index='region', columns='ASIN', values='popularity', title='Popolarità Top-5 ASIN x Regione')
    plt.show()


if __name__ == '__main__':
    main()
