import os

# Struttura della directory del progetto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Percorsi dei dati
data_dir = os.path.join(BASE_DIR, 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
processed_data_dir = os.path.join(data_dir, 'processed')

RAW_DATA_PATH = os.path.join(raw_data_dir, 'AmazonSaleReport.csv')
CLEANED_DATA_PATH = os.path.join(processed_data_dir, 'cleaned.csv')
TOP_N_PRODUCTS_PATH = os.path.join(processed_data_dir, 'top_n_products.csv')
SUMMARY_STATS_PATH = os.path.join(processed_data_dir, 'summary_stats.csv')
LONG_TAIL_PATH = os.path.join(processed_data_dir, 'long_tail_analysis.csv')
REGION_POPULARITY_PATH = os.path.join(processed_data_dir, 'region_popularity.csv')

# Report e output
reports_dir = os.path.join(BASE_DIR, 'reports')
plots_dir = os.path.join(reports_dir, 'plots')

# Directory di output per i grafici delle tendenze
TREND_PLOTS_DIR = os.path.join(plots_dir, 'trend')
POPULARITY_PLOTS_DIR = os.path.join(plots_dir, 'popularity')
GEOGRAPHY_PLOTS_DIR = os.path.join(plots_dir, 'geography')

# Configurazione della mappatura delle regioni
# Fornire un file CSV con colonne: geo_value, region_name
config_dir = os.path.join(BASE_DIR, 'config')
REGION_MAPPING_FILE = os.path.join(config_dir, 'region_mapping.csv')

# Parametri di analisi
TOP_N = 10  # numero predefinito di prodotti principali
LONG_TAIL_THRESHOLD = 0.8  # soglia per la testa nell'analisi long-tail

if __name__ == '__main__':
    print('Percorsi di configurazione:')
    for name, val in globals().items():
        if name.isupper() and isinstance(val, str):
            print(f'{name}: {val}')
