import os

# Project directory structure
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data paths
data_dir = os.path.join(BASE_DIR, 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
processed_data_dir = os.path.join(data_dir, 'processed')

RAW_DATA_PATH = os.path.join(raw_data_dir, 'AmazonSaleReport.csv')
CLEANED_DATA_PATH = os.path.join(processed_data_dir, 'cleaned.csv')
TOP_N_PRODUCTS_PATH = os.path.join(processed_data_dir, 'top_n_products.csv')
SUMMARY_STATS_PATH = os.path.join(processed_data_dir, 'summary_stats.csv')
LONG_TAIL_PATH = os.path.join(processed_data_dir, 'long_tail_analysis.csv')
REGION_POPULARITY_PATH = os.path.join(processed_data_dir, 'region_popularity.csv')

# Reports and outputs
reports_dir = os.path.join(BASE_DIR, 'reports')
plots_dir = os.path.join(reports_dir, 'plots')

# Trend plots output directory
TREND_PLOTS_DIR = os.path.join(plots_dir, 'trend')
POPULARITY_PLOTS_DIR = os.path.join(plots_dir, 'popularity')
GEOGRAPHY_PLOTS_DIR = os.path.join(plots_dir, 'geography')

# Region mapping configuration
# Provide a CSV file with columns: geo_value, region_name
config_dir = os.path.join(BASE_DIR, 'config')
REGION_MAPPING_FILE = os.path.join(config_dir, 'region_mapping.csv')

# Analysis parameters
TOP_N = 10  # default number of top products
LONG_TAIL_THRESHOLD = 0.8  # threshold for head in long-tail analysis

if __name__ == '__main__':
    print('Configuration paths:')
    for name, val in globals().items():
        if name.isupper() and isinstance(val, str):
            print(f'{name}: {val}')
