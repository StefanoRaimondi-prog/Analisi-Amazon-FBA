import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# ---------------- PULIZIA DEL DATASET ----------------
def clean_amazon_data(df):
    fill_with_mode = [
        'Courier Status', 'currency', 'Amount', 'ship-city',
        'ship-state', 'ship-postal-code',
        'promotion-ids'
    ]
    
    for col in fill_with_mode:
        if col in df.columns:
            mode_val = df[col].mode()[0]
            df[col] = df[col].fillna(mode_val)

    columns_to_drop = ['B2B', 'Unnamed: 22', 'fulfilled-by','ship-country']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    print("\n>> VALORI NULLI DOPO LA PULIZIA:")
    print(df.isnull().sum())

    return df

# ---------------- RANGE DELLA COLONNA DATA ----------------
def show_date_range(df, date_column):
    try:
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        print(f"Min {date_column}: {df[date_column].min()}")
        print(f"Max {date_column}: {df[date_column].max()}")
    except Exception as e:
        print(f"Errore durante l'elaborazione della colonna '{date_column}': {e}")

# ---------------- RAGGRUPPAMENTO STATUS ----------------
def raggruppa_status(status):
    if status in ['Shipped', 'Shipped - Delivered to Buyer']:
        return 'Completati'
    elif status in ['Cancelled', 'Shipped - Rejected by Buyer', 'Shipping']:
        return 'Annullati'
    elif status in ['Shipped - Picked Up', 'Shipped - Out for Delivery', 'Pending', 'Pending - Waiting for Pick Up']:
        return 'In transito'
    elif status in ['Shipped - Returned to Seller', 'Shipped - Returning to Seller']:
        return 'Resi'
    elif status in ['Shipped - Lost in Transit', 'Shipped - Damaged']:
        return 'Eccezioni'
    else:
        return 'Altro'

# ---------------- RAGGRUPPAMENTO CATEGORIA ----------------
def raggruppa_categoria(cat):
    if cat in ['Set', 'kurta']:
        return 'Kurta / Set'
    elif cat in ['Western Dress', 'Top']:
        return 'Western Wear'
    elif cat in ['Saree', 'Dupatta', 'Ethnic Dress', 'Blouse', 'Bottom']:
        return 'Traditional Wear'
    else:
        return 'Altro'
# ---------------- RAGGRUPPAMENTO ship state  ----------------
def pulisci_e_raggruppa_ship_state(df):
    # Normalizzazione iniziale
    df['ship-state'] = df['ship-state'].astype(str).str.strip().str.lower()

    # Correzione manuale di errori e abbreviazioni
    correzioni = {
        'rajsthan': 'rajasthan',
        'rajshthan': 'rajasthan',
        'orissa': 'odisha',
        'pondicherry': 'puducherry',
        'new delhi': 'delhi',
        'delhi': 'delhi',
        'punjab/mohali/zirakpur': 'punjab',
        'goa ': 'goa',
        'nl': 'nagaland',
        'apo': 'unknown',
        'pb': 'punjab',
        'rj': 'rajasthan',
        'ar': 'arunachal pradesh',
        'bihar ': 'bihar',
        'sikkim ': 'sikkim',
        'mizoram ': 'mizoram',
        'manipur ': 'manipur',
        'nagaland ': 'nagaland',
        'arunachal pradesh ': 'arunachal pradesh',
        'dadra and nagar': 'dadra and nagar haveli',
        'puducherry': 'puducherry',
    }

    df['ship-state'] = df['ship-state'].replace(correzioni)
    df['ship-state'] = df['ship-state'].str.title()

    # Mappa delle regioni geografiche
    region_map = {
        # Nord
        'Delhi': 'North', 'Punjab': 'North', 'Haryana': 'North', 'Uttarakhand': 'North',
        'Himachal Pradesh': 'North', 'Jammu & Kashmir': 'North', 'Chandigarh': 'North', 'Ladakh': 'North',

        # Sud
        'Karnataka': 'South', 'Tamil Nadu': 'South', 'Kerala': 'South',
        'Andhra Pradesh': 'South', 'Telangana': 'South', 'Puducherry': 'South',

        # Ovest
        'Maharashtra': 'West', 'Gujarat': 'West', 'Goa': 'West', 'Rajasthan': 'West', 'Dadra And Nagar Haveli': 'West',

        # Est
        'West Bengal': 'East', 'Odisha': 'East', 'Bihar': 'East', 'Jharkhand': 'East',

        # Centro
        'Madhya Pradesh': 'Central', 'Chhattisgarh': 'Central',

        # Nord-Est
        'Assam': 'North-East', 'Manipur': 'North-East', 'Meghalaya': 'North-East',
        'Nagaland': 'North-East', 'Mizoram': 'North-East', 'Tripura': 'North-East',
        'Sikkim': 'North-East', 'Arunachal Pradesh': 'North-East',

        # Isole
        'Andaman & Nicobar': 'Islands', 'Lakshadweep': 'Islands',

        # Sconosciuti
        'Unknown': 'Unknown'
    }

    # Crea la nuova colonna con la regione geografica
    df['ship-region'] = df['ship-state'].map(region_map).fillna('Unknown')

    return df


# ---------------- MAIN ----------------
df = pd.read_csv("Amazon Sale Report.csv", low_memory=False)
df = clean_amazon_data(df)
df = pulisci_e_raggruppa_ship_state(df)


# Mostra il range della colonna 'Date'
show_date_range(df, 'Date')

# ✅ Crea la nuova colonna con il raggruppamento di Status
df['Status_Gruppo'] = df['Status'].apply(raggruppa_status)
df = df.drop(columns='Status')

# ✅ Crea la nuova colonna con il raggruppamento di Category
df['Category_Macro'] = df['Category'].apply(raggruppa_categoria)
df = df.drop(columns='Category')
