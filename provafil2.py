import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import missingno as msno

# === Funzione per pulizia dei dati ===
def clean_data(amazon):
    print("=== Inizio pulizia dati ===")
    print("--------------------------------------------------------------------------")
    
    # Rimozione duplicati
    before = len(amazon)
    amazon.drop_duplicates(inplace=True)
    after = len(amazon)
    print(f"{before - after} righe duplicate rimosse. Totale righe: {after}")
    print("--------------------------------------------------------------------------")

    # Controllo e gestione dei valori mancanti
    missing_total = amazon.isnull().sum().sum()
    print(f"Totale valori mancanti: {missing_total}")
    if missing_total > 0:
        print("Esempio righe con valori mancanti:\n")
        print(amazon[amazon.isnull().any(axis=1)].head())
    print("--------------------------------------------------------------------------")
    
    amazon['promotion-ids'] = amazon['promotion-ids'].fillna('no promotion')
    amazon['Courier Status'] = amazon['Courier Status'].fillna('unknown')
    amazon['ship-city'] = amazon['ship-city'].fillna('unknown')
    amazon['ship-state'] = amazon['ship-state'].fillna('unknown')
    amazon['ship-postal-code'] = amazon['ship-postal-code'].fillna('unknown')
    amazon['B2B'] = amazon['B2B'].replace([True, False], ['business', 'consumer'])

    
    # Rinomina colonne
    mapper = {
        'Order ID':'orderID', 'Date':'date', 'Status':'shipStatus',
        'Fullfilment':'fullfilment', 'ship-service-level':'serviceLevel',
        'Style':'style', 'SKU':'sku', 'Category':'productCategory',
        'Size':'size', 'ASIN':'asin', 'Courier Status':'courierShipStatus',
        'Qty':'orderQuantity', 'Amount':'orderAmount (INR)', 'ship-city':'city',
        'ship-state':'state', 'ship-postal-code':'zip', 'promotion-ids':'promotion',
        'B2B':'customerType'
    }
    amazon.rename(columns=mapper, inplace=True)
    print("Colonne rinominate con successo:")
    print(amazon.columns.tolist())
    print("--------------------------------------------------------------------------")
    print("Pulizia completata.")
    print("--------------------------------------------------------------------------")
    return amazon

# === Visualizzazione dati mancanti ===
def visualize_missing_data(amazon):
    msno.matrix(amazon)
    plt.show()
    msno.bar(amazon)
    plt.show()

# === Visualizzazione valori unici ===
def visualize_unique_value(amazon):
    print("Valori unici per colonna:")
    print(amazon.nunique())
    print("--------------------------------------------------------------------------")
    print("Esempio di valori unici per colonna:")
    print(amazon.apply(pd.unique))
    print("--------------------------------------------------------------------------")

# === Funzione per convertire date e creare colonna 'month' ===
def process_dates(amazon):
    amazon['date'] = pd.to_datetime(amazon['date'], format='%m-%d-%y', errors='coerce')
    amazon['month'] = amazon['date'].dt.month
    amazon['month'] = amazon['month'].replace([3, 4, 5, 6], ['March', 'April', 'May', 'June'])

    print(f"Intervallo date: {amazon['date'].min().date()} - {amazon['date'].max().date()}")
    print("--------------------------------------------------------------------------")
    return amazon

# === MAIN ===
def main():
    # Caricamento dati
    amazon = pd.read_csv("C:/Users/filip/Documents/Analisi-Amazon-FBA/Amazon Sale Report.csv", low_memory=False).iloc[:, :-1]  # rimuove l’ultima colonna

    amazon.set_index('index', inplace=True)

    print("Righe con dati mancanti prima della pulizia:")
    print(amazon[amazon.isnull().any(axis=1)].head())
    print("--------------------------------------------------------------------------")

    # Pulizia dati
    amazon = clean_data(amazon)

    # Visualizzazione dati mancanti dopo pulizia
    visualize_missing_data(amazon)

    # Info dataset
    amazon.info()
    print("--------------------------------------------------------------------------")

    # Gestione date
    amazon = process_dates(amazon)

    # Imposta index su orderID
    amazon.set_index('orderID', inplace=True)

    return amazon

# Avvia il flusso principale
amazon = main()

# Verifica i tipi di dati prima della modifica
print(amazon.dtypes)

# Gestisci i tipi misti in 'currency', 'zip', 'ship-country', 'fulfilled-by'
amazon['currency'] = amazon['currency'].fillna('unknown').astype(str)
amazon['zip'] = amazon['zip'].fillna('unknown').astype(str)
amazon['ship-country'] = amazon['ship-country'].fillna('unknown').astype(str)
amazon['fulfilled-by'] = amazon['fulfilled-by'].fillna('unknown').astype(str)

# Verifica i tipi di dati dopo la modifica
print(amazon.dtypes)

