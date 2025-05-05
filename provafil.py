import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import missingno as msno

# === Funzione per pulizia dei dati ===
def clean_data(amazon):
    print("=== Inizio pulizia dati ===")
    print("--------------------------------------------------------------------------")
    
    # Eliminazione colonne inutili
    amazon.drop(columns=['Unnamed: 22', 'fulfilled-by', 'ship-country', 'currency'], inplace=True)
    
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
    
    amazon['promotion-ids'].fillna('no promotion', inplace=True)
    amazon['Courier Status'].fillna('unknown', inplace=True)
    amazon['Amount'].fillna(0, inplace=True)
    amazon['ship-city'].fillna('unknown', inplace=True)
    amazon['ship-state'].fillna('unknown', inplace=True)
    amazon['ship-postal-code'].fillna('unknown', inplace=True)
    
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
    
    amazon['customerType'].replace([True, False], ['business', 'consumer'], inplace=True)

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
    amazon['date'] = pd.to_datetime(amazon['date'])
    amazon['month'] = amazon['date'].dt.month
    amazon['month'].replace([3, 4, 5, 6], ['March', 'April', 'May', 'June'], inplace=True)
    print(f"Intervallo date: {amazon['date'].min().date()} - {amazon['date'].max().date()}")
    print("--------------------------------------------------------------------------")
    return amazon

# === MAIN ===
def main():
    # Caricamento dati
    amazon = pd.read_csv("C:/Users/filip/Documents/Analisi-Amazon-FBA/Amazon Sale Report.csv")
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
