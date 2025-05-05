import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import missingno as msno

amazon = pd.read_csv("C:/Users/filip/Documents/Analisi-Amazon-FBA/Amazon Sale Report.csv")
# === Data Cleaning ===
def clean_data(amazon):
    # Eliminazione delle colonne inutili
    amazon.drop(columns = ['Unnamed: 22','fulfilled-by','ship-country', 'currency'], inplace = True)
    
    # Eliminazione delle righe duplicate
    before_remove_duplicates = len(amazon)
    amazon.drop_duplicates(inplace = True)
    after_remove_duplicates = len(amazon)
    duplicate_rows_removed = before_remove_duplicates - after_remove_duplicates
    print(f'{duplicate_rows_removed} duplicate rows have been removed! \nThe Dataset now has {after_remove_duplicates} rows.')

    # Restituisce le righe con i dati mancanti
    print("Righe con dati mancanti:\n")
    amazon[amazon.isnull().any(axis = 1)]

    return amazon

# imposta una colonna chiamata 'index' come indice del DataFrame amazon.
amazon.set_index('index', inplace = True)

amazon.info()

amazon.describe()

# Visualizzazione dei dati mancanti
msno.matrix(amazon)
plt.show() 

msno.bar(amazon)
plt.show() 

print(amazon.nunique())
print(amazon.apply(pd.unique))

print(amazon[amazon.isnull().any(axis = 1)])

amazon = clean_data(amazon)
print(amazon)