import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import missingno as msno

amazon = pd.read_csv("C:/Users/filip/Documents/Analisi-Amazon-FBA/Amazon Sale Report.csv")
# === Data Cleaning ===
def clean_data(df):
    df = df.drop_duplicates()
    return df

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