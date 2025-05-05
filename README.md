# Analisi Vendite Amazon FBA

Una **pipeline end-to-end** per l’analisi dei dati di vendita provenienti da Amazon FBA, strutturata in moduli dedicati per:

* **Preprocessing** dei dati grezzi (pulizia, normalizzazione, estrazione di feature temporali)
* **Calcolo popolarità** dei prodotti (top‑N in base a quantità o fatturato)
* **Statistiche descrittive** e **long-tail analysis**
* **Trend temporali** e visualizzazioni di serie storiche
* **Analisi geografica** per macro‑aree
* **Visualizzazione** comune per grafici a barre, linee e heatmap
* **Modalità batch** e **interattiva** per flessibilità d’uso

---

## 📦 Panoramica della struttura
 │   Amazon Sale Report.csv
│   IntegrationFil.py
│   Pipeline.py
│   README.md
│   Requirement.txt
│
├───.vscode
│       settings.json
│
├───config
│       region_mapping.csv
│
├───data
│   ├───processed
│   │       cleaned.csv
│   │       long_tail_analysis.csv
│   │       region_popularity.csv
│   │       summary_stats.csv
│   │       top_n_products.csv
│   │
│   └───raw
│       │   AmazonSaleReport.csv
│       │
│       └───processed
├───reports
│   └───plots
│       └───trend
│               heatmap_top5_asin.png
│               monthly_qty_trend.png
│
└───src
    │   Config.py
    │   Geography.py
    │   Modeling.py
    │   Popularity.py
    │   Preprocessing.py
    │   Statistic.py
    │   Trend.py
    │   Visualization.py
    │   __init__.py
    │
    └───__pycache__
            Config.cpython-313.pyc
            Geography.cpython-313.pyc
            Popularity.cpython-313.pyc
            Preprocessing.cpython-313.pyc
            Statistic.cpython-313.pyc
            Trend.cpython-313.pyc
            Visualization.cpython-313.pyc
---

## 🎯 Obiettivi del progetto

1. **Pulizia robusta** dei CSV grezzi, inclusa la gestione di duplicati, valori mancanti e nomi di colonna incoerenti.
2. **Estrazione di insight** chiave sui prodotti più venduti, distribuzioni di vendita e outlier.
3. **Analisi head‑tail** per identificare prodotti di punta rispetto alla "long‑tail".
4. **Visualizzazioni** chiare delle serie temporali e mappe di calore per aiutare decisioni di business.
5. **Ricerca di pattern geografici**: individuare macro‑aree con performance diverse.
6. **Flessibilità di esecuzione**, sia in modalità automatica batch che attraverso un menù interattivo.

---

## 📋 Descrizione dei moduli

### 1. `preprocessing.py`

* **load\_raw**: carica CSV con gestione di `low_memory`.
* **drop\_duplicates**: elimina record ripetuti.
* **drop\_unused\_columns**: rimuove colonne non informative.
* **parse\_dates**: converte stringhe in datetime, segnala conversioni fallite.
* **extract\_date\_features**: aggiunge colonne `year`, `month`, `day`.
* **handle\_missing**: imputazione (mediana, moda, valore costante) o drop.
* **rename\_columns**: mappa nomi originali in snake\_case.
* **standardize\_text**: trim e `lower()` su colonne testuali.
* **clean\_data\_pipeline**: wrapper che esegue tutte le fasi successive.

### 2. `popularity.py`

* **compute\_popularity**: somma quantità o fatturato per prodotto.
* **top\_n\_products**: estrae i primi N ASIN per popolarità.

### 3. `statistics.py`

* **summary\_stats**: calcola count, mean, median, std, min, quantili (25%,75%), max.
* **long\_tail\_analysis**: separa prodotti in "head" (cumulativo ≤ soglia) e "tail".
* **detect\_outliers**: IQR method per identificare outlier.

### 4. `trend.py`

* **aggregate\_time**: raggruppa e somma metriche per periodi temporali (D/W/M).
* **plot\_time\_series**: linea temporale con top‑N gruppi opzionali.
* **save\_plot**: salva figure in PNG o PDF.

### 5. `geography.py`

* **define\_regions**: convalida mapping di paesi → regioni.
* **map\_to\_region**: crea colonna `region` dai valori geografici.
* **popularity\_by\_region**: calcola popolarità prodotto per regione.

### 6. `visualization.py`

* **bar\_chart**, **line\_chart**, **heatmap**: funzioni riutilizzabili per grafici.
* **save\_figure**: salva figure e chiude.

### 7. `pipeline.py` & `interactive_pipeline.py`

* Orchestrano le fasi batch o interattive, generando file CSV e grafici.

---

## ⚙️ Installazione e Setup

1. **Clona il repo**:

   ```bash
   git clone <url>
   cd Analisi-Amazon-FBA
   ```
2. **Crea l’ambiente virtuale**:

   ```bash
   python -m venv venv
   source venv/bin/activate   # macOS/Linux
   venv\Scripts\activate    # Windows
   ```
3. **Installa dipendenze**:

   ```bash
   pip install -r requirements.txt
   ```
4. **Verifica il file di mapping**:
   `config/region_mapping.csv` deve contenere tutte le chiavi `ship-country` presenti nel dataset.

---

## ▶️ Esecuzione della Pipeline

### Modalità batch

```bash
python pipeline.py
```

* Vengono creati **tutti** i file in `data/processed/` e i grafici in `reports/plots/`.

### Modalità interattiva

```bash
python interactive_pipeline.py
```

* Scegli le singole fasi da eseguire tramite un menù a video.

---

## 💡 Personalizzazioni

* **Parametri** (top-N, soglia long-tail) in `src/config.py`.
* **Formati date**: personalizza `date_format` in `parse_dates` se serve un pattern fisso.
* **Mapping geografico**: aggiorna `config/region_mapping.csv` per coprire nuovi paesi/regioni.

---

## 📝 Contributi

Pull request e issue sono benvenute. Segnala bug o suggerimenti di miglioramento.

---

## 📄 Licenza

MIT © 2025 \[Il Tuo Nome]
