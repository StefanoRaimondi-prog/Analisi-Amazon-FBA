import pandas as pd
import logging
import os
from typing import Optional

# Configurazione del logging
# Configura il logger per registrare messaggi di log con timestamp, nome del logger e livello di severità.
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()  # Gestore per inviare i log alla console
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)  # Imposta il livello di logging su INFO


def compute_popularity(
    df: pd.DataFrame,
    product_col: str = 'ASIN',
    metric: str = 'quantity'
) -> pd.DataFrame:
    """
    Calcola la popolarità dei prodotti in base alla quantità venduta o al ricavo.

    Args:
        df: DataFrame pre-elaborato contenente i dati di vendita.
        product_col: Nome della colonna per raggruppare i prodotti (es. 'ASIN' o 'SKU').
        metric: 'quantity' per sommare la colonna 'Qty', 'revenue' per sommare la colonna 'Amount'.

    Returns:
        DataFrame con colonne [product_col, 'popularity'], ordinato in ordine decrescente.

    Raises:
        ValueError: Se product_col non è presente nel DataFrame, se il metric è invalido o se manca una colonna necessaria.
    """
    # Controlla se la colonna del prodotto esiste nel DataFrame
    if product_col not in df.columns:
        logger.error(f"Colonna prodotto '{product_col}' non trovata nel DataFrame.")
        raise ValueError(f"Colonna '{product_col}' non trovata.")

    # Normalizza il valore del parametro metric
    metric = metric.lower()
    if metric == 'quantity':
        # Calcolo della popolarità in base alla quantità venduta
        qty_col = 'Qty'
        if qty_col not in df.columns:
            logger.error(f"Colonna quantità '{qty_col}' non trovata nel DataFrame.")
            raise ValueError(f"Colonna '{qty_col}' non trovata per il metric quantità.")
        agg_series = df.groupby(product_col)[qty_col].sum()
    elif metric == 'revenue':
        # Calcolo della popolarità in base al ricavo
        rev_col = 'Amount'
        if rev_col not in df.columns:
            logger.error(f"Colonna ricavo '{rev_col}' non trovata nel DataFrame.")
            raise ValueError(f"Colonna '{rev_col}' non trovata per il metric ricavo.")
        agg_series = df.groupby(product_col)[rev_col].sum()
    else:
        # Metric non valido
        logger.error(f"Metric invalido '{metric}'. Scegliere 'quantity' o 'revenue'.")
        raise ValueError("Il metric deve essere 'quantity' o 'revenue'.")

    # Creazione del DataFrame di popolarità ordinato in ordine decrescente
    popularity_df = (
        agg_series
        .reset_index(name='popularity')  # Reset dell'indice e rinomina della colonna aggregata
        .sort_values(by='popularity', ascending=False)  # Ordinamento decrescente
    )
    logger.info(f"Popolarità calcolata in base a '{metric}' per {len(popularity_df)} prodotti.")
    return popularity_df


def top_n_products(
    popularity_df: pd.DataFrame,
    n: int = 10,
    product_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Seleziona i primi N prodotti da un DataFrame di popolarità.

    Args:
        popularity_df: DataFrame restituito da compute_popularity.
        n: Numero di prodotti da selezionare.
        product_col: Nome della colonna del prodotto se non dedotto (prima colonna).

    Returns:
        DataFrame contenente le prime N righe.

    Raises:
        ValueError: Se manca la colonna 'popularity' o se n <= 0.
    """
    # Controlla se la colonna 'popularity' esiste
    if 'popularity' not in popularity_df.columns:
        logger.error("Colonna 'popularity' non trovata nel DataFrame.")
        raise ValueError("Il DataFrame deve contenere la colonna 'popularity'.")
    if n <= 0:
        logger.error(f"Valore di n={n} invalido. Deve essere > 0.")
        raise ValueError("n deve essere un intero positivo.")

    # Seleziona le prime N righe
    top_df = popularity_df.head(n).copy()
    logger.info(f"Selezionati i primi {n} prodotti.")
    return top_df


def save_top_n(
    df: pd.DataFrame,
    path: str,
    n: int = 10,
    product_col: Optional[str] = None
) -> None:
    """
    Calcola e salva i primi N prodotti in un file CSV.

    Args:
        df: DataFrame pre-elaborato contenente i dati di vendita.
        path: Percorso di output per il file CSV.
        n: Numero di prodotti da salvare.
        product_col: Colonna per raggruppare i prodotti.
    """
    # Calcola la popolarità e seleziona i primi N prodotti
    pop_df = compute_popularity(df, product_col or 'ASIN', 'quantity')
    top_df = top_n_products(pop_df, n, product_col)

    # Crea la directory di output se non esiste
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Salva il DataFrame in un file CSV
    top_df.to_csv(path, index=False)
    logger.info(f"Salvati i primi {n} prodotti in {path}.")


if __name__ == '__main__':
    import argparse

    # Parser degli argomenti della riga di comando
    parser = argparse.ArgumentParser(description='Calcola e salva i primi N prodotti popolari.')
    parser.add_argument('input', help='Percorso al file CSV pre-elaborato')
    parser.add_argument('output', help='Percorso per salvare il file CSV dei primi N prodotti')
    parser.add_argument('--product_col', default='ASIN', help="Colonna per raggruppare i prodotti")
    parser.add_argument('--metric', default='quantity', choices=['quantity','revenue'],
                        help="Metodo per misurare la popolarità")
    parser.add_argument('--top_n', type=int, default=10, help="Numero di prodotti da selezionare")
    args = parser.parse_args()

    # Carica il DataFrame dal file CSV di input
    df_processed = pd.read_csv(args.input)

    # Calcola la popolarità e seleziona i primi N prodotti
    pop = compute_popularity(df_processed, args.product_col, args.metric)
    top = top_n_products(pop, args.top_n, args.product_col)

    # Salva i risultati in un file CSV
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    top.to_csv(args.output, index=False)
    logger.info("Analisi della popolarità completata.")
