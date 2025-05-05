import pandas as pd
import os
import logging
from typing import List, Dict, Union

# Configurazione del logging per tracciare le operazioni e gli errori
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


def load_raw(path: str, **read_csv_kwargs) -> pd.DataFrame:
    """
    Carica i dati grezzi da un file CSV specificato.

    Args:
        path: Percorso del file CSV.
        **read_csv_kwargs: Argomenti aggiuntivi per pandas.read_csv.

    Returns:
        DataFrame contenente i dati caricati.

    Raises:
        FileNotFoundError: Se il file non esiste.
        pd.errors.ParserError: Se il parsing del CSV fallisce.
    """
    if not os.path.isfile(path):
        logger.error(f"File non trovato: {path}")
        raise FileNotFoundError(f"Il file '{path}' non è stato trovato.")
    try:
        df = pd.read_csv(path, **read_csv_kwargs)
        logger.info(f"Dati grezzi caricati da {path} con dimensioni {df.shape}")
        return df
    except Exception as e:
        logger.exception(f"Errore durante il caricamento dei dati grezzi da {path}")
        raise


def drop_unused_columns(df: pd.DataFrame, cols_to_drop: List[str]) -> pd.DataFrame:
    """
    Elimina colonne non necessarie per l'analisi.

    Args:
        df: DataFrame di input.
        cols_to_drop: Lista dei nomi delle colonne da eliminare.

    Returns:
        DataFrame senza le colonne specificate.
    """
    # Verifica quali colonne da eliminare esistono effettivamente nel DataFrame
    existing = [col for col in cols_to_drop if col in df.columns]
    df_dropped = df.drop(columns=existing)
    logger.info(f"Colonne eliminate: {existing}")
    return df_dropped


def parse_dates(
    df: pd.DataFrame,
    date_cols: List[str],
    date_format: str = None
) -> pd.DataFrame:
    """
    Converte le colonne specificate in formato datetime.

    Args:
        df: DataFrame di input.
        date_cols: Lista delle colonne da convertire.
        date_format: Stringa opzionale per specificare il formato datetime.

    Returns:
        DataFrame con le colonne datetime convertite.
    """
    for col in date_cols:
        if col in df.columns:
            # Conversione della colonna in datetime con gestione degli errori
            df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
            n_missing = df[col].isna().sum()
            logger.info(f"Date convertite nella colonna '{col}'. Conversioni fallite: {n_missing}")
        else:
            logger.warning(f"Colonna data '{col}' non trovata nel DataFrame.")
    return df


def handle_missing(
    df: pd.DataFrame,
    strategy_per_col: Dict[str, Union[str, tuple]] = None,
    default_strategy: str = 'drop'
) -> pd.DataFrame:
    """
    Gestisce i valori mancanti utilizzando strategie specificate.

    Args:
        df: DataFrame di input.
        strategy_per_col: Mappatura colonna->strategia. Strategie supportate:
            - 'drop': rimuove righe con valori mancanti
            - 'mean', 'median', 'mode': riempie valori numerici/categoriali
            - ('constant', value): riempie con un valore costante
        default_strategy: Strategia da applicare a tutti i valori mancanti se strategy_per_col è None.

    Returns:
        DataFrame con i valori mancanti gestiti.
    """
    df_clean = df.copy()
    if strategy_per_col:
        for col, strat in strategy_per_col.items():
            if col not in df_clean.columns:
                logger.warning(f"Strategia specificata per la gestione dei mancanti ma colonna '{col}' non trovata.")
                continue
            if strat == 'drop':
                # Rimuove righe con valori mancanti nella colonna specificata
                before = len(df_clean)
                df_clean = df_clean[df_clean[col].notna()]
                after = len(df_clean)
                logger.info(f"Rimosse {before-after} righe con valori mancanti in '{col}'.")
            elif strat == 'mean' and pd.api.types.is_numeric_dtype(df_clean[col]):
                # Riempie i valori mancanti con la media
                fill = df_clean[col].mean()
                df_clean[col].fillna(fill, inplace=True)
                logger.info(f"Riempiti i mancanti in '{col}' con la media={fill}.")
            elif strat == 'median' and pd.api.types.is_numeric_dtype(df_clean[col]):
                # Riempie i valori mancanti con la mediana
                fill = df_clean[col].median()
                df_clean[col].fillna(fill, inplace=True)
                logger.info(f"Riempiti i mancanti in '{col}' con la mediana={fill}.")
            elif strat == 'mode':
                # Riempie i valori mancanti con la moda
                modes = df_clean[col].mode(dropna=True)
                if not modes.empty:
                    fill = modes[0]
                    df_clean[col].fillna(fill, inplace=True)
                    logger.info(f"Riempiti i mancanti in '{col}' con la moda='{fill}'.")
            elif isinstance(strat, tuple) and strat[0] == 'constant':
                # Riempie i valori mancanti con un valore costante
                fill = strat[1]
                df_clean[col].fillna(fill, inplace=True)
                logger.info(f"Riempiti i mancanti in '{col}' con il valore costante={fill}.")
            else:
                logger.warning(f"Strategia sconosciuta o incompatibile '{strat}' per la colonna '{col}'.")
    elif default_strategy == 'drop':
        # Rimuove tutte le righe con valori mancanti
        before = len(df_clean)
        df_clean = df_clean.dropna()
        after = len(df_clean)
        logger.info(f"Rimosse {before-after} righe con valori mancanti.")
    else:
        logger.warning(f"Strategia predefinita '{default_strategy}' non implementata.")
    return df_clean


def standardize_text(df: pd.DataFrame, text_cols: List[str]) -> pd.DataFrame:
    """
    Standardizza le colonne di testo rimuovendo spazi e convertendo in minuscolo.

    Args:
        df: DataFrame di input.
        text_cols: Lista dei nomi delle colonne di testo.

    Returns:
        DataFrame con le colonne di testo standardizzate.
    """
    for col in text_cols:
        if col in df.columns and pd.api.types.is_object_dtype(df[col]):
            # Rimuove spazi e converte il testo in minuscolo
            df[col] = df[col].astype(str).str.strip().str.lower()
            logger.info(f"Testo standardizzato nella colonna '{col}'.")
        else:
            logger.warning(f"Colonna di testo '{col}' non trovata o non di tipo object.")
    return df


def save_processed(
    df: pd.DataFrame,
    path: str,
    index: bool = False
) -> None:
    """
    Salva il DataFrame processato in un file CSV.

    Args:
        df: DataFrame da salvare.
        path: Percorso del file di output.
        index: Se includere o meno i nomi delle righe (indice).
    """
    # Crea la directory di destinazione se non esiste
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    df.to_csv(path, index=index)
    logger.info(f"Dati processati salvati in {path}")
