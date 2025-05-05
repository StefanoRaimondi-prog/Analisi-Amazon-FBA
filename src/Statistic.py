import pandas as pd
import logging
import os
from typing import List, Tuple, Union

# Configurazione del logging per tracciare le operazioni e gli errori
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def summary_stats(
    df: pd.DataFrame,
    groupby_col: str,
    agg_cols: List[str]
) -> pd.DataFrame:
    """
    Calcola statistiche riassuntive (conteggio, media, mediana, deviazione standard, minimo, 25%, 75%, massimo)
    per colonne numeriche specificate, raggruppate per una chiave.

    Args:
        df: DataFrame di input.
        groupby_col: Nome della colonna su cui raggruppare.
        agg_cols: Lista di colonne numeriche da analizzare.

    Returns:
        DataFrame con colonne <groupby_col> e, per ogni colonna in agg_cols, col_count, col_mean, ecc.

    Raises:
        ValueError: Se la colonna di raggruppamento o una delle colonne da aggregare è mancante.
    """
    # Validazione delle colonne di input
    missing = [col for col in [groupby_col] + agg_cols if col not in df.columns]
    if missing:
        logger.error(f"Colonne mancanti per summary_stats: {missing}")
        raise ValueError(f"Colonne mancanti: {missing}")

    results = []
    # Per ogni colonna numerica, calcola le statistiche
    for col in agg_cols:
        group = df.groupby(groupby_col)[col]
        stats = pd.DataFrame({
            f"{col}_count": group.count(),
            f"{col}_mean": group.mean(),
            f"{col}_median": group.median(),
            f"{col}_std": group.std(),
            f"{col}_min": group.min(),
            f"{col}_25%": group.quantile(0.25),
            f"{col}_75%": group.quantile(0.75),
            f"{col}_max": group.max(),
        })
        results.append(stats)
        logger.info(f"Statistiche calcolate per la colonna '{col}'")

    # Unisce tutte le statistiche sull'indice (groupby_col)
    summary_df = pd.concat(results, axis=1).reset_index()
    logger.info(f"Statistiche riassuntive calcolate per {len(summary_df)} gruppi.")
    return summary_df


def detect_outliers(
    df: pd.DataFrame,
    col: str,
    method: str = 'iqr',
    factor: float = 1.5
) -> pd.Series:
    """
    Rileva outlier in una colonna numerica utilizzando il metodo IQR.

    Args:
        df: DataFrame di input.
        col: Nome della colonna da analizzare.
        method: Metodo per rilevare outlier (supportato solo 'iqr').
        factor: Fattore moltiplicativo per l'IQR.

    Returns:
        Serie booleana che indica gli outlier.
    """
    if col not in df.columns:
        logger.error(f"Colonna '{col}' non trovata per il rilevamento degli outlier.")
        raise ValueError(f"Colonna '{col}' non trovata.")
    if method.lower() != 'iqr':
        logger.error(f"Metodo non supportato: {method}")
        raise ValueError("Solo il metodo 'iqr' è supportato.")

    series = df[col].dropna()
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    outliers = (df[col] < lower) | (df[col] > upper)
    logger.info(f"Rilevati {outliers.sum()} outlier nella colonna '{col}' utilizzando IQR.")
    return outliers


def long_tail_analysis(
    df: pd.DataFrame,
    groupby_col: str,
    metric_col: str,
    threshold: float = 0.8
) -> pd.DataFrame:
    """
    Analizza la distribuzione "head" vs "long-tail" di una metrica tra i gruppi.

    Args:
        df: DataFrame di input.
        groupby_col: Nome della colonna su cui raggruppare.
        metric_col: Nome della colonna metrica da analizzare.
        threshold: Soglia per separare "head" e "tail" (valore tra 0 e 1).

    Returns:
        DataFrame con la distribuzione cumulativa e la segmentazione in "head" e "tail".
    """
    if groupby_col not in df.columns or metric_col not in df.columns:
        logger.error(f"Colonne mancanti per long_tail: {groupby_col}, {metric_col}")
        raise ValueError("Colonne richieste mancanti.")
    if not 0 < threshold < 1:
        raise ValueError("La soglia deve essere compresa tra 0 e 1.")

    agg = df.groupby(groupby_col)[metric_col].sum().reset_index(name='metric_sum')
    agg = agg.sort_values('metric_sum', ascending=False)
    total = agg['metric_sum'].sum()
    agg['cum_pct'] = agg['metric_sum'].cumsum() / total
    agg['segment'] = agg['cum_pct'].apply(lambda x: 'head' if x <= threshold else 'tail')
    logger.info(f"Long-tail: {sum(agg['segment']=='head')} gruppi 'head' su {len(agg)}.")
    return agg


def save_stats(
    df: pd.DataFrame,
    path: str,
    stats_df: pd.DataFrame
) -> None:
    """
    Salva il DataFrame delle statistiche in un file CSV.

    Args:
        df: DataFrame di input (non utilizzato direttamente).
        path: Percorso del file CSV di output.
        stats_df: DataFrame delle statistiche da salvare.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    stats_df.to_csv(path, index=False)
    logger.info(f"Statistiche salvate in {path}")


if __name__ == '__main__':
    import argparse
    # Parser degli argomenti della riga di comando
    parser = argparse.ArgumentParser()
    parser.add_argument('input')  # File di input
    parser.add_argument('--groupby', required=True)  # Colonna per il raggruppamento
    parser.add_argument('--metrics', nargs='+', required=True)  # Colonne metriche
    parser.add_argument('--output')  # File di output
    parser.add_argument('--detect_outlier_col')  # Colonna per rilevare outlier
    parser.add_argument('--long_tail_col')  # Colonna per l'analisi long-tail
    parser.add_argument('--threshold', type=float, default=0.8)  # Soglia per long-tail
    args = parser.parse_args()

    # Caricamento del DataFrame
    df = pd.read_csv(args.input)
    # Calcolo delle statistiche riassuntive
    summary = summary_stats(df, args.groupby, args.metrics)
    if args.output:
        save_stats(df, args.output, summary)
    # Rilevamento degli outlier
    if args.detect_outlier_col:
        print(detect_outliers(df, args.detect_outlier_col).sum())
    # Analisi long-tail
    if args.long_tail_col:
        lt = long_tail_analysis(df, args.groupby, args.long_tail_col, args.threshold)
        lt_path = args.output.replace('.csv','_longtail.csv') if args.output else 'longtail.csv'
        save_stats(df, lt_path, lt)
    logger.info("Analisi statistica completata.")
