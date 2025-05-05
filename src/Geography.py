import pandas as pd
import logging
import os
from typing import Dict, Optional

# Configura il logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def define_regions(mapping: Dict[str, str]) -> Dict[str, str]:
    """
    Valida e restituisce una mappatura paese-regione.

    Argomenti:
        mapping: Dizionario dove le chiavi sono codici/nomi di paesi e i valori sono nomi di regioni.

    Restituisce:
        Il dizionario di mappatura validato.

    Solleva:
        ValueError: Se la mappatura è vuota.
    """
    if not mapping:
        logger.error("Mappatura vuota fornita a define_regions.")
        raise ValueError("La mappatura delle regioni non può essere vuota.")
    logger.info(f"Definite {len(mapping)} mappature di regioni.")
    return mapping


def map_to_region(
    df: pd.DataFrame,
    geo_col: str,
    mapping: Dict[str, str],
    default_region: Optional[str] = 'Unknown'
) -> pd.DataFrame:
    """
    Mappa i valori di una colonna geografica alle regioni.

    Argomenti:
        df: DataFrame di input con informazioni geografiche.
        geo_col: Nome della colonna contenente paese o stato.
        mapping: Dizionario per mappare i valori in geo_col ai nomi delle regioni.
        default_region: Nome da assegnare se non viene trovata alcuna mappatura.

    Restituisce:
        DataFrame con una nuova colonna 'region'.

    Solleva:
        ValueError: Se geo_col non è presente nel DataFrame.
    """
    if geo_col not in df.columns:
        logger.error(f"Colonna geografica '{geo_col}' non trovata.")
        raise ValueError(f"Colonna '{geo_col}' non trovata nel DataFrame.")

    df = df.copy()
    df['region'] = df[geo_col].map(mapping).fillna(default_region)
    n_unmapped = (df['region'] == default_region).sum()
    logger.info(f"Mappata la colonna geografica '{geo_col}' alle regioni. Voci non mappate: {n_unmapped}")
    return df


def popularity_by_region(
    df: pd.DataFrame,
    region_col: str,
    product_col: str = 'ASIN',
    metric: str = 'Qty'
) -> pd.DataFrame:
    """
    Calcola il metric di popolarità aggregato per regione e prodotto.

    Argomenti:
        df: DataFrame con colonna 'region' e metriche di vendita.
        region_col: Nome della colonna per la regione.
        product_col: Nome della colonna per l'identificativo del prodotto.
        metric: Nome della colonna numerica da aggregare (es. 'Qty' o 'Amount').

    Restituisce:
        DataFrame con colonne [region_col, product_col, 'metric', 'popularity'],
        ordinato per regione e popolarità decrescente.

    Solleva:
        ValueError: Se mancano colonne richieste o se metric non è numerico.
    """
    # Valida le colonne
    required = [region_col, product_col, metric]
    missing = [col for col in required if col not in df.columns]
    if missing:
        logger.error(f"Colonne mancanti per popularity_by_region: {missing}")
        raise ValueError(f"Colonne mancanti: {missing}")

    if not pd.api.types.is_numeric_dtype(df[metric]):
        logger.error(f"La colonna metric '{metric}' non è numerica.")
        raise ValueError(f"La colonna metric '{metric}' deve essere numerica.")

    # Raggruppa e aggrega
    agg_df = (
        df.groupby([region_col, product_col])[metric]
        .sum()
        .reset_index(name='popularity')
    )

    # Ordina all'interno di ogni regione
    agg_df = agg_df.sort_values([region_col, 'popularity'], ascending=[True, False])
    logger.info(f"Calcolata la popolarità per regione per {agg_df[product_col].nunique()} prodotti in {agg_df[region_col].nunique()} regioni.")
    return agg_df


def save_region_popularity(
    df: pd.DataFrame,
    region_pop_df: pd.DataFrame,
    path: str
) -> None:
    """
    Salva il DataFrame di popolarità regionale in un file CSV.

    Argomenti:
        df: DataFrame originale (non utilizzato, per coerenza CLI).
        region_pop_df: DataFrame da popularity_by_region.
        path: Percorso del file di output.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    region_pop_df.to_csv(path, index=False)
    logger.info(f"Popolarità regionale salvata in {path}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Mappa alle regioni e calcola la popolarità per regione.')
    parser.add_argument('input', help='Percorso al file CSV preprocessato')
    parser.add_argument('--geo_col', default='ship-country', help='Colonna geografica da mappare')
    parser.add_argument('--mapping_file', help='Percorso al CSV con due colonne: geo_value, region_name')
    parser.add_argument('--metric', default='Qty', help='Colonna metric da aggregare')
    parser.add_argument('--product_col', default='ASIN', help='Colonna prodotto per il raggruppamento')
    parser.add_argument('--output', help='Percorso per salvare il CSV di popolarità regionale')
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    # Carica la mappatura
    mapping = {}
    if args.mapping_file:
        map_df = pd.read_csv(args.mapping_file)
        mapping = dict(zip(map_df.iloc[:,0], map_df.iloc[:,1]))
    regions = define_regions(mapping)
    df_mapped = map_to_region(df, args.geo_col, regions)
    region_pop = popularity_by_region(df_mapped, 'region', args.product_col, args.metric)
    if args.output:
        save_region_popularity(df, region_pop, args.output)
    else:
        print(region_pop.head())
    logger.info("Analisi geografica completata.")
