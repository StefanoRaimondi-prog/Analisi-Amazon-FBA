import pandas as pd
import matplotlib.pyplot as plt
import logging
import os
from typing import List, Optional

# Configurazione del logging per tracciare eventi e messaggi
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def aggregate_time(
    df: pd.DataFrame,
    date_col: str,
    freq: str = 'M',
    metrics: Optional[List[str]] = None,
    groupby_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Aggrega i dati di vendita nel tempo con un'opzione di raggruppamento.

    Args:
        df: DataFrame pre-elaborato con i dati di vendita.
        date_col: Nome della colonna contenente le date.
        freq: Frequenza di campionamento (es. 'D', 'W', 'M').
        metrics: Lista di colonne numeriche da aggregare (default: tutte le colonne numeriche).
        groupby_col: Colonna opzionale per ulteriori raggruppamenti (es. 'ASIN').

    Returns:
        DataFrame con le metriche aggregate. Se groupby_col=None, colonne: [date_col] + metrics.
        Se groupby_col è specificato, colonne: [date_col, groupby_col] + metrics.

    Raises:
        ValueError: Se date_col o metriche/groupby_col mancanti.
    """
    # Verifica che la colonna delle date esista
    if date_col not in df.columns:
        logger.error(f"Colonna delle date '{date_col}' non trovata.")
        raise ValueError(f"Colonna delle date mancante: {date_col}")

    # Imposta le metriche di default se non specificate
    if metrics is None:
        metrics = df.select_dtypes(include='number').columns.tolist()
        logger.info(f"Nessuna metrica specificata. Uso colonne numeriche: {metrics}")
    else:
        # Verifica che tutte le metriche esistano
        missing_metrics = [c for c in metrics if c not in df.columns]
        if missing_metrics:
            logger.error(f"Colonne metriche mancanti: {missing_metrics}")
            raise ValueError(f"Metriche mancanti: {missing_metrics}")

    # Controlla che la colonna di raggruppamento esista, se specificata
    if groupby_col and groupby_col not in df.columns:
        logger.error(f"Colonna di raggruppamento '{groupby_col}' non trovata.")
        raise ValueError(f"Colonna di raggruppamento mancante: {groupby_col}")

    # Assicura che la colonna delle date sia di tipo datetime
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        logger.info(f"Convertita la colonna '{date_col}' in formato datetime.")

    # Rimuove righe con date mancanti
    missing_dates = df[date_col].isna().sum()
    if missing_dates > 0:
        df = df.dropna(subset=[date_col])
        logger.warning(f"Rimosse {missing_dates} righe con date non valide.")

    # Esegue l'aggregazione
    if groupby_col:
        grouped = (
            df
            .groupby([pd.Grouper(key=date_col, freq=freq), groupby_col])[metrics]
            .sum()
            .reset_index()
        )
    else:
        grouped = (
            df
            .groupby(pd.Grouper(key=date_col, freq=freq))[metrics]
            .sum()
            .reset_index()
        )
    logger.info(f"Dati aggregati con freq='{freq}'{' e gruppo=' + groupby_col if groupby_col else ''}.")
    return grouped


def plot_time_series(
    df: pd.DataFrame,
    date_col: str,
    value_col: str,
    group_col: Optional[str] = None,
    top_n: Optional[int] = None,
    title: Optional[str] = None
) -> plt.Figure:
    """
    Crea un grafico a serie temporale per una metrica, opzionalmente per i primi N gruppi.

    Args:
        df: DataFrame aggregato da aggregate_time, con colonne [date_col, (group_col?), value_col].
        date_col: Nome della colonna delle date.
        value_col: Nome della metrica da visualizzare.
        group_col: Colonna opzionale per raggruppare (es. 'ASIN').
        top_n: Se group_col è specificato, numero di gruppi principali da visualizzare in base al valore totale.
        title: Titolo opzionale del grafico.

    Returns:
        Oggetto Figure di Matplotlib.

    Raises:
        ValueError: Se le colonne richieste sono mancanti o top_n non è valido.
    """
    # Verifica che le colonne richieste esistano
    for col in [date_col, value_col] + ([group_col] if group_col else []):
        if col and col not in df.columns:
            logger.error(f"Colonna '{col}' non trovata nel DataFrame per il grafico.")
            raise ValueError(f"Colonna mancante: {col}")

    # Prepara il grafico
    fig, ax = plt.subplots()

    if group_col:
        # Determina i gruppi principali da visualizzare
        grouped_totals = df.groupby(group_col)[value_col].sum().nlargest(top_n or 10)
        groups_to_plot = grouped_totals.index.tolist()
        logger.info(f"Visualizzazione dei gruppi principali: {groups_to_plot}")

        for grp in groups_to_plot:
            subset = df[df[group_col] == grp]
            ax.plot(subset[date_col], subset[value_col], label=str(grp))
        ax.legend(title=group_col)
    else:
        ax.plot(df[date_col], df[value_col], label=value_col)

    # Formattazione del grafico
    ax.set_xlabel('Data')
    ax.set_ylabel(value_col)
    if title:
        ax.set_title(title)
    ax.grid(True)

    fig.autofmt_xdate()
    plt.tight_layout()
    logger.info("Grafico a serie temporale generato.")
    return fig


def save_plot(
    fig: plt.Figure,
    path: str,
    fmt: str = 'png'
) -> None:
    """
    Salva un grafico Matplotlib in un file.

    Args:
        fig: Oggetto Figure da salvare.
        path: Percorso del file di output (senza estensione).
        fmt: Formato del file (es. 'png', 'pdf').
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    full_path = f"{path}.{fmt}"
    fig.savefig(full_path)
    logger.info(f"Grafico salvato in {full_path}")


if __name__ == '__main__':
    import argparse

    # Parser per gli argomenti della riga di comando
    parser = argparse.ArgumentParser(description='Aggrega e visualizza le tendenze di vendita nel tempo.')
    parser.add_argument('input', help='Percorso al file CSV pre-elaborato')
    parser.add_argument('--date_col', default='Date', help='Nome della colonna delle date')
    parser.add_argument('--freq', default='M', help="Frequenza di campionamento (es. 'D','W','M')")
    parser.add_argument('--metrics', nargs='+', default=['Qty'], help='Colonne metriche da aggregare')
    parser.add_argument('--groupby', help='Colonna opzionale per raggruppare (es. ASIN)')
    parser.add_argument('--plot_metric', default='Qty', help='Metrica da visualizzare')
    parser.add_argument('--top_n', type=int, default=10, help='Primi N gruppi da visualizzare')
    parser.add_argument('--output', help='Prefisso del percorso per salvare il grafico')
    args = parser.parse_args()

    # Carica il file CSV e converte la colonna delle date
    df = pd.read_csv(args.input, parse_dates=[args.date_col])
    df_agg = aggregate_time(df, args.date_col, args.freq, args.metrics, args.groupby)
    if args.output:
        for metric in args.metrics:
            fig = plot_time_series(
                df_agg, args.date_col, metric,
                group_col=args.groupby, top_n=args.top_n,
                title=f"Tendenza di {metric}"
            )
            save_plot(fig, f"{args.output}_{metric}")
    else:
        # Mostra un solo grafico
        fig = plot_time_series(
            df_agg, args.date_col, args.plot_metric,
            group_col=args.groupby, top_n=args.top_n
        )
        plt.show()
