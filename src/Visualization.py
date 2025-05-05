import matplotlib.pyplot as plt
import os
import pandas as pd


def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str = None,
    xlabel: str = None,
    ylabel: str = None,
    figsize: tuple = (10, 6)
) -> plt.Figure:
    """
    Crea un grafico a barre a partire da un DataFrame.

    Args:
        df: DataFrame contenente i dati.
        x: Nome della colonna per le categorie sull'asse x.
        y: Nome della colonna per i valori sull'asse y.
        title: Titolo opzionale per il grafico.
        xlabel: Etichetta per l'asse x (di default usa il nome della colonna x).
        ylabel: Etichetta per l'asse y (di default usa il nome della colonna y).
        figsize: Dimensioni della figura (larghezza, altezza).

    Returns:
        Oggetto Figure di Matplotlib.
    """
    # Crea una figura e un asse
    fig, ax = plt.subplots(figsize=figsize)
    # Disegna un grafico a barre
    ax.bar(df[x].astype(str), df[y])
    # Imposta le etichette degli assi
    ax.set_xlabel(xlabel or x)
    ax.set_ylabel(ylabel or y)
    # Imposta il titolo, se fornito
    if title:
        ax.set_title(title)
    # Ruota le etichette sull'asse x per una migliore leggibilità
    plt.xticks(rotation=45, ha='right')
    # Adatta automaticamente il layout per evitare sovrapposizioni
    plt.tight_layout()
    return fig


def line_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    hue: str = None,
    title: str = None,
    xlabel: str = None,
    ylabel: str = None,
    figsize: tuple = (10, 6)
) -> plt.Figure:
    """
    Crea un grafico a linee, opzionalmente con raggruppamenti.

    Args:
        df: DataFrame contenente i dati.
        x: Nome della colonna per l'asse x (deve essere ordinabile, ad esempio datetime o numerico).
        y: Nome della colonna per i valori sull'asse y.
        hue: Nome opzionale della colonna per il raggruppamento (linee multiple).
        title: Titolo opzionale per il grafico.
        xlabel: Etichetta per l'asse x (di default usa il nome della colonna x).
        ylabel: Etichetta per l'asse y (di default usa il nome della colonna y).
        figsize: Dimensioni della figura (larghezza, altezza).

    Returns:
        Oggetto Figure di Matplotlib.
    """
    # Crea una figura e un asse
    fig, ax = plt.subplots(figsize=figsize)
    # Se è specificato un raggruppamento (hue), disegna una linea per ogni gruppo
    if hue and hue in df.columns:
        for key, grp in df.groupby(hue):
            ax.plot(grp[x], grp[y], label=str(key))
        # Aggiunge una legenda per identificare i gruppi
        ax.legend(title=hue)
    else:
        # Disegna una singola linea
        ax.plot(df[x], df[y])
    # Imposta le etichette degli assi
    ax.set_xlabel(xlabel or x)
    ax.set_ylabel(ylabel or y)
    # Imposta il titolo, se fornito
    if title:
        ax.set_title(title)
    # Adatta automaticamente il layout per evitare sovrapposizioni
    fig.autofmt_xdate()
    plt.tight_layout()
    return fig


def heatmap(
    df: pd.DataFrame,
    index: str,
    columns: str,
    values: str,
    title: str = None,
    figsize: tuple = (10, 8)
) -> plt.Figure:
    """
    Crea una heatmap a partire da una pivot del DataFrame.

    Args:
        df: DataFrame contenente i dati.
        index: Colonna da usare come righe della heatmap.
        columns: Colonna da usare come colonne della heatmap.
        values: Colonna per i valori delle celle.
        title: Titolo opzionale per la heatmap.
        figsize: Dimensioni della figura (larghezza, altezza).

    Returns:
        Oggetto Figure di Matplotlib.
    """
    # Crea una tabella pivot a partire dal DataFrame
    pivot = df.pivot(index=index, columns=columns, values=values).fillna(0)
    # Crea una figura e un asse
    fig, ax = plt.subplots(figsize=figsize)
    # Disegna la heatmap
    cax = ax.imshow(pivot, aspect='auto')
    # Imposta le etichette delle colonne e delle righe
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha='right')
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    # Imposta il titolo, se fornito
    if title:
        ax.set_title(title)
    # Aggiunge una barra dei colori per rappresentare i valori
    fig.colorbar(cax, ax=ax)
    # Adatta automaticamente il layout per evitare sovrapposizioni
    plt.tight_layout()
    return fig


def save_figure(
    fig: plt.Figure,
    path: str,
    fmt: str = 'png'
) -> None:
    """
    Salva una figura Matplotlib su disco.

    Args:
        fig: Oggetto Figure.
        path: Percorso del file senza estensione.
        fmt: Formato del file (ad esempio, 'png', 'pdf').

    Returns:
        None
    """
    # Crea la directory se non esiste
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Costruisce il percorso completo del file
    full_path = f"{path}.{fmt}"
    # Salva la figura nel formato specificato
    fig.savefig(full_path)
    # Chiude la figura per liberare memoria
    plt.close(fig)
    return
