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
    Create a bar chart from a DataFrame.

    Args:
        df: DataFrame containing the data.
        x: Column name for x-axis categories.
        y: Column name for y-axis values.
        title: Optional title for the chart.
        xlabel: Label for the x-axis (defaults to x).
        ylabel: Label for the y-axis (defaults to y).
        figsize: Figure size.

    Returns:
        Matplotlib Figure object.
    """
    fig, ax = plt.subplots(figsize=figsize)
    ax.bar(df[x].astype(str), df[y])
    ax.set_xlabel(xlabel or x)
    ax.set_ylabel(ylabel or y)
    if title:
        ax.set_title(title)
    plt.xticks(rotation=45, ha='right')
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
    Create a line chart, optionally with grouping.

    Args:
        df: DataFrame containing the data.
        x: Column name for x-axis (must be sortable, e.g., datetime or numeric).
        y: Column name for y-axis values.
        hue: Optional column name for grouping (multiple lines).
        title: Optional title for the chart.
        xlabel: Label for the x-axis (defaults to x).
        ylabel: Label for the y-axis (defaults to y).
        figsize: Figure size.

    Returns:
        Matplotlib Figure object.
    """
    fig, ax = plt.subplots(figsize=figsize)
    if hue and hue in df.columns:
        for key, grp in df.groupby(hue):
            ax.plot(grp[x], grp[y], label=str(key))
        ax.legend(title=hue)
    else:
        ax.plot(df[x], df[y])
    ax.set_xlabel(xlabel or x)
    ax.set_ylabel(ylabel or y)
    if title:
        ax.set_title(title)
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
    Create a heatmap from a pivot of the DataFrame.

    Args:
        df: DataFrame containing the data.
        index: Column to use as heatmap rows.
        columns: Column to use as heatmap columns.
        values: Column for cell values.
        title: Optional title for the heatmap.
        figsize: Figure size.

    Returns:
        Matplotlib Figure object.
    """
    pivot = df.pivot(index=index, columns=columns, values=values).fillna(0)
    fig, ax = plt.subplots(figsize=figsize)
    cax = ax.imshow(pivot, aspect='auto')
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha='right')
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    if title:
        ax.set_title(title)
    fig.colorbar(cax, ax=ax)
    plt.tight_layout()
    return fig


def save_figure(
    fig: plt.Figure,
    path: str,
    fmt: str = 'png'
) -> None:
    """
    Save a Matplotlib figure to disk.

    Args:
        fig: Figure object.
        path: File path without extension.
        fmt: File format (e.g., 'png', 'pdf').
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    full_path = f"{path}.{fmt}"
    fig.savefig(full_path)
    plt.close(fig)
    return
