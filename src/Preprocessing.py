import pandas as pd
import os
import logging
from typing import List, Dict, Union

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(handler)


def load_raw(path: str, **read_csv_kwargs) -> pd.DataFrame:
    """
    Load raw CSV data from the specified path.

    Args:
        path: Path to the CSV file.
        **read_csv_kwargs: Additional keyword arguments for pandas.read_csv.

    Returns:
        DataFrame containing the loaded data.

    Raises:
        FileNotFoundError: If the file does not exist.
        pd.errors.ParserError: If CSV parsing fails.
    """
    if not os.path.isfile(path):
        logger.error(f"File not found: {path}")
        raise FileNotFoundError(f"The file '{path}' was not found.")
    try:
        df = pd.read_csv(path, **read_csv_kwargs)
        logger.info(f"Loaded raw data from {path} with shape {df.shape}")
        return df
    except Exception as e:
        logger.exception(f"Failed to load raw data from {path}")
        raise


def drop_unused_columns(df: pd.DataFrame, cols_to_drop: List[str]) -> pd.DataFrame:
    """
    Drop columns that are not needed for analysis.

    Args:
        df: Input DataFrame.
        cols_to_drop: List of column names to drop.

    Returns:
        DataFrame without the specified columns.
    """
    existing = [col for col in cols_to_drop if col in df.columns]
    df_dropped = df.drop(columns=existing)
    logger.info(f"Dropped columns: {existing}")
    return df_dropped


def parse_dates(
    df: pd.DataFrame,
    date_cols: List[str],
    date_format: str = None
) -> pd.DataFrame:
    """
    Parse specified columns into datetime.

    Args:
        df: Input DataFrame.
        date_cols: List of columns to convert.
        date_format: Optional strftime format string.

    Returns:
        DataFrame with parsed datetime columns.
    """
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
            n_missing = df[col].isna().sum()
            logger.info(f"Parsed dates in '{col}'. Failed conversions: {n_missing}")
        else:
            logger.warning(f"Date column '{col}' not found in DataFrame.")
    return df


def handle_missing(
    df: pd.DataFrame,
    strategy_per_col: Dict[str, Union[str, tuple]] = None,
    default_strategy: str = 'drop'
) -> pd.DataFrame:
    """
    Handle missing values using specified strategies.

    Args:
        df: Input DataFrame.
        strategy_per_col: Mapping column->strategy. Strategies:
            - 'drop': remove rows with missing
            - 'mean', 'median', 'mode': fill numeric/categorical
            - ('constant', value): fill with a constant
        default_strategy: Strategy to apply to all missing if strategy_per_col is None.

    Returns:
        DataFrame with missing values handled.
    """
    df_clean = df.copy()
    if strategy_per_col:
        for col, strat in strategy_per_col.items():
            if col not in df_clean.columns:
                logger.warning(f"Strategy specified for missing handling but column '{col}' not found.")
                continue
            if strat == 'drop':
                before = len(df_clean)
                df_clean = df_clean[df_clean[col].notna()]
                after = len(df_clean)
                logger.info(f"Dropped {before-after} rows due to missing in '{col}'.")
            elif strat == 'mean' and pd.api.types.is_numeric_dtype(df_clean[col]):
                fill = df_clean[col].mean()
                df_clean[col].fillna(fill, inplace=True)
                logger.info(f"Filled missing in '{col}' with mean={fill}.")
            elif strat == 'median' and pd.api.types.is_numeric_dtype(df_clean[col]):
                fill = df_clean[col].median()
                df_clean[col].fillna(fill, inplace=True)
                logger.info(f"Filled missing in '{col}' with median={fill}.")
            elif strat == 'mode':
                modes = df_clean[col].mode(dropna=True)
                if not modes.empty:
                    fill = modes[0]
                    df_clean[col].fillna(fill, inplace=True)
                    logger.info(f"Filled missing in '{col}' with mode='{fill}'.")
            elif isinstance(strat, tuple) and strat[0] == 'constant':
                fill = strat[1]
                df_clean[col].fillna(fill, inplace=True)
                logger.info(f"Filled missing in '{col}' with constant={fill}.")
            else:
                logger.warning(f"Unknown or incompatible strategy '{strat}' for column '{col}'.")
    elif default_strategy == 'drop':
        before = len(df_clean)
        df_clean = df_clean.dropna()
        after = len(df_clean)
        logger.info(f"Dropped {before-after} rows with any missing values.")
    else:
        logger.warning(f"Default strategy '{default_strategy}' is not implemented.")
    return df_clean


def standardize_text(df: pd.DataFrame, text_cols: List[str]) -> pd.DataFrame:
    """
    Standardize text columns by stripping whitespace and lowercasing.

    Args:
        df: Input DataFrame.
        text_cols: List of text column names.

    Returns:
        DataFrame with standardized text.
    """
    for col in text_cols:
        if col in df.columns and pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str).str.strip().str.lower()
            logger.info(f"Standardized text in column '{col}'.")
        else:
            logger.warning(f"Text column '{col}' not found or not object dtype.")
    return df


def save_processed(
    df: pd.DataFrame,
    path: str,
    index: bool = False
) -> None:
    """
    Save processed DataFrame to CSV.

    Args:
        df: DataFrame to save.
        path: Output file path.
        index: Whether to write row names (index).
    """
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    df.to_csv(path, index=index)
    logger.info(f"Processed data saved to {path}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Preprocess raw data file.')
    parser.add_argument('input', help='Path to raw CSV file')
    parser.add_argument('output', help='Path for saving processed CSV')
    args = parser.parse_args()

    df_raw = load_raw(args.input)
    df_clean = drop_unused_columns(df_raw, ['Unnamed: 22'])
    df_clean = parse_dates(df_clean, ['Date'])
    df_clean = handle_missing(df_clean)
    # Example: standardize text for some columns
    df_clean = standardize_text(df_clean, ['Status', 'Courier Status', 'Fulfilment'])
    save_processed(df_clean, args.output)
    logger.info("Preprocessing complete.")
