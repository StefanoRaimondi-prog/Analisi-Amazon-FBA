import os
import logging
import pandas as pd
import joblib
from typing import List, Optional, Union, Dict
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, mean_squared_error, r2_score

# Configurazione del logging per tracciare le operazioni e i messaggi di errore
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def prepare_features(
    df: pd.DataFrame,
    feature_cols: List[str],
    target_col: str,
    test_size: float = 0.2,
    random_state: int = 42
) -> tuple:
    """
    Divide i dati in set di train e test e separa le feature dal target.

    Args:
        df: DataFrame contenente i dati.
        feature_cols: Lista delle colonne da usare come feature.
        target_col: Nome della colonna target.
        test_size: Percentuale di dati da usare per il test.
        random_state: Seed per la riproducibilità.

    Returns:
        X_train, X_test, y_train, y_test
    """
    if target_col not in df.columns:
        logger.error(f"Colonna target '{target_col}' non trovata nel DataFrame.")
        raise ValueError(f"Colonna target mancante: {target_col}")

    # Separazione delle feature e del target
    X = df[feature_cols].copy()
    y = df[target_col].copy()

    # Divisione in train e test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y if y.nunique() < 20 else None
    )
    logger.info(f"Dati divisi: {len(X_train)} campioni di train e {len(X_test)} campioni di test.")
    return X_train, X_test, y_train, y_test


def build_preprocessor(
    numeric_features: List[str],
    categorical_features: List[str]
) -> ColumnTransformer:
    """
    Crea un ColumnTransformer per la pre-elaborazione dei dati.

    - Numerico: imputazione con la mediana, scaling con StandardScaler.
    - Categoriale: imputazione con il valore più frequente, codifica OneHot.

    Args:
        numeric_features: Lista delle colonne numeriche.
        categorical_features: Lista delle colonne categoriali.

    Returns:
        ColumnTransformer
    """
    # Pipeline per le feature numeriche
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])

    # Pipeline per le feature categoriali
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse=False))
    ])

    # Creazione del ColumnTransformer
    preprocessor = ColumnTransformer(transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ])
    logger.info("Preprocessore creato con pipeline numeriche e categoriali.")
    return preprocessor


def train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    preprocessor: ColumnTransformer,
    task: str = 'classification',
    model_params: Optional[Dict] = None
) -> Pipeline:
    """
    Allena un modello pipeline (classificazione o regressione).

    Args:
        X_train: Dati di training.
        y_train: Target di training.
        preprocessor: Preprocessore da applicare ai dati.
        task: Tipo di task ('classification' o 'regression').
        model_params: Dizionario di iperparametri per GridSearchCV.

    Returns:
        Pipeline addestrata.
    """
    # Selezione del modello in base al task
    if task == 'classification':
        estimator = RandomForestClassifier(random_state=42)
        scoring = 'f1'
    elif task == 'regression':
        estimator = RandomForestRegressor(random_state=42)
        scoring = 'neg_mean_squared_error'
    else:
        logger.error(f"Task non supportato: {task}")
        raise ValueError("Il task deve essere 'classification' o 'regression'.")

    # Creazione della pipeline
    pipe = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('model', estimator)
    ])

    # Se sono forniti iperparametri, utilizza GridSearchCV
    if model_params:
        grid = GridSearchCV(pipe, model_params, scoring=scoring, cv=5, n_jobs=-1)
        grid.fit(X_train, y_train)
        logger.info(f"Migliori parametri trovati da GridSearch: {grid.best_params_}")
        return grid.best_estimator_
    else:
        pipe.fit(X_train, y_train)
        logger.info("Addestramento del modello completato.")
        return pipe


def evaluate_model(
    model: Pipeline,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    task: str = 'classification'
) -> dict:
    """
    Valuta il modello addestrato sui dati di test.

    Args:
        model: Pipeline addestrata.
        X_test: Dati di test.
        y_test: Target di test.
        task: Tipo di task ('classification' o 'regression').

    Returns:
        Dizionario con le metriche di valutazione.
    """
    y_pred = model.predict(X_test)
    results = {}

    # Calcolo delle metriche in base al task
    if task == 'classification':
        results['accuracy'] = accuracy_score(y_test, y_pred)
        results['f1_score'] = f1_score(y_test, y_pred, average='weighted')
        if hasattr(model.named_steps['model'], 'predict_proba'):
            y_prob = model.predict_proba(X_test)[:, 1]
            results['roc_auc'] = roc_auc_score(y_test, y_prob)
    else:
        results['mse'] = mean_squared_error(y_test, y_pred)
        results['rmse'] = mean_squared_error(y_test, y_pred, squared=False)
        results['r2'] = r2_score(y_test, y_pred)
    logger.info(f"Risultati della valutazione: {results}")
    return results


def save_model(
    model: Pipeline,
    path: str
) -> None:
    """
    Salva la pipeline del modello addestrato su disco.

    Args:
        model: Pipeline addestrata.
        path: Percorso dove salvare il modello.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    joblib.dump(model, path)
    logger.info(f"Modello salvato in {path}")


if __name__ == '__main__':
    import argparse

    # Parser per gli argomenti della riga di comando
    parser = argparse.ArgumentParser(description='Addestra e valuta un modello di ML')
    parser.add_argument('input', help='Percorso al file CSV preprocessato')
    parser.add_argument('--features', nargs='+', required=True, help='Nomi delle colonne delle feature')
    parser.add_argument('--target', required=True, help='Nome della colonna target')
    parser.add_argument('--task', choices=['classification','regression'], default='classification', help='Tipo di task')
    parser.add_argument('--model_output', help='Percorso per salvare il modello addestrato')
    parser.add_argument('--metrics_output', help='Percorso per salvare le metriche di valutazione in formato JSON')
    args = parser.parse_args()

    # Caricamento dei dati
    df = pd.read_csv(args.input)

    # Preparazione delle feature e del target
    X_train, X_test, y_train, y_test = prepare_features(df, args.features, args.target)

    # Identificazione delle colonne numeriche e categoriali
    numeric = X_train.select_dtypes(include='number').columns.tolist()
    categorical = X_train.select_dtypes(include=['object','category']).columns.tolist()

    # Creazione del preprocessore
    preprocessor = build_preprocessor(numeric, categorical)

    # Addestramento del modello
    model = train_model(X_train, y_train, preprocessor, task=args.task)

    # Valutazione del modello
    results = evaluate_model(model, X_test, y_test, task=args.task)

    # Salvataggio delle metriche di valutazione
    if args.metrics_output:
        import json
        with open(args.metrics_output, 'w') as f:
            json.dump(results, f, indent=4)

    # Salvataggio del modello addestrato
    if args.model_output:
        save_model(model, args.model_output)

    # Stampa dei risultati
    print(results)
