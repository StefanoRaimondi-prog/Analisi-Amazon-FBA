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

# Configure logging
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
    Split data into train and test sets and separate features/target.

    Returns:
        X_train, X_test, y_train, y_test
    """
    if target_col not in df.columns:
        logger.error(f"Target column '{target_col}' not found in DataFrame.")
        raise ValueError(f"Missing target column: {target_col}")

    X = df[feature_cols].copy()
    y = df[target_col].copy()
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y if y.nunique() < 20 else None
    )
    logger.info(f"Split data: {len(X_train)} train and {len(X_test)} test samples.")
    return X_train, X_test, y_train, y_test


def build_preprocessor(
    numeric_features: List[str],
    categorical_features: List[str]
) -> ColumnTransformer:
    """
    Create a ColumnTransformer for preprocessing.

    Numeric: impute with median, scale with StandardScaler.
    Categorical: impute with most frequent, OneHotEncode.

    Returns:
        ColumnTransformer
    """
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])

    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse=False))
    ])

    preprocessor = ColumnTransformer(transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ])
    logger.info("Preprocessor created with numeric and categorical pipelines.")
    return preprocessor


def train_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    preprocessor: ColumnTransformer,
    task: str = 'classification',
    model_params: Optional[Dict] = None
) -> Pipeline:
    """
    Train a model pipeline (classification or regression).

    Args:
        task: 'classification' or 'regression'
        model_params: dict of hyperparameters for GridSearchCV

    Returns:
        Trained pipeline
    """
    if task == 'classification':
        estimator = RandomForestClassifier(random_state=42)
        scoring = 'f1'
    elif task == 'regression':
        estimator = RandomForestRegressor(random_state=42)
        scoring = 'neg_mean_squared_error'
    else:
        logger.error(f"Unsupported task: {task}")
        raise ValueError("Task must be 'classification' or 'regression'.")

    pipe = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('model', estimator)
    ])

    if model_params:
        grid = GridSearchCV(pipe, model_params, scoring=scoring, cv=5, n_jobs=-1)
        grid.fit(X_train, y_train)
        logger.info(f"GridSearch best params: {grid.best_params_}")
        return grid.best_estimator_
    else:
        pipe.fit(X_train, y_train)
        logger.info("Model training complete.")
        return pipe


def evaluate_model(
    model: Pipeline,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    task: str = 'classification'
) -> dict:
    """
    Evaluate the trained model on test data.

    Returns:
        Dictionary of metrics.
    """
    y_pred = model.predict(X_test)
    results = {}
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
    logger.info(f"Evaluation results: {results}")
    return results


def save_model(
    model: Pipeline,
    path: str
) -> None:
    """
    Save trained model pipeline to disk.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    joblib.dump(model, path)
    logger.info(f"Model saved to {path}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Train and evaluate ML model')
    parser.add_argument('input', help='Path to preprocessed CSV file')
    parser.add_argument('--features', nargs='+', required=True, help='Feature column names')
    parser.add_argument('--target', required=True, help='Target column name')
    parser.add_argument('--task', choices=['classification','regression'], default='classification', help='Task type')
    parser.add_argument('--model_output', help='Path to save trained model')
    parser.add_argument('--metrics_output', help='Path to save evaluation metrics as JSON')
    args = parser.parse_args()

    df = pd.read_csv(args.input)
    X_train, X_test, y_train, y_test = prepare_features(df, args.features, args.target)
    # Identify numeric vs categorical
    numeric = X_train.select_dtypes(include='number').columns.tolist()
    categorical = X_train.select_dtypes(include=['object','category']).columns.tolist()
    preprocessor = build_preprocessor(numeric, categorical)
    model = train_model(X_train, y_train, preprocessor, task=args.task)
    results = evaluate_model(model, X_test, y_test, task=args.task)
    if args.metrics_output:
        import json
        with open(args.metrics_output, 'w') as f:
            json.dump(results, f, indent=4)
    if args.model_output:
        save_model(model, args.model_output)
    print(results)
