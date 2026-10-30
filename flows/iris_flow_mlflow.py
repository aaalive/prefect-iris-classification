"""
Iris Classification Flow using MLflow and Prefect.

Pipeline:  load_data -> split_data -> train_model -> evaluate_model -> log_to_mlflow

* Hyperparameters live in Prefect Variables, so they can be changed in the UI.
* The MLflow server address comes from the MLFLOW_TRACKING_URI environment variable.
"""

import os
from typing import Dict, Tuple

import mlflow
import mlflow.sklearn
import numpy as np
from mlflow.models import infer_signature
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from prefect.variables import Variable
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split

EXPERIMENT_NAME = "iris_classification"
RANDOM_STATE = 42

# Default values of the Prefect Variables (also used if a Variable does not exist).
DEFAULT_VARIABLES = {
    "rf_n_estimators": 100,
    "rf_max_depth": 10,
    "rf_test_size": 0.2,
    "model_name": "iris_model",
    "model_version": "1",
}


def get_variable(name: str):
    """Read a Prefect Variable and fall back to its default if it is missing."""
    return Variable.get(name, default=DEFAULT_VARIABLES[name])


def setup_variables() -> None:
    """Create the Prefect Variables if they don't exist (never overwrite existing ones)."""
    for name, value in DEFAULT_VARIABLES.items():
        if Variable.get(name, default=None) is None:
            Variable.set(name, value)


@task(retries=3, retry_delay_seconds=5)
def load_data() -> Tuple[np.ndarray, np.ndarray]:
    """Load the iris dataset."""
    logger = get_run_logger()
    try:
        X, y = load_iris(return_X_y=True)
        logger.info(f"Dataset loaded with shape: {X.shape}")
        return X, y
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise


@task
def split_data(
    X: np.ndarray, y: np.ndarray
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Split data into train and test sets (test size comes from a Variable)."""
    logger = get_run_logger()
    test_size = float(get_variable("rf_test_size"))
    logger.info(f"Splitting data with test_size={test_size}")
    return train_test_split(
        X, y, test_size=test_size, random_state=RANDOM_STATE, stratify=y
    )


@task
def train_model(X_train: np.ndarray, y_train: np.ndarray) -> RandomForestClassifier:
    """Train a Random Forest using the hyperparameters stored in Variables."""
    logger = get_run_logger()
    n_estimators = int(get_variable("rf_n_estimators"))
    max_depth = int(get_variable("rf_max_depth"))
    logger.info(f"Training RandomForest: n_estimators={n_estimators}, max_depth={max_depth}")
    model = RandomForestClassifier(
        n_estimators=n_estimators, max_depth=max_depth, random_state=RANDOM_STATE
    )
    model.fit(X_train, y_train)
    return model


@task
def evaluate_model(
    model: RandomForestClassifier, X_test: np.ndarray, y_test: np.ndarray
) -> Dict:
    """Evaluate the model on the test set."""
    logger = get_run_logger()
    y_pred = model.predict(X_test)
    accuracy = float(accuracy_score(y_test, y_pred))
    report = classification_report(y_test, y_pred)
    logger.info(f"Model accuracy: {accuracy:.4f}")
    return {"accuracy": accuracy, "n_test_samples": len(y_test), "report": report}


@task(retries=3, retry_delay_seconds=5)
def log_to_mlflow(
    model: RandomForestClassifier, X_train: np.ndarray, results: Dict
) -> str:
    """Log parameters, metrics, the report and the model to MLflow."""
    logger = get_run_logger()
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(EXPERIMENT_NAME)

    model_name = str(get_variable("model_name"))
    model_version = str(get_variable("model_version"))

    with mlflow.start_run(run_name=flow_run.name or "iris_classification_run", nested=True) as run:
        mlflow.log_params(
            {
                "n_estimators": model.n_estimators,
                "max_depth": model.max_depth,
                "test_size": float(get_variable("rf_test_size")),
            }
        )
        mlflow.log_metric("accuracy", results["accuracy"])
        mlflow.log_metric("n_test_samples", results["n_test_samples"])
        mlflow.set_tag("model_version", model_version)
        if flow_run.id:
            mlflow.set_tag("prefect.flow_run_id", flow_run.id)
        mlflow.log_text(results["report"], "classification_report.txt")
        mlflow.sklearn.log_model(
            model,
            name=model_name,
            registered_model_name=model_name,
            signature=infer_signature(X_train, model.predict(X_train)),
            # Explicit format: it works the same on every MLflow version.
            serialization_format="cloudpickle",
        )
        logger.info(f"Logged MLflow run {run.info.run_id} to {tracking_uri}")
        return run.info.run_id


@flow(name="iris-classification-flow")
def iris_classification_flow() -> float:
    """Main flow for iris classification. Returns the test accuracy."""
    logger = get_run_logger()
    logger.info("Starting Iris classification workflow")
    try:
        X, y = load_data()
        X_train, X_test, y_train, y_test = split_data(X, y)
        model = train_model(X_train, y_train)
        results = evaluate_model(model, X_test, y_test)
        log_to_mlflow(model, X_train, results)
        logger.info("Workflow completed successfully!")
        return results["accuracy"]
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        raise


if __name__ == "__main__":
    setup_variables()
    iris_classification_flow()
