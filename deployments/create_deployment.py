"""
Create a deployment for the iris classification flow.

Option 1 (default): the worker clones the LATEST code from GitHub for every run.
Option 2: set DEPLOYMENT_SOURCE=local to use the local project folder instead.
"""

import os

from prefect import flow
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials

WORK_POOL_NAME = "ml-pool"
ENTRYPOINT = "flows/iris_flow_mlflow.py:iris_classification_flow"
CREDENTIALS_BLOCK_NAME = "github-credentials"


def create_deployment():
    """Create the deployment (GitHub source by default)."""
    if os.getenv("DEPLOYMENT_SOURCE", "github").lower() == "local":
        # OPTION 2: local flow
        flow_from_source = flow.from_source(source="./", entrypoint=ENTRYPOINT)
        name, tags = "iris-model-local", ["ml", "iris", "local"]
    else:
        # OPTION 1: remote flow on GitHub
        credentials = GitHubCredentials.load(CREDENTIALS_BLOCK_NAME)
        repository = os.environ["GITHUB_REPOSITORY"]
        source = GitRepository(
            url=f"https://github.com/{repository}.git",
            credentials=credentials,  # saved block: the token is never written in the deployment
            branch=os.getenv("GITHUB_BRANCH", "main"),
        )
        flow_from_source = flow.from_source(source=source, entrypoint=ENTRYPOINT)
        name, tags = "iris-model", ["ml", "iris"]

    deployment_id = flow_from_source.deploy(
        name=name,
        work_pool_name=WORK_POOL_NAME,
        tags=tags,
        description="Iris Random Forest training tracked with MLflow",
        ignore_warnings=True,  # hides the "process work pool" hint
    )
    print(f"Deployment '{name}' created/updated: {deployment_id}")


if __name__ == "__main__":
    create_deployment()
